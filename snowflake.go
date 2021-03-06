/**
 * Author:        Tony.Shao
 * Email:         xiocode@gmail.com
 * Github:        github.com/xiocode
 * File:          uuid.go
 * Description:   uuid Generator
 */

package snowflake

import (
	"errors"
	"sync"
	"time"
)

const (
	// custom epoch - time offset, milliseconds
	// Oct 25, 2014 05:06:02.373 UTC, incidentally equal to the first few digits of sqrt(2)
	epoch int64 = 1414213562373

	// number of bit allocated for server id (max 1023)
	numWorkerBits = 10
	// # of bits allocated for millisecond
	numSequenceBits = 12

	// workerId mask
	maxWorkerId = -1 ^ (-1 << numWorkerBits)
	// sequence mask
	maxSequence = -1 ^ (-1 << numSequenceBits)
)

type SnowFlake struct {
	lastTimestamp uint64
	sequence      uint32
	workerId      uint32
	lock          sync.Mutex
}

// pack bits into a snowflake
func (sf *SnowFlake) pack() uint64 {
	return (sf.lastTimestamp << (numWorkerBits + numSequenceBits)) |
		(uint64(sf.workerId) << numSequenceBits) |
		(uint64(sf.sequence))
}

// Initialise generator
func NewSnowFlake(workerId uint32) (*SnowFlake, error) {
	if workerId < 0 || workerId > maxWorkerId {
		return nil, errors.New("invalid worker Id")
	}
	return &SnowFlake{workerId: workerId}, nil
}

// Generate next unique ID
func (sf *SnowFlake) Next() (uint64, error) {
	sf.lock.Lock()
	defer sf.lock.Unlock()

	ts := timestamp()
	if ts == sf.lastTimestamp {
		sf.sequence = (sf.sequence + 1) & maxSequence
		if sf.sequence == 0 {
			ts = sf.waitNextMilli(ts)
		}
	} else {
		sf.sequence = 0
	}

	if ts < sf.lastTimestamp {
		return 0, errors.New("invalid system clock")
	}
	sf.lastTimestamp = ts
	return sf.pack(), nil
}

// Sequance exhausted. Wait till next millisocond
func (sf *SnowFlake) waitNextMilli(ts uint64) uint64 {
	for ts == sf.lastTimestamp {
		time.Sleep(100 * time.Microsecond)
		ts = timestamp()
	}
	return ts
}

func timestamp() uint64 {
	// convert from nanoseconds to milliseconds, adjust for custom epoch
	return uint64(time.Now().UnixNano()/int64(time.Millisecond) - epoch)
}
