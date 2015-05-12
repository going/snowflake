/**
 * Author:        Tony.Shao
 * Email:         xiocode@gmail.com
 * Github:        github.com/xiocode
 * File:          uuid.go
 * Description:   uuid Generator
 */

package snowflake

import (
	"sync"
	"sync/atomic"
	"time"
)

var generator *Generator

func init() {
	generator = newGenerator(1, 1)
}

func GetUUID() int64 {
	return generator.next()
}

type Generator struct {
	workerId           int64
	dataCenterId       int64
	idsGenerated       int64
	currentTimeStamp   int64
	sequence           int64
	workerIdBits       uint
	dataCenterIdBits   uint
	maxWorkerId        int64
	max_data_center_id int64
	sequenceBits       uint
	workerIdShift      uint
	dataCenterIdShift  uint
	timeStampLeftShift uint
	sequenceMask       int64
	lastTimeStamp      int64
	sync.Mutex
}

func newGenerator(workerId, dataCenterId int64) *Generator {
	Generator := &Generator{}
	Generator.workerId = workerId
	Generator.dataCenterId = dataCenterId
	Generator.idsGenerated = 0
	Generator.currentTimeStamp = 1318323746000
	Generator.sequence = 0
	Generator.workerIdBits = 5
	Generator.dataCenterIdBits = 5
	Generator.sequenceBits = 12
	Generator.maxWorkerId = -1 ^ (-1 << Generator.workerIdBits)
	Generator.max_data_center_id = -1 ^ (-1 << Generator.dataCenterIdBits)
	Generator.workerIdShift = Generator.sequenceBits
	Generator.dataCenterIdShift = Generator.sequenceBits + Generator.workerIdBits
	Generator.timeStampLeftShift = Generator.sequenceBits + Generator.workerIdBits + Generator.dataCenterIdBits
	Generator.sequenceMask = -1 ^ (-1 << Generator.sequenceBits)
	Generator.lastTimeStamp = -1
	return Generator
}

func (this *Generator) now() int64 {
	return time.Now().Unix() * 1000
}

func (this *Generator) nextTime(lastTimeStamp int64) int64 {
	timeStamp := this.now()
	for timeStamp == lastTimeStamp {
		timeStamp = this.now()
	}
	return timeStamp
}

func (this *Generator) next() int64 {
	this.Lock()
	defer this.Unlock()

	timeStamp := this.now()
	if this.lastTimeStamp > timeStamp {
		return 0
	}
	if this.lastTimeStamp == timeStamp {
		this.sequence = (atomic.AddInt64(&this.sequence, 1)) & this.sequenceMask
		if this.sequence == 0 {
			timeStamp = this.nextTime(this.lastTimeStamp)
		}
	} else {
		this.sequence = 0
	}
	this.lastTimeStamp = timeStamp

	return ((timeStamp - this.currentTimeStamp) << this.timeStampLeftShift) | (this.dataCenterId << this.dataCenterIdShift) | (this.workerId << this.workerIdShift) | this.sequence
}
