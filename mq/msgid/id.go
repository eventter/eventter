package msgid

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const Size = 16

type ID [Size]byte

func (id *ID) FromBytes(buf []byte) {
	_ = buf[15]

	id[0] = buf[0]
	id[1] = buf[1]
	id[2] = buf[2]
	id[3] = buf[3]
	id[4] = buf[4]
	id[5] = buf[5]
	id[6] = buf[6]
	id[7] = buf[7]
	id[8] = buf[8]
	id[9] = buf[9]
	id[10] = buf[10]
	id[11] = buf[11]
	id[12] = buf[12]
	id[13] = buf[13]
	id[14] = buf[14]
	id[15] = buf[15]
}

func (id *ID) Bytes() []byte {
	return id[:]
}

func (id *ID) Time() time.Time {
	millis := uint64(id[0])<<40 + uint64(id[1])<<32 + uint64(id[2])<<24 + uint64(id[3])<<16 + uint64(id[4])<<8 + uint64(id[5])
	return time.Unix(int64(millis/1000), int64(millis%1000)*int64(time.Millisecond))
}

func (id *ID) Randomness() []byte {
	return id[6:]
}

type TimeSource interface {
	Now() time.Time
}

type stdTimeSource struct{}

func (*stdTimeSource) Now() time.Time {
	return time.Now()
}

func NewStdTimeSource() TimeSource {
	return &stdTimeSource{}
}

type Generator interface {
	New() ID
}

type generator struct {
	timeSource TimeSource
	mutex      sync.RWMutex
	rand       *rand.Rand
	lastMillis uint64
	lastRandHi uint32
	lastRandLo uint64
}

func NewGenerator(timeSource TimeSource, randSource rand.Source) Generator {
	return &generator{
		timeSource: timeSource,
		rand:       rand.New(randSource),
	}
}

func (g *generator) New() ID {
	now := g.timeSource.Now()
	millis := uint64(now.Unix()*1000) + uint64(now.Nanosecond()/int(time.Millisecond))

	var randHi uint32
	var randLo uint64

	for {
		var ok = false

		if millis == atomic.LoadUint64(&g.lastMillis) {
			g.mutex.RLock()

			lastRandHi := atomic.LoadUint32(&g.lastRandHi)
			lastRandLo := atomic.LoadUint64(&g.lastRandLo)

			if lastRandLo+1 < lastRandLo {
				if lastRandHi+1 < lastRandHi {
					millis += 1
				} else {
					randHi = lastRandHi + 1
					if atomic.CompareAndSwapUint32(&g.lastRandHi, lastRandHi, randHi) {
						randLo = 0
						if !atomic.CompareAndSwapUint64(&g.lastRandLo, lastRandLo, randLo) {
							randLo = lastRandLo
						}
						ok = true
					}
				}

			} else {
				randHi = lastRandHi
				randLo = lastRandLo + 1
				if atomic.CompareAndSwapUint64(&g.lastRandLo, lastRandLo, randLo) {
					ok = true
				}
			}

			g.mutex.RUnlock()

		} else {
			g.mutex.Lock()

			g.lastMillis = millis
			randHi = g.rand.Uint32()
			g.lastRandHi = randHi
			randLo = g.rand.Uint64()
			g.lastRandLo = randLo

			g.mutex.Unlock()

			ok = true
		}

		if ok {
			break
		}
	}

	id := ID{}

	id[0] = byte(millis >> 40)
	id[1] = byte(millis >> 32)
	id[2] = byte(millis >> 24)
	id[3] = byte(millis >> 16)
	id[4] = byte(millis >> 8)
	id[5] = byte(millis)
	id[6] = byte(randHi >> 8)
	id[7] = byte(randHi)
	id[8] = byte(randLo >> 56)
	id[9] = byte(randLo >> 48)
	id[10] = byte(randLo >> 40)
	id[11] = byte(randLo >> 32)
	id[12] = byte(randLo >> 24)
	id[13] = byte(randLo >> 16)
	id[14] = byte(randLo >> 8)
	id[15] = byte(randLo)

	return id
}
