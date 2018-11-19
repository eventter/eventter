package consumer

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

const (
	ready = 0
	ack   = math.MaxUint64
)

var (
	ErrGroupClosed = errors.New("group is closed")
)

type Group struct {
	mutex               sync.Mutex
	cond                *sync.Cond
	messages            []*Message
	leases              []uint64
	read                int
	write               int
	closed              uint32
	subscriptionCounter uint64
}

func NewGroup(size int) (*Group, error) {
	if size < 1 {
		return nil, errors.New("size must be positive")
	}

	g := &Group{
		messages: make([]*Message, size),
		leases:   make([]uint64, size),
	}

	g.cond = sync.NewCond(&g.mutex)

	return g, nil
}

func (g *Group) Offer(message *Message) error {
	if atomic.LoadUint32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	g.mutex.Lock()
	defer g.mutex.Unlock()

	for (g.write+1)%len(g.messages) == g.read {
		g.cond.Wait()
		if atomic.LoadUint32(&g.closed) == 1 {
			return ErrGroupClosed
		}
	}

	g.messages[g.write] = message
	g.leases[g.write] = ready
	g.write = (g.write + 1) % len(g.messages)

	g.cond.Broadcast()

	return nil
}

func (g *Group) Subscribe() *Subscription {
	return &Subscription{
		ID:    atomic.AddUint64(&g.subscriptionCounter, 1),
		group: g,
	}
}

func (g *Group) Close() error {
	atomic.StoreUint32(&g.closed, 1)
	g.cond.Broadcast()
	return nil
}
