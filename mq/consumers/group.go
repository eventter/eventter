package consumers

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
	ErrGroupClosed               = errors.New("group is closed")
	currentSubscriptionID uint64 = 0
)

type Group struct {
	mutex    sync.Mutex
	cond     *sync.Cond
	messages []*Message
	leases   []uint64
	read     int
	write    int
	closed   uint32
}

func NewGroup(n int) (*Group, error) {
	if n < 1 {
		return nil, errors.New("n must be positive")
	}

	g := &Group{
		messages: make([]*Message, n+1),
		leases:   make([]uint64, n+1),
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
		if atomic.LoadUint32(&g.closed) == 1 {
			return ErrGroupClosed
		}
		g.cond.Wait()
	}

	g.messages[g.write] = message
	g.leases[g.write] = ready
	g.write = (g.write + 1) % len(g.messages)

	g.cond.Broadcast()

	return nil
}

func (g *Group) Subscribe() *Subscription {
	return &Subscription{
		ID:    atomic.AddUint64(&currentSubscriptionID, 1),
		group: g,
	}
}

func (g *Group) Close() error {
	atomic.StoreUint32(&g.closed, 1)
	g.cond.Broadcast()
	return nil
}
