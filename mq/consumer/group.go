package consumer

import (
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	ErrGroupClosed = errors.New("group is closed")
)

type Group struct {
	mutex               sync.Mutex
	cond                *sync.Cond
	messages            []*Message
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
	}

	g.cond = sync.NewCond(&g.mutex)

	return g, nil
}

func (g *Group) Offer(message *Message) error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	for (g.write+1)%len(g.messages) == g.read {
		g.cond.Wait()
		if atomic.LoadUint32(&g.closed) == 1 {
			return ErrGroupClosed
		}
	}

	g.messages[g.write] = message
	g.write = (g.write + 1) % len(g.messages)

	g.cond.Signal()

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
