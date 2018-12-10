package consumers

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

const (
	ready     = math.MaxUint64
	ack       = 0
	zeroSeqNo = 0
)

var (
	ErrGroupClosed               = errors.New("group is closed")
	currentSubscriptionID uint64 = 0
)

type Group struct {
	n        int
	mutex    sync.Mutex
	cond     *sync.Cond
	messages []Message
	read     int
	write    int
	closed   uint32
	Ack      <-chan MessageAck
	sendAck  chan<- MessageAck
}

func NewGroup(n int) (*Group, error) {
	if n < 1 {
		return nil, errors.New("n must be positive")
	}

	ack := make(chan MessageAck, n)

	g := &Group{
		n:        n,
		messages: make([]Message, n+1),
		Ack:      ack,
		sendAck:  ack,
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

	g.messages[g.write] = *message
	g.messages[g.write].SubscriptionID = ready
	g.messages[g.write].SeqNo = zeroSeqNo
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
	g.mutex.Lock()
	atomic.StoreUint32(&g.closed, 1)
	g.sendAck = nil
	g.cond.Broadcast()
	g.mutex.Unlock()
	return nil
}
