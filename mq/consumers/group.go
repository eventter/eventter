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
	Commits  chan Commit
}

func NewGroup(n int) (*Group, error) {
	if n < 1 {
		return nil, errors.New("n must be positive")
	}

	g := &Group{
		n:        n,
		messages: make([]Message, n+1),
	}

	g.cond = sync.NewCond(&g.mutex)

	return g, nil
}

func (g *Group) Offer(message *Message) error {
	if atomic.LoadUint32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	g.mutex.Lock()

	for (g.write+1)%len(g.messages) == g.read {
		if atomic.LoadUint32(&g.closed) == 1 {
			g.mutex.Unlock()
			return ErrGroupClosed
		}
		g.cond.Wait()
	}

	g.messages[g.write] = *message
	g.messages[g.write].SubscriptionID = ready
	g.messages[g.write].SeqNo = zeroSeqNo
	g.write = (g.write + 1) % len(g.messages)

	g.cond.Broadcast()
	g.mutex.Unlock()

	return nil
}

// Subscribe to consumer group with unlimited messages in-flight.
func (g *Group) Subscribe() *Subscription {
	return g.SubscribeN(-1)
}

// Subscribe to consumer group with limited messages in-flight. Negative `n` means no limit.
func (g *Group) SubscribeN(n int) *Subscription {
	return &Subscription{
		ID:    atomic.AddUint64(&currentSubscriptionID, 1),
		n:     n,
		group: g,
	}
}

func (g *Group) Close() error {
	g.mutex.Lock()
	atomic.StoreUint32(&g.closed, 1)
	g.Commits = nil
	g.cond.Broadcast()
	g.mutex.Unlock()
	return nil
}
