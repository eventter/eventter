package consumer

import (
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	ErrSubscriptionClosed = errors.New("subscription is closed")
)

type Subscription struct {
	ID     uint64
	group  *Group
	closed uint32
}

func (s *Subscription) Next() (*Message, error) {
	s.group.mutex.Lock()
	defer s.group.mutex.Unlock()

	for s.group.read == s.group.write {
		s.group.cond.Wait()
		if atomic.LoadUint32(&s.closed) == 1 {
			return nil, ErrSubscriptionClosed
		}
		if atomic.LoadUint32(&s.group.closed) == 1 {
			return nil, ErrGroupClosed
		}
	}

	message := s.group.messages[s.group.read]
	s.group.messages[s.group.read] = nil
	s.group.read = (s.group.read + 1) % len(s.group.messages)

	s.group.cond.Broadcast()

	return message, nil
}

func (s *Subscription) Close() error {
	atomic.StoreUint32(&s.closed, 1)
	s.group.cond.Broadcast()
	return nil
}
