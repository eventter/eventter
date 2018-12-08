package consumers

import (
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	ErrSubscriptionClosed = errors.New("subscription is closed")
	ErrNotLeased          = errors.New("cannot (n)ack message, it is not leased to this subscription")
)

type Subscription struct {
	ID     uint64
	group  *Group
	closed uint32
	seq    uint64
}

func (s *Subscription) Next() (*Message, error) {
	if atomic.LoadUint32(&s.closed) == 1 {
		return nil, ErrSubscriptionClosed
	}

	s.group.mutex.Lock()
	defer s.group.mutex.Unlock()

	var i int
	for {
		i = -1
		for j := s.group.read; j != s.group.write; j = (j + 1) % len(s.group.messages) {
			if s.group.leases[j] == ready {
				i = j
				break
			}
		}
		if i != -1 {
			break
		}
		if atomic.LoadUint32(&s.closed) == 1 {
			return nil, ErrSubscriptionClosed
		}
		if s.group.read == s.group.write && atomic.LoadUint32(&s.group.closed) == 1 {
			return nil, ErrGroupClosed
		}
		s.group.cond.Wait()
	}

	s.seq++
	s.group.leases[i] = s.ID
	s.group.messages[i].SeqNo = s.seq

	return s.group.messages[i], nil
}

func (s *Subscription) Ack(seqNo uint64) error {
	s.group.mutex.Lock()
	defer s.group.mutex.Unlock()

	i := -1
	for j := s.group.read; j != s.group.write; j = (j + 1) % len(s.group.messages) {
		if s.group.leases[j] == s.ID && s.group.messages[j].SeqNo == seqNo {
			i = j
			break
		}
	}
	if i == -1 {
		return ErrNotLeased
	}

	s.group.leases[i] = ack
	s.group.messages[i] = nil

	i = s.group.read
	for i != s.group.write && s.group.leases[i] == ack {
		i = (i + 1) % len(s.group.messages)
	}

	s.group.read = i

	s.group.cond.Broadcast()

	return nil
}

func (s *Subscription) Nack(seqNo uint64) error {
	s.group.mutex.Lock()
	defer s.group.mutex.Unlock()

	i := -1
	for j := s.group.read; j != s.group.write; j = (j + 1) % len(s.group.messages) {
		if s.group.leases[j] == s.ID && s.group.messages[j].SeqNo == seqNo {
			i = j
			break
		}
	}
	if i == -1 {
		return ErrNotLeased
	}

	s.group.leases[i] = ready
	s.group.messages[i].SeqNo = 0

	s.group.cond.Broadcast()

	return nil
}

func (s *Subscription) Close() error {
	s.group.mutex.Lock()
	defer s.group.mutex.Unlock()

	for i := s.group.read; i != s.group.write; i = (i + 1) % len(s.group.messages) {
		if s.group.leases[i] == s.ID {
			s.group.leases[i] = ready
			s.group.messages[i].SeqNo = 0
		}
	}

	atomic.StoreUint32(&s.closed, 1)

	s.group.cond.Broadcast()

	return nil
}
