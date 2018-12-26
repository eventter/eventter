package consumers

import (
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	ErrSubscriptionClosed = errors.New("subscription is closed")
	ErrNotLeased          = errors.New("cannot (n)ack message, it is not leased to this subscription")
	messageAcksPool       = sync.Pool{
		New: func() interface{} {
			return make([]MessageAck, 64)
		},
	}
)

type Subscription struct {
	ID     uint64
	group  *Group
	n      int
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
		if s.n != 0 {
			i = -1
			for j := s.group.read; j != s.group.write; j = (j + 1) % len(s.group.messages) {
				if s.group.messages[j].SubscriptionID == ready {
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
		}
		s.group.cond.Wait()
	}

	s.seq++
	s.group.messages[i].SubscriptionID = s.ID
	s.group.messages[i].SeqNo = s.seq
	if s.n > 0 {
		s.n--
	}

	return &s.group.messages[i], nil
}

func (s *Subscription) Ack(seqNo uint64) error {
	s.group.mutex.Lock()

	i := -1
	for j := s.group.read; j != s.group.write; j = (j + 1) % len(s.group.messages) {
		if s.group.messages[j].SubscriptionID == s.ID && s.group.messages[j].SeqNo == seqNo {
			i = j
			break
		}
	}
	if i == -1 {
		s.group.mutex.Unlock()
		return ErrNotLeased
	}

	s.group.messages[i].SubscriptionID = ack
	s.group.messages[i].SeqNo = zeroSeqNo
	if s.n >= 0 {
		s.n++
	}

	messageAcks := messageAcksPool.Get().([]MessageAck)[:0]

	j := s.group.read
	for ; j != s.group.write && s.group.messages[j].SubscriptionID == ack; j = (j + 1) % len(s.group.messages) {
		messageAcks = append(messageAcks, MessageAck{
			SegmentID:    s.group.messages[j].SegmentID,
			CommitOffset: s.group.messages[j].CommitOffset,
		})
		s.group.messages[j].Reset()
	}
	s.group.read = j

	c := s.group.sendAck
	s.group.cond.Broadcast()
	s.group.mutex.Unlock()

	if c != nil {
		for _, messageAck := range messageAcks {
			c <- messageAck
		}
	}

	messageAcksPool.Put(messageAcks)

	return nil
}

func (s *Subscription) Nack(seqNo uint64) error {
	s.group.mutex.Lock()
	defer s.group.mutex.Unlock()

	i := -1
	for j := s.group.read; j != s.group.write; j = (j + 1) % len(s.group.messages) {
		if s.group.messages[j].SubscriptionID == s.ID && s.group.messages[j].SeqNo == seqNo {
			i = j
			break
		}
	}
	if i == -1 {
		return ErrNotLeased
	}

	s.group.messages[i].SubscriptionID = ready
	s.group.messages[i].SeqNo = zeroSeqNo
	if s.n >= 0 {
		s.n++
	}

	s.group.cond.Broadcast()

	return nil
}

func (s *Subscription) Close() error {
	s.group.mutex.Lock()
	defer s.group.mutex.Unlock()

	for i := s.group.read; i != s.group.write; i = (i + 1) % len(s.group.messages) {
		if s.group.messages[i].SubscriptionID == s.ID {
			s.group.messages[i].SubscriptionID = ready
			s.group.messages[i].SeqNo = zeroSeqNo
		}
	}

	atomic.StoreUint32(&s.closed, 1)

	s.group.cond.Broadcast()

	return nil
}
