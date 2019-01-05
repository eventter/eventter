package consumers

import (
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	ErrSubscriptionClosed = errors.New("subscription is closed")
	ErrNotLeased          = errors.New("cannot (n)ack message, it is not leased to this subscription")
	ErrEmpty              = errors.New("subscription is empty")
	commitsPool           = sync.Pool{
		New: func() interface{} {
			return make([]Commit, 64)
		},
	}
)

type Subscription struct {
	ID          uint64
	group       *Group
	size        uint32
	inflight    uint32
	maxMessages uint64
	blocking    bool
	closed      uint32
	seq         uint64
}

// Subscription size is max number of in-flight messages. Zero means there is no limit.
func (s *Subscription) SetSize(size uint32) {
	s.group.mutex.Lock()
	s.size = size
	s.group.cond.Broadcast()
	s.group.mutex.Unlock()
}

func (s *Subscription) SetMaxMessages(maxMessages uint64) {
	s.group.mutex.Lock()
	s.maxMessages = maxMessages
	s.group.cond.Broadcast()
	s.group.mutex.Unlock()
}

func (s *Subscription) SetBlocking(blocking bool) {
	s.group.mutex.Lock()
	s.blocking = blocking
	s.group.cond.Broadcast()
	s.group.mutex.Unlock()
}

func (s *Subscription) Next() (*Message, error) {
	if atomic.LoadUint32(&s.closed) == 1 {
		return nil, ErrSubscriptionClosed
	}

	s.group.mutex.Lock()

	var i int
	for {
		if (s.size == 0 || s.inflight < s.size) && (s.maxMessages == 0 || s.seq < s.maxMessages) {
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
				s.group.mutex.Unlock()
				return nil, ErrSubscriptionClosed
			}
			if s.group.read == s.group.write && atomic.LoadUint32(&s.group.closed) == 1 {
				s.group.mutex.Unlock()
				return nil, ErrGroupClosed
			}
		}

		if s.inflight == 0 && (!s.blocking || (s.maxMessages != 0 && s.seq >= s.maxMessages)) {
			s.group.mutex.Unlock()
			return nil, ErrEmpty
		}

		s.group.cond.Wait()
	}

	s.seq++
	s.group.messages[i].SubscriptionID = s.ID
	s.group.messages[i].SeqNo = s.seq
	s.inflight++

	s.group.mutex.Unlock()

	message := &Message{}
	*message = s.group.messages[i]

	return message, nil
}

func (s *Subscription) Ack(seqNo uint64) error {
	if seqNo == 0 {
		return errors.New("seq no must be positive")
	}

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
	s.inflight--

	c := s.group.Commits
	var commits []Commit
	if c != nil {
		commits = commitsPool.Get().([]Commit)[:0]
	}

	j := s.group.read
	for ; j != s.group.write && s.group.messages[j].SubscriptionID == ack; j = (j + 1) % len(s.group.messages) {
		if c != nil {
			commits = append(commits, Commit{
				SegmentID:    s.group.messages[j].SegmentID,
				CommitOffset: s.group.messages[j].CommitOffset,
			})
		}
		s.group.messages[j].Reset()
	}
	s.group.read = j

	s.group.cond.Broadcast()
	s.group.mutex.Unlock()

	if c != nil {
		for _, commit := range commits {
			c <- commit
		}

		commitsPool.Put(commits)
	}

	return nil
}

func (s *Subscription) Nack(seqNo uint64) error {
	if seqNo == 0 {
		return errors.New("seq no must be positive")
	}

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

	s.group.messages[i].SubscriptionID = ready
	s.group.messages[i].SeqNo = zeroSeqNo
	s.inflight--

	s.group.cond.Broadcast()
	s.group.mutex.Unlock()

	return nil
}

func (s *Subscription) Close() error {
	s.group.mutex.Lock()

	for i := s.group.read; i != s.group.write; i = (i + 1) % len(s.group.messages) {
		if s.group.messages[i].SubscriptionID == s.ID {
			s.group.messages[i].SubscriptionID = ready
			s.group.messages[i].SeqNo = zeroSeqNo
		}
	}

	atomic.StoreUint32(&s.closed, 1)

	s.group.cond.Broadcast()
	s.group.mutex.Unlock()

	return nil
}
