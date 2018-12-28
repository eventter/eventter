package consumers

import (
	"time"

	"eventter.io/mq/emq"
)

type Message struct {
	Topic          emq.NamespaceName
	SegmentID      uint64
	CommitOffset   int64
	Time           time.Time
	Message        *emq.Message
	SubscriptionID uint64
	SeqNo          uint64
}

func (m *Message) Reset() {
	*m = Message{}
}

type Commit struct {
	SegmentID    uint64
	CommitOffset int64
}
