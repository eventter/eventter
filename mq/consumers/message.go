package consumers

import (
	"time"

	"eventter.io/mq/client"
)

type Message struct {
	Topic          client.NamespaceName
	SegmentID      uint64
	CommitOffset   int64
	Time           time.Time
	Message        *client.Message
	SubscriptionID uint64
	SeqNo          uint64
}

type MessageAck struct {
	SegmentID    uint64
	CommitOffset int64
}
