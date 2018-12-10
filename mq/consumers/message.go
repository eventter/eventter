package consumers

import (
	"eventter.io/mq/client"
)

type Message struct {
	Topic          client.NamespaceName
	SegmentID      uint64
	Offset         int64
	Message        *client.Message
	SubscriptionID uint64
	SeqNo          uint64
}

type MessageAck struct {
	SegmentID uint64
	Offset    int64
}
