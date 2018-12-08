package consumers

import (
	"eventter.io/mq/client"
)

type Message struct {
	Topic   client.NamespaceName
	Message *client.Message
	SeqNo   uint64
}
