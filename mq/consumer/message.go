package consumer

import (
	"eventter.io/mq/client"
)

type Message struct {
	Message *client.Message
}
