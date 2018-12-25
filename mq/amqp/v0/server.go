package v0

import (
	"time"

	"eventter.io/mq/amqp/authentication"
)

type ServerConn struct {
	Transport   *Transport
	Token       authentication.Token
	ChannelMax  uint16
	FrameMax    uint32
	Heartbeat   time.Duration
	VirtualHost string
}
