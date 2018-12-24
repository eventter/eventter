package v0

import (
	"net"
	"time"

	"eventter.io/mq/amqp/authentication"
)

type ServerConn struct {
	Conn        net.Conn
	Transport   *Transport
	Token       authentication.Token
	ChannelMax  uint16
	FrameMax    uint32
	Heartbeat   time.Duration
	VirtualHost string
}
