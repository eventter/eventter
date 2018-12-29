package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
)

func (s *Server) ServeAMQPv1(ctx context.Context, transport *v1.Transport) error {
	panic("implement me")
}
