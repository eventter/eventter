package mq

import (
	"context"

	"eventter.io/mq/amqp/v0"
)

func (s *Server) handleAMQPv0BasicQos(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.BasicQos) error {
	// prefetch-size is ignored
	ch.prefetchCount = uint32(frame.PrefetchCount)
	// global is ignored

	return transport.Send(&v0.BasicQosOk{
		FrameMeta: v0.FrameMeta{Channel: ch.id},
	})
}
