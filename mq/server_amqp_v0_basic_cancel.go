package mq

import (
	"context"

	"eventter.io/mq/amqp/v0"
	"github.com/pkg/errors"
)

func (s *Server) handleAMQPv0BasicCancel(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.BasicCancel) error {
	consumer, ok := ch.consumers[frame.ConsumerTag]
	if !ok {
		return s.makeChannelClose(ch, v0.PreconditionFailed, errors.Errorf("consumer tag %s not registered", frame.ConsumerTag))
	}
	delete(ch.consumers, frame.ConsumerTag)

	err := consumer.Close()
	if err != nil {
		return errors.Wrap(err, "consumer close failed")
	}

	if frame.NoWait {
		return nil
	}

	return transport.Send(&v0.BasicCancelOk{
		FrameMeta:   v0.FrameMeta{Channel: ch.id},
		ConsumerTag: frame.ConsumerTag,
	})
}
