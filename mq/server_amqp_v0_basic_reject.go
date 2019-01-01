package mq

import (
	"context"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/emq"
	"github.com/pkg/errors"
)

func (s *Server) handleAMQPv0BasicReject(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.BasicReject) error {
	i := -1
	for j := 0; j < len(ch.inflight); j++ {
		if ch.inflight[j].deliveryTag == frame.DeliveryTag {
			i = j
			break
		}
	}

	if i == -1 {
		return s.makeChannelClose(ch, v0.PreconditionFailed, errors.Errorf("delivery tag %d doesn't exist", frame.DeliveryTag))
	}

	if frame.Requeue {
		_, err := s.Nack(ctx, &emq.MessageNackRequest{
			NodeID:         ch.inflight[i].nodeID,
			SubscriptionID: ch.inflight[i].subscriptionID,
			SeqNo:          ch.inflight[i].seqNo,
		})
		if err != nil {
			return errors.Wrap(err, "nack failed")
		}

	} else {
		_, err := s.Ack(ctx, &emq.MessageAckRequest{
			NodeID:         ch.inflight[i].nodeID,
			SubscriptionID: ch.inflight[i].subscriptionID,
			SeqNo:          ch.inflight[i].seqNo,
		})
		if err != nil {
			return errors.Wrap(err, "ack failed")
		}
	}

	ch.inflight = ch.inflight[:i+copy(ch.inflight[i:], ch.inflight[i+1:])]

	return nil
}
