package mq

import (
	"context"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/emq"
	"github.com/pkg/errors"
)

func (s *Server) handleAMQPv0BasicAck(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.BasicAck) error {
	if frame.Multiple {
		i := 0
		n := 0
		for ; i < len(ch.inflight) && ch.inflight[i].deliveryTag <= frame.DeliveryTag; i++ {
			_, err := s.Ack(ctx, &emq.AckRequest{
				NodeID:         ch.inflight[i].nodeID,
				SubscriptionID: ch.inflight[i].subscriptionID,
				SeqNo:          ch.inflight[i].seqNo,
			})
			if err != nil {
				return errors.Wrap(err, "ack failed")
			}
			n++
		}
		if n == 0 {
			return s.makeChannelClose(ch, v0.PreconditionFailed, errors.Errorf("delivery tag %d doesn't exist", frame.DeliveryTag))
		}
		ch.inflight = ch.inflight[i:]
	} else {
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

		_, err := s.Ack(ctx, &emq.AckRequest{
			NodeID:         ch.inflight[i].nodeID,
			SubscriptionID: ch.inflight[i].subscriptionID,
			SeqNo:          ch.inflight[i].seqNo,
		})
		if err != nil {
			return errors.Wrap(err, "ack failed")
		}

		ch.inflight = ch.inflight[:i+copy(ch.inflight[i:], ch.inflight[i+1:])]
	}

	return nil
}
