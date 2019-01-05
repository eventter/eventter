package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"github.com/pkg/errors"
)

func (s *sessionAMQPv1) Disposition(ctx context.Context, frame *v1.Disposition) (err error) {
	var condition v1.ErrorCondition

	{
		if frame.Role != v1.ReceiverRole {
			condition = v1.InvalidFieldAMQPError
			err = errors.New("send role not expected")
			goto Error
		}
		if !frame.Settled {
			condition = v1.NotImplementedAMQPError
			err = errors.New("unsettled disposition not implemented")
			goto Error
		}

		s.mutex.Lock()
		defer s.mutex.Unlock()

		firstIndex := -1
		lastIndex := -1
		for i := 0; i < len(s.inflight); i++ {
			if s.inflight[i].deliveryID < frame.First {
				continue
			}
			if frame.Last != 0 && s.inflight[i].deliveryID > frame.Last {
				break
			}

			if firstIndex == -1 {
				firstIndex = i
			}
			lastIndex = i

			switch frame.State.(type) {
			case *v1.Accepted:
				_, err = s.connection.server.Ack(ctx, &emq.MessageAckRequest{
					NodeID:         s.inflight[i].nodeID,
					SubscriptionID: s.inflight[i].subscriptionID,
					SeqNo:          s.inflight[i].seqNo,
				})
			case *v1.Released:
				_, err = s.connection.server.Nack(ctx, &emq.MessageNackRequest{
					NodeID:         s.inflight[i].nodeID,
					SubscriptionID: s.inflight[i].subscriptionID,
					SeqNo:          s.inflight[i].seqNo,
				})
			default:
				condition = v1.NotImplementedAMQPError
				err = errors.Errorf("state %T not implemented", frame.State)
				goto Error
			}

			if err != nil {
				condition = v1.InternalErrorAMQPError
				err = errors.Wrap(err, "(n)ack failed")
				goto Error
			}
		}

		s.inflight = append(s.inflight[:firstIndex], s.inflight[lastIndex:]...)

		return nil
	}

Error:
	s.state = sessionStateEnding
	return s.Send(&v1.End{
		Error: &v1.Error{
			Condition:   condition,
			Description: err.Error(),
		},
	})
}
