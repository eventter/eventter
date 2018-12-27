package mq

import (
	"context"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/client"
	"eventter.io/mq/structvalue"
	"github.com/hashicorp/go-uuid"
	"github.com/pkg/errors"
)

func (s *Server) handleAMQPv0QueueDeclare(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.QueueDeclare) error {
	size, err := structvalue.Uint32(frame.Arguments, "size", defaultConsumerGroupSize)
	if err != nil {
		return s.makeConnectionClose(v0.SyntaxError, errors.Wrap(err, "size field failed"))
	}

	request := &client.CreateConsumerGroupRequest{
		ConsumerGroup: client.ConsumerGroup{
			Name: client.NamespaceName{
				Namespace: namespaceName,
				Name:      frame.Queue,
			},
			Size_: size,
		},
	}

	if request.ConsumerGroup.Name.Name == "" {
		generated, err := uuid.GenerateUUID()
		if err != nil {
			return errors.Wrap(err, "generate queue name failed")
		}
		request.ConsumerGroup.Name.Name = "amq-" + generated
	}

	state := s.clusterState.Current()
	namespace, _ := state.FindNamespace(request.ConsumerGroup.Name.Namespace)
	if namespace == nil {
		return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("vhost %q not found", request.ConsumerGroup.Name.Namespace))
	}

	cg, _ := namespace.FindConsumerGroup(request.ConsumerGroup.Name.Name)

	if cg != nil {
		for _, binding := range cg.Bindings {
			request.ConsumerGroup.Bindings = append(request.ConsumerGroup.Bindings, s.convertBinding(binding))
		}
	}

	if frame.Passive {
		if cg == nil {
			return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("queue %q not found", request.ConsumerGroup.Name.Name))
		}
	} else {
		_, err = s.CreateConsumerGroup(ctx, request)
		if err != nil {
			return errors.Wrap(err, "create failed")
		}

		_, err = s.ConsumerGroupWait(ctx, &ConsumerGroupWaitRequest{
			ConsumerGroup: request.ConsumerGroup.Name,
		})
		if err != nil {
			return errors.Wrap(err, "wait failed")
		}
	}

	if frame.NoWait {
		return nil
	}

	return transport.Send(&v0.QueueDeclareOk{
		FrameMeta: v0.FrameMeta{Channel: ch.id},
		Queue:     request.ConsumerGroup.Name.Name,
	})
}
