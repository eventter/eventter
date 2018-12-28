package mq

import (
	"context"
	"reflect"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/emq"
	"eventter.io/mq/structvalue"
	"github.com/pkg/errors"
)

func (s *Server) handleAMQPv0QueueBind(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.QueueBind) error {
	if frame.Exchange == "" {
		return s.makeConnectionClose(v0.SyntaxError, errors.New("trying to bind to default exchange"))
	}

	state := s.clusterState.Current()
	namespace, _ := state.FindNamespace(namespaceName)
	if namespace == nil {
		return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("vhost %q not found", namespaceName))
	}

	cg, _ := namespace.FindConsumerGroup(frame.Queue)
	if cg == nil {
		return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("queue %q not found", frame.Queue))
	}

	tp, _ := namespace.FindTopic(frame.Exchange)
	if tp == nil {
		return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("exchange %q not found", frame.Exchange))
	}

	newBinding := &emq.ConsumerGroup_Binding{
		TopicName: tp.Name,
	}

	switch tp.Type {
	case emq.ExchangeTypeFanout:
		// do nothing
	case emq.ExchangeTypeDirect:
		fallthrough
	case emq.ExchangeTypeTopic:
		newBinding.By = &emq.ConsumerGroup_Binding_RoutingKey{RoutingKey: frame.RoutingKey}
	case emq.ExchangeTypeHeaders:
		if frame.Arguments == nil || frame.Arguments.Fields == nil {
			return s.makeConnectionClose(v0.SyntaxError, errors.New("trying to bind to headers exchange, but arguments not set"))
		}

		algo, err := structvalue.String(frame.Arguments, "x-match", "")
		if err != nil {
			return s.makeConnectionClose(v0.SyntaxError, errors.Wrap(err, "x-match field failed"))
		}

		delete(frame.Arguments.Fields, "x-match")

		switch algo {
		case "":
			return s.makeConnectionClose(v0.SyntaxError, errors.Errorf("trying to bind to headers exchange, but %q not set", "x-match"))
		case "all":
			newBinding.By = &emq.ConsumerGroup_Binding_HeadersAll{HeadersAll: frame.Arguments}
		case "any":
			newBinding.By = &emq.ConsumerGroup_Binding_HeadersAny{HeadersAny: frame.Arguments}
		default:
			return s.makeConnectionClose(v0.SyntaxError, errors.Errorf("unknown matching algorithm %q", algo))
		}
	default:
		panic("unhandled exchange type")
	}

	request := &emq.CreateConsumerGroupRequest{
		ConsumerGroup: emq.ConsumerGroup{
			Name: emq.NamespaceName{
				Namespace: namespaceName,
				Name:      frame.Queue,
			},
			Size_: cg.Size_,
		},
	}

	exists := false
	for _, binding := range cg.Bindings {
		clientBinding := s.convertBinding(binding)
		request.ConsumerGroup.Bindings = append(request.ConsumerGroup.Bindings, clientBinding)
		if reflect.DeepEqual(clientBinding, newBinding) {
			exists = true
		}
	}

	if !exists {
		request.ConsumerGroup.Bindings = append(request.ConsumerGroup.Bindings, newBinding)
	}

	_, err := s.CreateConsumerGroup(ctx, request)
	if err != nil {
		return errors.Wrap(err, "create failed")
	}

	if frame.NoWait {
		return nil
	}

	return transport.Send(&v0.QueueBindOk{
		FrameMeta: v0.FrameMeta{Channel: ch.id},
	})
}
