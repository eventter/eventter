package mq

import (
	"context"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/emq"
	"github.com/hashicorp/go-uuid"
	"github.com/pkg/errors"
)

func (s *Server) handleAMQPv0BasicConsume(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.BasicConsume) error {
	if frame.NoLocal {
		return s.makeConnectionClose(v0.NotImplemented, errors.New("no-local not implemented"))
	}
	if frame.Exclusive {
		return s.makeConnectionClose(v0.NotImplemented, errors.New("exclusive not implemented"))
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

	if frame.ConsumerTag == "" {
		generated, err := uuid.GenerateUUID()
		if err != nil {
			return errors.Wrap(err, "generate consumer tag failed")
		}
		frame.ConsumerTag = "ctag-" + generated
	}

	if _, ok := ch.consumers[frame.ConsumerTag]; ok {
		return s.makeChannelClose(ch, v0.PreconditionFailed, errors.Errorf("consumer tag %s already registered", frame.ConsumerTag))
	}

	request := &emq.SubscribeRequest{
		ConsumerGroup: emq.NamespaceName{
			Namespace: namespaceName,
			Name:      frame.Queue,
		},
		Size_:   ch.prefetchCount,
		AutoAck: frame.NoAck,
	}
	stream := newSubscribeConsumer(ctx, ch.id, frame.ConsumerTag, ch.deliveries)

	go func() {
		err := s.Subscribe(request, stream)
		if err != nil {
			ch.subscribeErrors <- err
		}
	}()

	ch.consumers[frame.ConsumerTag] = stream

	if frame.NoWait {
		return nil
	}

	return transport.Send(&v0.BasicConsumeOk{
		FrameMeta:   v0.FrameMeta{Channel: ch.id},
		ConsumerTag: frame.ConsumerTag,
	})
}

func (s *Server) handleAMQPv0ChannelDelivery(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, consumerTag string, response *emq.SubscribeResponse) error {
	if _, ok := ch.consumers[consumerTag]; !ok {
		// ignore deliveries for unknown consumer tags => consumer, hence subscription, was closed, therefore messages
		// will be released and do not need need to be (n)acked
		return nil
	}

	ch.deliveryTag++

	err := transport.Send(&v0.BasicDeliver{
		FrameMeta:   v0.FrameMeta{Channel: ch.id},
		ConsumerTag: consumerTag,
		DeliveryTag: ch.deliveryTag,
		Exchange:    response.Topic.Name,
		RoutingKey:  response.Message.RoutingKey,
	})
	if err != nil {
		return errors.Wrap(err, "send basic.deliver failed")
	}

	err = transport.Send(s.convertAMQPv0ContentHeader(ch, response))
	if err != nil {
		return errors.Wrap(err, "send content header failed")
	}

	err = transport.SendBody(ch.id, response.Message.Data)
	if err != nil {
		return errors.Wrap(err, "send content body failed")
	}

	// node ID / subscription ID / seq no of zero means that the subscription is in auto-ack mode
	// => do not track such in-flight messages as (n)acks won't ever arrive
	if response.NodeID != 0 {
		ch.inflight = append(ch.inflight, serverAMQPv0ChannelInflight{
			deliveryTag:    ch.deliveryTag,
			nodeID:         response.NodeID,
			subscriptionID: response.SubscriptionID,
			seqNo:          response.SeqNo,
		})
	}

	return nil
}
