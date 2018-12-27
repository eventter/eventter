package mq

import (
	"context"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/client"
	"github.com/pkg/errors"
)

func (s *Server) handleAMQPv0BasicGet(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.BasicGet) error {
	state := s.clusterState.Current()
	namespace, _ := state.FindNamespace(namespaceName)
	if namespace == nil {
		return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("vhost %q not found", namespaceName))
	}
	cg, _ := namespace.FindConsumerGroup(frame.Queue)
	if cg == nil {
		return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("queue %q not found", frame.Queue))
	}

	request := &client.SubscribeRequest{
		ConsumerGroup: client.NamespaceName{
			Namespace: namespaceName,
			Name:      frame.Queue,
		},
		Size_:       1,
		AutoAck:     frame.NoAck,
		DoNotBlock:  true,
		MaxMessages: 1,
	}
	stream := newSubscribeConsumer(ctx, ch.id, "", nil)

	go func() {
		defer stream.Close()
		err := s.Subscribe(request, stream)
		if err != nil {
			ch.subscribeErrors <- err
			return
		}
	}()

	delivery, ok := <-stream.C
	if !ok {
		return transport.Send(&v0.BasicGetEmpty{
			FrameMeta: v0.FrameMeta{Channel: ch.id},
		})
	}

	ch.deliveryTag++

	err := transport.Send(&v0.BasicGetOk{
		FrameMeta:   v0.FrameMeta{Channel: ch.id},
		DeliveryTag: ch.deliveryTag,
		Exchange:    delivery.Response.Topic.Name,
		RoutingKey:  delivery.Response.Message.RoutingKey,
	})
	if err != nil {
		return errors.Wrap(err, "send basic.get-ok failed")
	}

	err = transport.Send(s.convertAMQPv0ContentHeader(ch, delivery.Response))
	if err != nil {
		return errors.Wrap(err, "send content header failed")
	}

	err = transport.SendBody(ch.id, delivery.Response.Message.Data)
	if err != nil {
		return errors.Wrap(err, "send content body failed")
	}

	// node ID / subscription ID / seq no of zero means that the subscription is in auto-ack mode
	// => do not track such in-flight messages as (n)acks won't ever arrive
	if delivery.Response.NodeID != 0 {
		ch.inflight = append(ch.inflight, serverAMQPv0ChannelInflight{
			deliveryTag:    ch.deliveryTag,
			nodeID:         delivery.Response.NodeID,
			subscriptionID: delivery.Response.SubscriptionID,
			seqNo:          delivery.Response.SeqNo,
		})
	}

	return nil
}
