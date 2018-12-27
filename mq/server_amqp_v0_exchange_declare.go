package mq

import (
	"context"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/client"
	"eventter.io/mq/structvalue"
	"github.com/pkg/errors"
)

func (s *Server) handleAMQPv0ExchangeDeclare(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.ExchangeDeclare) error {
	if frame.Exchange == "" || frame.Exchange == defaultExchangeTopicName {
		return s.makeConnectionClose(v0.SyntaxError, errors.New("trying to declare default exchange"))
	}
	if !frame.Durable {
		return s.makeConnectionClose(v0.NotImplemented, errors.New("non-durable exchanges not implemented"))
	}
	if frame.AutoDelete {
		return s.makeConnectionClose(v0.NotImplemented, errors.New("auto-delete exchanges not implemented"))
	}
	if frame.Internal {
		return s.makeConnectionClose(v0.NotImplemented, errors.New("internal exchanges not implemented"))
	}

	shards, err := structvalue.Uint32(frame.Arguments, "shards", 1)
	if err != nil {
		return s.makeConnectionClose(v0.SyntaxError, errors.Wrap(err, "shards field failed"))
	}
	replicationFactor, err := structvalue.Uint32(frame.Arguments, "replication-factor", defaultReplicationFactor)
	if err != nil {
		return s.makeConnectionClose(v0.SyntaxError, errors.Wrap(err, "replication-factor field failed"))
	}
	retention, err := structvalue.Duration(frame.Arguments, "retention", 1)
	if err != nil {
		return s.makeConnectionClose(v0.SyntaxError, errors.Wrap(err, "retention field failed"))
	}

	request := &client.CreateTopicRequest{
		Topic: client.Topic{
			Name: client.NamespaceName{
				Namespace: namespaceName,
				Name:      frame.Exchange,
			},
			Type:              frame.Type,
			Shards:            shards,
			ReplicationFactor: replicationFactor,
			Retention:         retention,
		},
	}

	state := s.clusterState.Current()
	namespace, _ := state.FindNamespace(request.Topic.Name.Namespace)
	if namespace == nil {
		return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("vhost %q not found", request.Topic.Name.Namespace))
	}

	if frame.Passive {
		topic, _ := namespace.FindTopic(request.Topic.Name.Name)
		if topic == nil {
			return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("exchange %q not found", request.Topic.Name.Name))
		}

	} else {
		_, err = s.CreateTopic(ctx, request)
		if err != nil {
			return errors.Wrap(err, "create failed")
		}
	}

	if frame.NoWait {
		return nil
	}

	return transport.Send(&v0.ExchangeDeclareOk{
		FrameMeta: v0.FrameMeta{Channel: ch.id},
	})
}
