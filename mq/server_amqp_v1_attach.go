package mq

import (
	"context"
	"math"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"eventter.io/mq/structvalue"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

func (s *sessionAMQPv1) Attach(ctx context.Context, frame *v1.Attach) error {
	if _, ok := s.links[frame.Handle]; ok {
		s.state = sessionStateClosing
		return s.Send(&v1.End{
			Error: &v1.Error{
				Condition:   string(v1.HandleInUseSessionError),
				Description: errors.Errorf("link handle %v already in use", frame.Handle).Error(),
			},
		})
	}

	switch frame.Role {
	case v1.SenderRole:
		return s.attachTopic(ctx, frame)
	case v1.ReceiverRole:
		return s.attachConsumerGroup(ctx, frame)
	default:
		return errors.Errorf("unexpected role %v", frame.Role)
	}
}

func (s *sessionAMQPv1) attachTopic(ctx context.Context, frame *v1.Attach) (err error) {
	var link *linkAMQPv1
	request := &emq.CreateTopicRequest{}

	request.Topic.Name.Namespace, err = structvalue.String((*types.Struct)(frame.Properties), "namespace", s.namespace)
	if err != nil {
		err = errors.Wrap(err, "get namespace failed")
		goto ImmediateDetach
	}

	if frame.Target == nil {
		err = errors.New("link has no target")
		goto ImmediateDetach
	}

	switch address := frame.Target.Address.(type) {
	case v1.AddressString:
		request.Topic.Name.Name = string(address)
	default:
		err = errors.Errorf("unhandled address type %T", address)
		goto ImmediateDetach
	}

	request.Topic.Shards, err = structvalue.Uint32((*types.Struct)(frame.Properties), "shards", 1)
	if err != nil {
		err = errors.Wrap(err, "get shards failed")
		goto ImmediateDetach
	}

	request.Topic.ReplicationFactor, err = structvalue.Uint32((*types.Struct)(frame.Properties), "replication-factor", defaultReplicationFactor)
	if err != nil {
		err = errors.Wrap(err, "get replication-factor failed")
		goto ImmediateDetach
	}

	request.Topic.Retention, err = structvalue.Duration((*types.Struct)(frame.Properties), "retention", 1)
	if err != nil {
		err = errors.Wrap(err, "get retention failed")
		goto ImmediateDetach
	}

	request.Topic.DefaultExchangeType, err = structvalue.String((*types.Struct)(frame.Properties), "default-exchange-type", emq.ExchangeTypeFanout)
	if err != nil {
		err = errors.Wrap(err, "get default-exchange-type failed")
		goto ImmediateDetach
	}

	_, err = s.server.CreateTopic(ctx, request)
	if err != nil {
		err = errors.Wrap(err, "create topic failed")
		goto ImmediateDetach
	}

	link = &linkAMQPv1{
		session:       s,
		handle:        frame.Handle,
		role:          frame.Role,
		deliveryCount: frame.InitialDeliveryCount,
		linkCredit:    math.MaxUint16,
		namespace:     request.Topic.Name.Namespace,
		topic:         request.Topic.Name.Name,
	}

	s.links[frame.Handle] = link
	frame.Role = !frame.Role
	err = s.Send(frame)
	if err != nil {
		return errors.Wrap(err, "send attach failed")
	}

	err = s.Send(&v1.Flow{
		NextIncomingID: s.nextIncomingID,
		IncomingWindow: s.incomingWindow,
		NextOutgoingID: s.nextOutgoingID,
		OutgoingWindow: s.outgoingWindow,
		Handle:         frame.Handle,
		LinkCredit:     link.linkCredit,
	})
	if err != nil {
		return errors.Wrap(err, "send flow failed")
	}

	return nil

ImmediateDetach:
	frame.Role = !frame.Role
	sendErr := s.Send(frame)
	if sendErr != nil {
		return errors.Wrapf(sendErr, "immediate detach because of (%v) failed", err)
	}
	sendErr = s.Send(&v1.Detach{
		Handle: frame.Handle,
		Closed: true,
		Error: &v1.Error{
			Condition:   string(v1.InternalErrorAMQPError),
			Description: err.Error(),
		},
	})
	return errors.Wrapf(sendErr, "immediate detach because of (%v) failed", err)
}

func (s *sessionAMQPv1) attachConsumerGroup(ctx context.Context, frame *v1.Attach) error {
	return errors.New("attach consumer group not implemented")
}
