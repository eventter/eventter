package mq

import (
	"context"
	"math"

	"eventter.io/mq/amqp/v1"
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
	var namespaceName, topicName string
	var namespace *ClusterNamespace
	var topic *ClusterTopic

	namespaceName, err = structvalue.String((*types.Struct)(frame.Properties), "namespace", s.namespace)
	if err != nil {
		err = errors.Wrap(err, "get namespace failed")
		goto ImmediateDetach
	}

	namespace, _ = s.server.clusterState.Current().FindNamespace(namespaceName)
	if namespace == nil {
		err = errors.Errorf("namespace %q not found", namespaceName)
		goto ImmediateDetach
	}

	if frame.Target == nil {
		err = errors.New("link has no target")
		goto ImmediateDetach
	}

	switch address := frame.Target.Address.(type) {
	case v1.AddressString:
		topicName = string(address)
	default:
		err = errors.Errorf("unhandled address type %T", address)
		goto ImmediateDetach
	}

	topic, _ = namespace.FindTopic(topicName)
	if topic == nil {
		err = errors.Errorf("topic %q not found", topicName)
		goto ImmediateDetach
	}

	link = &linkAMQPv1{
		session:       s,
		handle:        frame.Handle,
		role:          frame.Role,
		deliveryCount: frame.InitialDeliveryCount,
		linkCredit:    math.MaxUint16,
		namespace:     namespaceName,
		topic:         topicName,
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
	return s.detachImmediately(frame, err)
}

func (s *sessionAMQPv1) attachConsumerGroup(ctx context.Context, frame *v1.Attach) error {
	return errors.New("attach consumer group not implemented")
}

func (s *sessionAMQPv1) detachImmediately(frame *v1.Attach, description error) error {
	frame.Role = !frame.Role
	sendErr := s.Send(frame)
	if sendErr != nil {
		return errors.Wrapf(sendErr, "immediate detach because of (%v) failed", description)
	}
	sendErr = s.Send(&v1.Detach{
		Handle: frame.Handle,
		Closed: true,
		Error: &v1.Error{
			Condition:   string(v1.InternalErrorAMQPError),
			Description: description.Error(),
		},
	})
	return errors.Wrapf(sendErr, "immediate detach because of (%v) failed", description)
}
