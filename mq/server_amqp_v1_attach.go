package mq

import (
	"context"
	"log"
	"math"
	"strings"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"eventter.io/mq/structvalue"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

func (s *sessionAMQPv1) Attach(ctx context.Context, frame *v1.Attach) error {
	if _, ok := s.links[frame.Handle]; ok {
		s.state = sessionStateEnding
		return s.Send(&v1.End{
			Error: &v1.Error{
				Condition:   v1.HandleInUseSessionError,
				Description: errors.Errorf("link handle %v already in use", frame.Handle).Error(),
			},
		})
	}

	if frame.Handle == v1.HandleNull {
		s.state = sessionStateEnding
		return s.Send(&v1.End{
			Error: &v1.Error{
				Condition:   v1.InvalidFieldAMQPError,
				Description: "handle is null",
			},
		})
	}

	if frame.RcvSettleMode == v1.SecondReceiverSettleMode {
		s.state = sessionStateEnding
		return s.Send(&v1.End{
			Error: &v1.Error{
				Condition:   v1.NotImplementedAMQPError,
				Description: "rcv-settle-mode second not implemented",
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
	var namespace, name string
	var condition v1.ErrorCondition = v1.InvalidFieldAMQPError

	{
		if frame.Target == nil {
			err = errors.New("link has no target")
			goto ImmediateDetach
		}

		switch address := frame.Target.Address.(type) {
		case v1.AddressString:
			name = string(address)
		default:
			err = errors.Errorf("unhandled address type %T", address)
			goto ImmediateDetach
		}

		if strings.HasPrefix(name, "/") {
			parts := strings.SplitN(name[1:], "/", 2)
			if len(parts) != 2 {
				err = errors.Errorf("address %q has bad format", name)
				goto ImmediateDetach
			}

			namespace = parts[0]
			name = parts[1]

		} else {
			namespace, err = structvalue.String((*types.Struct)(frame.Properties), "namespace", s.namespace)
			if err != nil {
				err = errors.Wrap(err, "get namespace failed")
				goto ImmediateDetach
			}
		}

		ns, _ := s.connection.server.clusterState.Current().FindNamespace(namespace)
		if ns == nil {
			condition = v1.NotFoundAMQPError
			err = errors.Errorf("namespace %q not found", namespace)
			goto ImmediateDetach
		}

		tp, _ := ns.FindTopic(name)
		if tp == nil {
			condition = v1.NotFoundAMQPError
			err = errors.Errorf("topic %q not found", name)
			goto ImmediateDetach
		}

		link := &topicLinkAMQPV1{
			base: baseLinkAMQPv1{
				state:         linkStateReady,
				session:       s,
				handle:        frame.Handle,
				role:          !frame.Role,
				deliveryCount: frame.InitialDeliveryCount,
				linkCredit:    math.MaxUint16,
			},
			namespace: namespace,
			name:      name,
		}
		link.initialLinkCredit = link.base.linkCredit

		s.links[frame.Handle] = link
		err = s.Send(&v1.Attach{
			Name:          frame.Name,
			Handle:        frame.Handle,
			Role:          !frame.Role,
			SndSettleMode: frame.SndSettleMode,
			RcvSettleMode: frame.RcvSettleMode,
			Source:        frame.Source,
			Target:        frame.Target,
		})
		if err != nil {
			return errors.Wrap(err, "send attach failed")
		}

		s.mutex.Lock()
		err = s.Send(&v1.Flow{
			NextIncomingID: s.nextIncomingID,
			IncomingWindow: s.incomingWindow,
			NextOutgoingID: s.nextOutgoingID,
			OutgoingWindow: s.outgoingWindow,
			Handle:         frame.Handle,
			LinkCredit:     link.base.linkCredit,
		})
		s.mutex.Unlock()
		if err != nil {
			return errors.Wrap(err, "send flow failed")
		}

		return nil
	}

ImmediateDetach:
	return s.detachImmediately(frame, condition, err)
}

func (s *sessionAMQPv1) attachConsumerGroup(ctx context.Context, frame *v1.Attach) (err error) {
	var condition v1.ErrorCondition = v1.InvalidFieldAMQPError

	{
		if frame.Source == nil {
			err = errors.New("link has no source")
			goto ImmediateDetach
		}

		var name string

		switch address := frame.Source.Address.(type) {
		case v1.AddressString:
			name = string(address)
		default:
			err = errors.Errorf("unhandled address type %T", address)
			goto ImmediateDetach
		}

		var namespace string
		if strings.HasPrefix(name, "/") {
			parts := strings.SplitN(name[1:], "/", 2)
			if len(parts) != 2 {
				err = errors.Errorf("address %q has bad format", name)
				goto ImmediateDetach
			}

			namespace = parts[0]
			name = parts[1]

		} else {
			namespace, err = structvalue.String((*types.Struct)(frame.Properties), "namespace", s.namespace)
			if err != nil {
				err = errors.Wrap(err, "get namespace failed")
				goto ImmediateDetach
			}
		}

		ns, _ := s.connection.server.clusterState.Current().FindNamespace(namespace)
		if ns == nil {
			condition = v1.NotFoundAMQPError
			err = errors.Errorf("namespace %q not found", namespace)
			goto ImmediateDetach
		}

		cg, _ := ns.FindConsumerGroup(name)
		if cg == nil {
			condition = v1.NotFoundAMQPError
			err = errors.Errorf("consumer group %q not found", name)
			goto ImmediateDetach
		}

		subscribeCtx, subscribeCancel := context.WithCancel(ctx)
		link := &consumerGroupLinkAMQPv1{
			base: baseLinkAMQPv1{
				state:         linkStateReady,
				session:       s,
				handle:        frame.Handle,
				role:          !frame.Role,
				deliveryCount: v1.SequenceNo(0),
			},
			ctx:    subscribeCtx,
			cancel: subscribeCancel,
		}
		link.cond.L = &link.mutex

		s.links[frame.Handle] = link
		err = s.Send(&v1.Attach{
			Name:                 frame.Name,
			Handle:               frame.Handle,
			Role:                 !frame.Role,
			SndSettleMode:        frame.SndSettleMode,
			RcvSettleMode:        frame.RcvSettleMode,
			Source:               frame.Source,
			Target:               frame.Target,
			InitialDeliveryCount: link.base.deliveryCount,
		})
		if err != nil {
			return errors.Wrap(err, "send attach failed")
		}

		go func() {
			err = s.connection.server.Subscribe(&emq.ConsumerGroupSubscribeRequest{
				Namespace: namespace,
				Name:      name,
				Size_:     1,
				AutoAck:   true, // FIXME
			}, link)
			if err != nil {
				link.base.session.state = linkStateDetaching
				sendErr := link.base.session.Send(&v1.Detach{
					Handle: link.base.handle,
					Closed: true,
					Error: &v1.Error{
						Condition:   v1.InternalErrorAMQPError,
						Description: err.Error(),
					},
				})
				if sendErr != nil {
					log.Printf("detach error link error: %v", sendErr)
				}
			}
		}()

		return nil
	}

ImmediateDetach:
	return s.detachImmediately(frame, condition, err)
}

func (s *sessionAMQPv1) detachImmediately(frame *v1.Attach, condition v1.ErrorCondition, description error) error {
	frame.Role = !frame.Role
	sendErr := s.Send(frame)
	if sendErr != nil {
		return errors.Wrapf(sendErr, "immediate detach because of (%v) failed", description)
	}
	sendErr = s.Send(&v1.Detach{
		Handle: frame.Handle,
		Closed: true,
		Error: &v1.Error{
			Condition:   condition,
			Description: description.Error(),
		},
	})
	return errors.Wrapf(sendErr, "immediate detach because of (%v) failed", description)
}
