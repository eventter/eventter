package mq

import (
	"context"
	"fmt"
	"reflect"

	"eventter.io/mq/amqp"
	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/client"
	"eventter.io/mq/structvalue"
	"github.com/gogo/protobuf/types"
	"github.com/hashicorp/go-uuid"
	"github.com/pkg/errors"
)

const (
	readyState          = 1
	closingState        = 2
	awaitingHeaderState = 3
	awaitingBodyState   = 4
)

func (s *Server) ServeAMQPv0(ctx context.Context, transport *v0.Transport) error {
	namespace, err := amqp.VirtualHost(ctx)
	if err != nil {
		return errors.Wrap(err, "get virtual host from context")
	} else if namespace == "/" {
		namespace = "default"
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	frames := make(chan v0.Frame, 64)
	errc := make(chan error, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				frame, err := transport.Receive()
				if err != nil {
					errc <- errors.Wrap(err, "receive failed")
					return
				}
				frames <- frame
			}
		}
	}()

	channels := make(map[uint16]*serverAMQPv0Channel)
	defer func() {
		for _, ch := range channels {
			ch.Close()
		}
	}()
	for {
		select {
		case <-s.closeC:
			return s.forceCloseAMQPv0(transport, v0.ConnectionForced, "shutdown")
		case frame := <-frames:
			switch frame := frame.(type) {
			case v0.MethodFrame:
				meta := frame.GetFrameMeta()
				if meta.Channel == 0 {
					switch frame := frame.(type) {
					case *v0.ConnectionClose:
						if frame.ReplyCode != 0 && frame.ReplyCode != v0.ReplySuccess {
							return errors.Errorf("received connection error %q (%d)", frame.ReplyText, frame.ReplyCode)
						}
						return transport.Send(&v0.ConnectionCloseOk{})
					default:
						return s.forceCloseAMQPv0(transport, v0.SyntaxError, "non-close frame")
					}
				} else {
					switch frame.(type) {
					case *v0.ChannelOpen:
						if _, ok := channels[meta.Channel]; ok {
							return s.forceCloseAMQPv0(transport, v0.ChannelError, "channel already open")
						}
						channels[meta.Channel] = &serverAMQPv0Channel{
							id:    meta.Channel,
							state: readyState,
						}
						err := transport.Send(&v0.ChannelOpenOk{FrameMeta: v0.FrameMeta{Channel: meta.Channel}})
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err.Error())
						}
					case *v0.ChannelClose:
						ch, ok := channels[meta.Channel]
						if !ok {
							return s.forceCloseAMQPv0(transport, v0.ChannelError, "trying to close channel that isn't open")
						}
						delete(channels, meta.Channel)
						err := ch.Close()
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err.Error())
						}
						err = transport.Send(&v0.ChannelCloseOk{FrameMeta: v0.FrameMeta{Channel: meta.Channel}})
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err.Error())
						}
					case *v0.ChannelCloseOk:
						ch, ok := channels[meta.Channel]
						if !ok {
							return s.forceCloseAMQPv0(transport, v0.ChannelError, "channel not open")
						}
						if ch.state != closingState {
							return s.forceCloseAMQPv0(transport, v0.SyntaxError, "channel not closing")
						}
						delete(channels, meta.Channel)
						err := ch.Close()
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err.Error())
						}
					default:
						ch, ok := channels[meta.Channel]
						if !ok {
							return s.forceCloseAMQPv0(transport, v0.ChannelError, "channel not open")
						}
						err := s.handleAMQPv0ChannelMethod(ctx, transport, namespace, ch, frame)
						if err != nil {
							if connClose, ok := err.(*v0.ConnectionClose); ok {
								return transport.Send(connClose)
							} else if chanClose, ok := err.(*v0.ChannelClose); ok {
								ch.state = closingState
								err = transport.Send(chanClose)
								if err != nil {
									return s.forceCloseAMQPv0(transport, v0.InternalError, err.Error())
								}
							} else {
								return s.forceCloseAMQPv0(transport, v0.InternalError, err.Error())
							}
						}
					}
				}
			case *v0.ContentHeaderFrame:
				if frame.FrameMeta.Channel == 0 {
					return s.forceCloseAMQPv0(transport, v0.SyntaxError, "content header frame on zero channel")
				}
				ch, ok := channels[frame.FrameMeta.Channel]
				if !ok {
					return s.forceCloseAMQPv0(transport, v0.ChannelError, "channel not open")
				}
				err := s.handleAMQPv0ChannelContentHeader(ctx, transport, namespace, ch, frame)
				if err != nil {
					if connClose, ok := err.(*v0.ConnectionClose); ok {
						return transport.Send(connClose)
					} else if chanClose, ok := err.(*v0.ChannelClose); ok {
						ch.state = closingState
						err = transport.Send(chanClose)
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err.Error())
						}
					} else {
						return s.forceCloseAMQPv0(transport, v0.InternalError, err.Error())
					}
				}
			case *v0.ContentBodyFrame:
				if frame.FrameMeta.Channel == 0 {
					return s.forceCloseAMQPv0(transport, v0.SyntaxError, "content body frame on zero channel")
				}
				ch, ok := channels[frame.FrameMeta.Channel]
				if !ok {
					return s.forceCloseAMQPv0(transport, v0.ChannelError, "channel not open")
				}
				err := s.handleAMQPv0ChannelContentBody(ctx, transport, namespace, ch, frame)
				if err != nil {
					if connClose, ok := err.(*v0.ConnectionClose); ok {
						return transport.Send(connClose)
					} else if chanClose, ok := err.(*v0.ChannelClose); ok {
						ch.state = closingState
						err = transport.Send(chanClose)
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err.Error())
						}
					} else {
						return s.forceCloseAMQPv0(transport, v0.InternalError, err.Error())
					}
				}
			case *v0.HeartbeatFrame:
				if frame.FrameMeta.Channel != 0 {
					return s.forceCloseAMQPv0(transport, v0.SyntaxError, "heartbeat frame on non-zero channel")
				}
			default:
				return s.forceCloseAMQPv0(transport, v0.SyntaxError, fmt.Sprintf("unhandled frame of type %T", frame))
			}
		case err := <-errc:
			cause := errors.Cause(err)
			if cause == v0.ErrMalformedFrame {
				err = s.forceCloseAMQPv0(transport, v0.FrameError, "malformed frame")
			}
			return err
		}
	}
}

type serverAMQPv0Channel struct {
	id                uint16
	state             int
	prefetchCount     int
	publishExchange   string
	publishRoutingKey string
	publishProperties *client.Message_Properties
	publishHeaders    *types.Struct
	publishData       []byte
	publishRemaining  int
}

func (ch *serverAMQPv0Channel) ResetPublish() {
	ch.publishExchange = ""
	ch.publishRoutingKey = ""
	ch.publishProperties = nil
	ch.publishHeaders = nil
	ch.publishData = nil
	ch.publishRemaining = 0
}

func (ch *serverAMQPv0Channel) Close() error {
	return nil
}

func (s *Server) handleAMQPv0ChannelMethod(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame v0.MethodFrame) error {
	if ch.state == closingState {
		return nil
	} else if ch.state != readyState {
		return s.makeConnectionClose(v0.UnexpectedFrame, errors.New("expected method frame"))
	}

	switch frame := frame.(type) {
	case *v0.ExchangeDeclare:
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
				return s.makeConnectionClose(v0.InternalError, errors.Wrap(err, "create failed"))
			}
		}

		if frame.NoWait {
			return nil
		}

		return transport.Send(&v0.ExchangeDeclareOk{
			FrameMeta: v0.FrameMeta{Channel: ch.id},
		})

	case *v0.ExchangeDelete:
		request := &client.DeleteTopicRequest{
			Topic: client.NamespaceName{
				Namespace: namespaceName,
				Name:      frame.Exchange,
			},
			IfUnused: frame.IfUnused,
		}

		_, err := s.DeleteTopic(ctx, request)
		if err != nil {
			return s.makeConnectionClose(v0.InternalError, errors.Wrapf(err, "delete failed"))
		}

		if frame.NoWait {
			return nil
		}

		return transport.Send(&v0.ExchangeDeleteOk{
			FrameMeta: v0.FrameMeta{Channel: ch.id},
		})

	case *v0.QueueDeclare:
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
				return s.makeConnectionClose(v0.InternalError, errors.Wrap(err, "generate queue name failed"))
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
				return s.makeConnectionClose(v0.InternalError, errors.Wrap(err, "create failed"))
			}
		}

		if frame.NoWait {
			return nil
		}

		return transport.Send(&v0.QueueDeclareOk{
			FrameMeta: v0.FrameMeta{Channel: ch.id},
			Queue:     request.ConsumerGroup.Name.Name,
		})

	case *v0.QueueDelete:
		if frame.IfUnused {
			return s.makeConnectionClose(v0.NotImplemented, errors.New("queue.delete if-unused not implemented"))
		}
		if frame.IfEmpty {
			return s.makeConnectionClose(v0.NotImplemented, errors.New("queue.delete if-empty not implemented"))
		}

		request := &client.DeleteConsumerGroupRequest{
			ConsumerGroup: client.NamespaceName{
				Namespace: namespaceName,
				Name:      frame.Queue,
			},
		}

		_, err := s.DeleteConsumerGroup(ctx, request)
		if err != nil {
			return s.makeConnectionClose(v0.InternalError, errors.Wrapf(err, "delete failed"))
		}

		if frame.NoWait {
			return nil
		}

		return transport.Send(&v0.QueueDeleteOk{
			FrameMeta: v0.FrameMeta{Channel: ch.id},
		})

	case *v0.QueueBind:
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

		newBinding := &client.ConsumerGroup_Binding{
			TopicName: tp.Name,
		}

		switch tp.Type {
		case client.ExchangeTypeFanout:
			// do nothing
		case client.ExchangeTypeDirect:
			fallthrough
		case client.ExchangeTypeTopic:
			newBinding.By = &client.ConsumerGroup_Binding_RoutingKey{RoutingKey: frame.RoutingKey}
		case client.ExchangeTypeHeaders:
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
				newBinding.By = &client.ConsumerGroup_Binding_HeadersAll{HeadersAll: frame.Arguments}
			case "any":
				newBinding.By = &client.ConsumerGroup_Binding_HeadersAny{HeadersAny: frame.Arguments}
			default:
				return s.makeConnectionClose(v0.SyntaxError, errors.Errorf("unknown matching algorithm %q", algo))
			}
		default:
			panic("unhandled exchange type")
		}

		request := &client.CreateConsumerGroupRequest{
			ConsumerGroup: client.ConsumerGroup{
				Name: client.NamespaceName{
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
			return s.makeConnectionClose(v0.InternalError, errors.Wrap(err, "create failed"))
		}

		if frame.NoWait {
			return nil
		}

		return transport.Send(&v0.QueueBindOk{
			FrameMeta: v0.FrameMeta{Channel: ch.id},
		})

	case *v0.QueueUnbind:
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

		unbinding := &client.ConsumerGroup_Binding{
			TopicName: tp.Name,
		}

		switch tp.Type {
		case client.ExchangeTypeFanout:
			// do nothing
		case client.ExchangeTypeDirect:
			fallthrough
		case client.ExchangeTypeTopic:
			unbinding.By = &client.ConsumerGroup_Binding_RoutingKey{RoutingKey: frame.RoutingKey}
		case client.ExchangeTypeHeaders:
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
				unbinding.By = &client.ConsumerGroup_Binding_HeadersAll{HeadersAll: frame.Arguments}
			case "any":
				unbinding.By = &client.ConsumerGroup_Binding_HeadersAny{HeadersAny: frame.Arguments}
			default:
				return s.makeConnectionClose(v0.SyntaxError, errors.Errorf("unknown matching algorithm %q", algo))
			}
		default:
			panic("unhandled exchange type")
		}

		request := &client.CreateConsumerGroupRequest{
			ConsumerGroup: client.ConsumerGroup{
				Name: client.NamespaceName{
					Namespace: namespaceName,
					Name:      frame.Queue,
				},
				Size_: cg.Size_,
			},
		}

		for _, binding := range cg.Bindings {
			clientBinding := s.convertBinding(binding)
			if !reflect.DeepEqual(clientBinding, unbinding) {
				request.ConsumerGroup.Bindings = append(request.ConsumerGroup.Bindings, clientBinding)
			}
		}

		_, err := s.CreateConsumerGroup(ctx, request)
		if err != nil {
			return s.makeConnectionClose(v0.InternalError, errors.Wrap(err, "create failed"))
		}

		return transport.Send(&v0.QueueUnbindOk{
			FrameMeta: v0.FrameMeta{Channel: ch.id},
		})

	case *v0.QueuePurge:
		return s.makeConnectionClose(v0.NotImplemented, errors.New("queue.purge not implemented"))

	case *v0.BasicQos:
		// prefetch-size is ignored
		ch.prefetchCount = int(frame.PrefetchCount)
		// global is ignored

		return transport.Send(&v0.BasicQosOk{
			FrameMeta: v0.FrameMeta{Channel: ch.id},
		})

	case *v0.BasicPublish:
		ch.publishExchange = frame.Exchange
		ch.publishRoutingKey = frame.RoutingKey
		ch.state = awaitingHeaderState
		return nil

	case *v0.TxSelect:
		return s.makeConnectionClose(v0.NotImplemented, errors.New("tx.select not implemented"))

	case *v0.TxCommit:
		return s.makeConnectionClose(v0.NotImplemented, errors.New("tx.commit not implemented"))

	case *v0.TxRollback:
		return s.makeConnectionClose(v0.NotImplemented, errors.New("tx.rollback not implemented"))

	default:
		return s.makeConnectionClose(v0.SyntaxError, errors.Errorf("unexpected frame of type %T", frame))
	}
}

func (s *Server) handleAMQPv0ChannelContentHeader(ctx context.Context, transport *v0.Transport, namespace string, ch *serverAMQPv0Channel, frame *v0.ContentHeaderFrame) error {
	if ch.state == closingState {
		return nil
	} else if ch.state != awaitingHeaderState {
		return s.makeConnectionClose(v0.UnexpectedFrame, errors.New("expected header frame"))
	}

	ch.publishRemaining = int(frame.BodySize)

	if frame.ContentType != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.ContentType = frame.ContentType
	}
	if frame.ContentEncoding != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.ContentEncoding = frame.ContentEncoding
	}
	ch.publishHeaders = frame.Headers
	if frame.DeliveryMode != 0 {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.DeliveryMode = int32(frame.DeliveryMode)
	}
	if frame.Priority != 0 {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.Priority = int32(frame.Priority)
	}
	if frame.CorrelationID != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.CorrelationID = frame.CorrelationID
	}
	if frame.ReplyTo != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.ReplyTo = frame.ReplyTo
	}
	if frame.Expiration != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.Expiration = frame.Expiration
	}
	if frame.MessageID != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.MessageID = frame.MessageID
	}
	if !frame.Timestamp.IsZero() {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.Timestamp = frame.Timestamp
	}
	if frame.Type != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.Type = frame.Type
	}
	if frame.UserID != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.UserID = frame.UserID
	}
	if frame.AppID != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &client.Message_Properties{}
		}
		ch.publishProperties.AppID = frame.AppID
	}

	ch.state = awaitingBodyState

	return nil
}

func (s *Server) handleAMQPv0ChannelContentBody(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.ContentBodyFrame) error {
	if ch.state == closingState {
		return nil
	} else if ch.state != awaitingBodyState {
		return s.makeConnectionClose(v0.UnexpectedFrame, errors.New("expected body frame"))
	}

	if len(frame.Data) > ch.publishRemaining {
		return s.makeConnectionClose(v0.FrameError, errors.Errorf("body remaining %d, got %d", ch.publishRemaining, len(frame.Data)))
	}

	ch.publishData = append(ch.publishData, frame.Data...)
	ch.publishRemaining -= len(frame.Data)

	if ch.publishRemaining > 0 {
		return nil
	}

	_, err := s.Publish(ctx, &client.PublishRequest{
		Topic: client.NamespaceName{
			Namespace: namespaceName,
			Name:      ch.publishExchange,
		},
		Message: &client.Message{
			RoutingKey: ch.publishRoutingKey,
			Properties: ch.publishProperties,
			Headers:    ch.publishHeaders,
			Data:       ch.publishData,
		},
	})
	if err != nil {
		return s.makeConnectionClose(v0.InternalError, errors.Wrap(err, "publish failed"))
	}

	ch.ResetPublish()
	ch.state = readyState

	return nil
}

func (s *Server) forceCloseAMQPv0(transport *v0.Transport, replyCode uint16, replyText string) error {
	err := transport.Send(&v0.ConnectionClose{
		ReplyCode: replyCode,
		ReplyText: replyText,
	})
	if err != nil {
		return errors.Wrap(err, "force close failed")
	}
	return nil
}

func (s *Server) makeConnectionClose(code uint16, err error) *v0.ConnectionClose {
	return &v0.ConnectionClose{
		ReplyCode: code,
		ReplyText: err.Error(),
	}
}

func (s *Server) makeChannelClose(ch *serverAMQPv0Channel, code uint16, err error) *v0.ChannelClose {
	return &v0.ChannelClose{
		FrameMeta: v0.FrameMeta{Channel: ch.id},
		ReplyCode: code,
		ReplyText: err.Error(),
	}
}