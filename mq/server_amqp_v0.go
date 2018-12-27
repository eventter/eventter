package mq

import (
	"context"

	"eventter.io/mq/amqp"
	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/client"
	"github.com/gogo/protobuf/types"
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
	receiveErrors := make(chan error, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				frame, err := transport.Receive()
				if err != nil {
					receiveErrors <- errors.Wrap(err, "receive failed")
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

	subscribeErrors := make(chan error, 1)
	deliveries := make(chan subscribeDelivery)

	for {
		select {
		case <-s.closed:
			return s.forceCloseAMQPv0(transport, v0.ConnectionForced, errors.New("shutdown"))
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
						return s.forceCloseAMQPv0(transport, v0.SyntaxError, errors.New("non-close frame"))
					}
				} else {
					switch frame.(type) {
					case *v0.ChannelOpen:
						if _, ok := channels[meta.Channel]; ok {
							return s.forceCloseAMQPv0(transport, v0.ChannelError, errors.New("channel already open"))
						}
						channels[meta.Channel] = &serverAMQPv0Channel{
							id:              meta.Channel,
							state:           readyState,
							subscribeErrors: subscribeErrors,
							deliveries:      deliveries,
							consumers:       make(map[string]*subscribeConsumer),
						}
						err := transport.Send(&v0.ChannelOpenOk{FrameMeta: v0.FrameMeta{Channel: meta.Channel}})
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err)
						}
					case *v0.ChannelClose:
						ch, ok := channels[meta.Channel]
						if !ok {
							return s.forceCloseAMQPv0(transport, v0.ChannelError, errors.New("trying to close channel that isn't open"))
						}
						delete(channels, meta.Channel)
						err := ch.Close()
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err)
						}
						err = transport.Send(&v0.ChannelCloseOk{FrameMeta: v0.FrameMeta{Channel: meta.Channel}})
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err)
						}
					case *v0.ChannelCloseOk:
						ch, ok := channels[meta.Channel]
						if !ok {
							return s.forceCloseAMQPv0(transport, v0.ChannelError, errors.New("channel not open"))
						}
						if ch.state != closingState {
							return s.forceCloseAMQPv0(transport, v0.SyntaxError, errors.New("channel not closing"))
						}
						delete(channels, meta.Channel)
						err := ch.Close()
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err)
						}
					default:
						ch, ok := channels[meta.Channel]
						if !ok {
							return s.forceCloseAMQPv0(transport, v0.ChannelError, errors.New("channel not open"))
						}
						err := s.handleAMQPv0ChannelMethod(ctx, transport, namespace, ch, frame)
						if err != nil {
							if connClose, ok := err.(*v0.ConnectionClose); ok {
								return transport.Send(connClose)
							} else if chanClose, ok := err.(*v0.ChannelClose); ok {
								ch.state = closingState
								err = transport.Send(chanClose)
								if err != nil {
									return s.forceCloseAMQPv0(transport, v0.InternalError, err)
								}
							} else {
								return s.forceCloseAMQPv0(transport, v0.InternalError, err)
							}
						}
					}
				}
			case *v0.ContentHeaderFrame:
				if frame.FrameMeta.Channel == 0 {
					return s.forceCloseAMQPv0(transport, v0.SyntaxError, errors.New("content header frame on zero channel"))
				}
				ch, ok := channels[frame.FrameMeta.Channel]
				if !ok {
					return s.forceCloseAMQPv0(transport, v0.ChannelError, errors.New("channel not open"))
				}
				err := s.handleAMQPv0ChannelContentHeader(ctx, transport, namespace, ch, frame)
				if err != nil {
					if connClose, ok := err.(*v0.ConnectionClose); ok {
						return transport.Send(connClose)
					} else if chanClose, ok := err.(*v0.ChannelClose); ok {
						ch.state = closingState
						err = transport.Send(chanClose)
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err)
						}
					} else {
						return s.forceCloseAMQPv0(transport, v0.InternalError, err)
					}
				}
			case *v0.ContentBodyFrame:
				if frame.FrameMeta.Channel == 0 {
					return s.forceCloseAMQPv0(transport, v0.SyntaxError, errors.New("content body frame on zero channel"))
				}
				ch, ok := channels[frame.FrameMeta.Channel]
				if !ok {
					return s.forceCloseAMQPv0(transport, v0.ChannelError, errors.New("channel not open"))
				}
				err := s.handleAMQPv0ChannelContentBody(ctx, transport, namespace, ch, frame)
				if err != nil {
					if connClose, ok := err.(*v0.ConnectionClose); ok {
						return transport.Send(connClose)
					} else if chanClose, ok := err.(*v0.ChannelClose); ok {
						ch.state = closingState
						err = transport.Send(chanClose)
						if err != nil {
							return s.forceCloseAMQPv0(transport, v0.InternalError, err)
						}
					} else {
						return s.forceCloseAMQPv0(transport, v0.InternalError, err)
					}
				}
			case *v0.HeartbeatFrame:
				if frame.FrameMeta.Channel != 0 {
					return s.forceCloseAMQPv0(transport, v0.SyntaxError, errors.New("heartbeat frame on non-zero channel"))
				}
				return transport.Send(&v0.HeartbeatFrame{})
			default:
				return s.forceCloseAMQPv0(transport, v0.SyntaxError, errors.Errorf("unhandled frame of type %T", frame))
			}
		case err := <-receiveErrors:
			cause := errors.Cause(err)
			if cause == v0.ErrMalformedFrame {
				err = s.forceCloseAMQPv0(transport, v0.FrameError, errors.New("malformed frame"))
			}
			return err
		case err := <-subscribeErrors:
			return s.forceCloseAMQPv0(transport, v0.InternalError, errors.Wrap(err, "subscribe failed"))
		case delivery := <-deliveries:
			ch, ok := channels[delivery.Channel]
			if !ok {
				// ignore deliveries on closed channels => consumer, hence subscription, will be closed by channel,
				// therefore messages will be released and do not need need to be (n)acked
				continue
			}
			err := s.handleAMQPv0ChannelDelivery(ctx, transport, namespace, ch, delivery.ConsumerTag, delivery.Response)
			if err != nil {
				if connClose, ok := err.(*v0.ConnectionClose); ok {
					return transport.Send(connClose)
				} else if chanClose, ok := err.(*v0.ChannelClose); ok {
					ch.state = closingState
					err = transport.Send(chanClose)
					if err != nil {
						return s.forceCloseAMQPv0(transport, v0.InternalError, err)
					}
				} else {
					return s.forceCloseAMQPv0(transport, v0.InternalError, err)
				}
			}
		}
	}
}

type serverAMQPv0Channel struct {
	id                uint16
	state             int
	prefetchCount     uint32
	publishExchange   string
	publishRoutingKey string
	publishProperties *client.Message_Properties
	publishHeaders    *types.Struct
	publishData       []byte
	publishRemaining  int
	subscribeErrors   chan error
	consumers         map[string]*subscribeConsumer
	deliveries        chan subscribeDelivery
	deliveryTag       uint64
	inflight          []serverAMQPv0ChannelInflight
}

type serverAMQPv0ChannelInflight struct {
	deliveryTag    uint64
	nodeID         uint64
	subscriptionID uint64
	seqNo          uint64
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
	for _, consumer := range ch.consumers {
		consumer.Close()
	}
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
		return s.handleAMQPv0ExchangeDeclare(ctx, transport, namespaceName, ch, frame)
	case *v0.ExchangeDelete:
		return s.handleAMQPv0ExchangeDelete(ctx, transport, namespaceName, ch, frame)
	case *v0.QueueDeclare:
		return s.handleAMQPv0QueueDeclare(ctx, transport, namespaceName, ch, frame)
	case *v0.QueueDelete:
		return s.handleAMQPv0QueueDelete(ctx, transport, namespaceName, ch, frame)
	case *v0.QueueBind:
		return s.handleAMQPv0QueueBind(ctx, transport, namespaceName, ch, frame)
	case *v0.QueueUnbind:
		return s.handleAMQPv0QueueUnbind(ctx, transport, namespaceName, ch, frame)
	case *v0.QueuePurge:
		return s.makeConnectionClose(v0.NotImplemented, errors.New("queue.purge not implemented"))
	case *v0.BasicQos:
		return s.handleAMQPv0BasicQos(ctx, transport, namespaceName, ch, frame)
	case *v0.BasicConsume:
		return s.handleAMQPv0BasicConsume(ctx, transport, namespaceName, ch, frame)
	case *v0.BasicCancel:
		return s.handleAMQPv0BasicCancel(ctx, transport, namespaceName, ch, frame)
	case *v0.BasicPublish:
		return s.handleAMQPv0BasicPublish(ctx, transport, namespaceName, ch, frame)
	case *v0.BasicGet:
		return s.handleAMQPv0BasicGet(ctx, transport, namespaceName, ch, frame)
	case *v0.BasicAck:
		return s.handleAMQPv0BasicAck(ctx, transport, namespaceName, ch, frame)
	case *v0.BasicReject:
		return s.handleAMQPv0BasicReject(ctx, transport, namespaceName, ch, frame)
	case *v0.BasicRecover:
		return s.makeConnectionClose(v0.NotImplemented, errors.New("basic.recover not implemented"))
	case *v0.BasicRecoverAsync:
		return s.makeConnectionClose(v0.NotImplemented, errors.New("basic.recover-async not implemented"))
	case *v0.BasicNack:
		return s.handleAMQPv0BasicNack(ctx, transport, namespaceName, ch, frame)
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

func (s *Server) convertAMQPv0ContentHeader(ch *serverAMQPv0Channel, response *client.SubscribeResponse) *v0.ContentHeaderFrame {
	contentHeader := &v0.ContentHeaderFrame{
		FrameMeta: v0.FrameMeta{Channel: ch.id},
		ClassID:   v0.BasicClass,
		BodySize:  uint64(len(response.Message.Data)),
	}
	if response.Message.Properties != nil {
		contentHeader.ContentType = response.Message.Properties.ContentType
		contentHeader.ContentEncoding = response.Message.Properties.ContentEncoding
		contentHeader.DeliveryMode = uint8(response.Message.Properties.DeliveryMode)
		contentHeader.Priority = uint8(response.Message.Properties.Priority)
		contentHeader.CorrelationID = response.Message.Properties.CorrelationID
		contentHeader.ReplyTo = response.Message.Properties.ReplyTo
		contentHeader.Expiration = response.Message.Properties.Expiration
		contentHeader.MessageID = response.Message.Properties.MessageID
		contentHeader.Timestamp = response.Message.Properties.Timestamp
		contentHeader.Type = response.Message.Properties.Type
		contentHeader.UserID = response.Message.Properties.UserID
		contentHeader.AppID = response.Message.Properties.AppID
	}
	contentHeader.Headers = response.Message.Headers
	return contentHeader
}

func (s *Server) forceCloseAMQPv0(transport *v0.Transport, code uint16, err error) error {
	err = transport.Send(&v0.ConnectionClose{
		ReplyCode: code,
		ReplyText: err.Error(),
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
