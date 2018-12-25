package mq

import (
	"context"
	"fmt"
	"time"

	"eventter.io/mq/amqp"
	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/client"
	"eventter.io/mq/structvalue"
	"github.com/pkg/errors"
)

const (
	readyState   = 1
	closingState = 2
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

	heartbeat, err := amqp.Heartbeat(ctx)
	if err != nil {
		return errors.Wrap(err, "get heartbeat from context")
	}
	var heartbeats <-chan time.Time
	if heartbeat > 0 {
		heartbeatTicker := time.NewTicker(heartbeat)
		defer heartbeatTicker.Stop()
		heartbeats = heartbeatTicker.C
	}

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
		case <-heartbeats:
			err := transport.Send(&v0.HeartbeatFrame{})
			if err != nil {
				return errors.Wrap(err, "send heartbeat failed")
			}
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
						err := s.handleAMQPv0ChannelMethod(nil, transport, namespace, ch, frame)
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
				err := s.handleAMQPv0ChannelContentHeader(nil, transport, namespace, ch, frame)
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
				err := s.handleAMQPv0ChannelContentBody(nil, transport, namespace, ch, frame)
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
	id    uint16
	state int
}

func (ch *serverAMQPv0Channel) Close() error {
	return nil
}

func (s *Server) handleAMQPv0ChannelMethod(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame v0.MethodFrame) error {
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

		if frame.Passive {
			state := s.clusterState.Current()
			namespace, _ := state.FindNamespace(request.Topic.Name.Namespace)
			if namespace == nil {
				return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("namespace %q not found", request.Topic.Name.Namespace))
			}

			topic, _ := namespace.FindTopic(request.Topic.Name.Name)
			if topic == nil {
				return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("topic %q not found", request.Topic.Name.Name))
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

		return transport.Send(&v0.ExchangeDeclareOk{FrameMeta: v0.FrameMeta{Channel: ch.id}})

	default:
		return s.makeConnectionClose(v0.SyntaxError, errors.Errorf("unexpected frame of type %T", frame))
	}
}

func (s *Server) handleAMQPv0ChannelContentHeader(ctx context.Context, transport *v0.Transport, namespace string, ch *serverAMQPv0Channel, frame *v0.ContentHeaderFrame) error {
	panic("implement me")
}

func (s *Server) handleAMQPv0ChannelContentBody(ctx context.Context, transport *v0.Transport, namespace string, ch *serverAMQPv0Channel, frame *v0.ContentBodyFrame) error {
	panic("implement me")
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
