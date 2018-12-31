package mq

import (
	"context"
	"log"
	"math"
	"time"

	"eventter.io/mq/about"
	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"eventter.io/mq/structvalue"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

func (s *Server) ServeAMQPv1(ctx context.Context, transport *v1.Transport) (err error) {
	var clientOpen *v1.Open
	err = transport.Expect(&clientOpen)
	if err != nil {
		return errors.Wrap(err, "expect open failed")
	}

	if clientOpen.MaxFrameSize == 0 {
		clientOpen.MaxFrameSize = math.MaxUint32
	}

	transport.SetFrameMax(clientOpen.MaxFrameSize)

	serverOpen := &v1.Open{
		ContainerID:  about.Name,
		MaxFrameSize: clientOpen.MaxFrameSize,
		ChannelMax:   v1.ChannelMax,
		IdleTimeOut:  v1.Milliseconds(60000),
		Properties: &v1.Fields{Fields: map[string]*types.Value{
			"product": {Kind: &types.Value_StringValue{StringValue: about.Name}},
			"version": {Kind: &types.Value_StringValue{StringValue: about.Version}},
		}},
	}
	if clientOpen.IdleTimeOut < 1000 {
		err = transport.Send(serverOpen)
		if err != nil {
			return errors.Wrap(err, "send open failed")
		}
		err = transport.Send(&v1.Close{Error: &v1.Error{
			Condition:   string(v1.ResourceLimitExceededAMQPError),
			Description: "client timeout too short",
		}})
		return errors.Wrap(err, "close failed")
	} else if clientOpen.IdleTimeOut > 3600*1000 {
		err = transport.Send(serverOpen)
		if err != nil {
			return errors.Wrap(err, "send open failed")
		}
		err = transport.Send(&v1.Close{Error: &v1.Error{
			Condition:   string(v1.ResourceLimitExceededAMQPError),
			Description: "client timeout too long",
		}})
		return errors.Wrap(err, "close failed")
	} else {
		// use client-proposed idle timeout
		serverOpen.IdleTimeOut = clientOpen.IdleTimeOut
		err = transport.Send(serverOpen)
		if err != nil {
			return errors.Wrap(err, "send open failed")
		}
	}

	heartbeat := time.Duration(clientOpen.IdleTimeOut) * time.Millisecond / 2
	err = transport.SetReceiveTimeout(heartbeat * 2)
	if err != nil {
		return errors.Wrap(err, "set receive timeout failed")
	}
	err = transport.SetSendTimeout(heartbeat / 2)
	if err != nil {
		return errors.Wrap(err, "set send timeout failed")
	}

	frames := make(chan v1.Frame, 64)
	receiveErrors := make(chan error, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				frame, err := transport.Receive()
				if err != nil {
					receiveErrors <- err
					return
				}
				if frame != nil { // do not send empty (heartbeat) frames
					frames <- frame
				}
			}
		}
	}()

	heartbeats := time.NewTicker(heartbeat)
	defer heartbeats.Stop()

	sessions := make(map[uint16]*sessionAMQPv1)
	defer func() {
		for _, session := range sessions {
			session.Close()
		}
	}()

	for {
		select {
		case <-s.closed:
			return s.forceCloseAMQPv1(transport, string(v1.ConnectionForcedConnectionError), errors.New("shutdown"))

		case <-heartbeats.C:
			err = transport.Send(nil)
			if err != nil {
				return errors.Wrap(err, "send heartbeat failed")
			}

		case receiveErr := <-receiveErrors:
			return s.forceCloseAMQPv1(transport, string(v1.FramingErrorConnectionError), errors.Wrap(receiveErr, "receive failed"))

		case frame := <-frames:
			switch frame := frame.(type) {
			case *v1.Close:
				err = transport.Send(&v1.Close{})
				if err != nil {
					return errors.Wrap(err, "send close failed")
				}
				if frame.Error != nil {
					return errors.Errorf(
						"received connection error from client [condition=%s, description=%s]",
						frame.Error.Condition,
						frame.Error.Description,
					)
				}
				return nil

			case *v1.Begin:
				if _, ok := sessions[frame.FrameMeta.Channel]; ok {
					return s.forceCloseAMQPv1(
						transport,
						string(v1.IllegalStateAMQPError),
						errors.Errorf("received begin on channel %d, but session already begun", frame.FrameMeta.Channel),
					)
				}
				namespace, err := structvalue.String((*types.Struct)(frame.Properties), "namespace", emq.DefaultNamespace)
				if err != nil {
					return s.forceCloseAMQPv1(
						transport,
						string(v1.InvalidFieldAMQPError),
						errors.Wrapf(err, "get namespace failed"),
					)
				}
				session := &sessionAMQPv1{
					server:               s,
					transport:            transport,
					state:                sessionStateReady,
					remoteChannel:        frame.FrameMeta.Channel,
					nextIncomingID:       frame.NextOutgoingID,
					remoteIncomingWindow: frame.IncomingWindow,
					remoteOutgoingWindow: frame.OutgoingWindow,
					channel:              frame.FrameMeta.Channel, // just use the same channel as client
					nextOutgoingID:       v1.TransferNumber(0),
					incomingWindow:       math.MaxUint16,
					outgoingWindow:       math.MaxUint16,
					namespace:            namespace,
					links:                make(map[v1.Handle]*linkAMQPv1),
				}
				sessions[session.remoteChannel] = session
				err = transport.Send(&v1.Begin{
					FrameMeta:      v1.FrameMeta{Channel: session.channel},
					RemoteChannel:  session.remoteChannel,
					NextOutgoingID: session.nextOutgoingID,
					IncomingWindow: session.incomingWindow,
					OutgoingWindow: session.outgoingWindow,
					HandleMax:      v1.HandleMax,
				})
				if err != nil {
					return errors.Wrap(err, "send begin failed")
				}

			case *v1.End:
				session, ok := sessions[frame.FrameMeta.Channel]
				if !ok {
					return s.forceCloseAMQPv1(
						transport,
						string(v1.IllegalStateAMQPError),
						errors.Errorf("received end on channel %d, but no session begun",
							frame.FrameMeta.Channel),
					)
				}

				if frame.Error != nil {
					log.Printf(
						"received session error from client [condition=%s, description=%s]",
						frame.Error.Condition,
						frame.Error.Description,
					)
				}

				delete(sessions, frame.FrameMeta.Channel)
				err = session.Close()
				var endError *v1.Error
				if err != nil {
					endError = &v1.Error{
						Condition:   string(v1.InternalErrorAMQPError),
						Description: err.Error(),
					}
					if session.state == sessionStateClosing {
						log.Printf("session close failed: %s", err)
					}
				}

				if session.state != sessionStateClosing {
					err = transport.Send(&v1.End{FrameMeta: v1.FrameMeta{Channel: session.channel}, Error: endError})
					if err != nil {
						return errors.Wrap(err, "send end failed")
					}
				}

			default:
				meta := frame.GetFrameMeta()
				session, ok := sessions[meta.Channel]
				if !ok {
					return s.forceCloseAMQPv1(
						transport,
						string(v1.IllegalStateAMQPError),
						errors.Errorf("received attach on channel %d, but no session begun", meta.Channel),
					)
				}

				if session.state != sessionStateClosing {
					err = session.Process(ctx, frame)
					if err != nil {
						session.state = sessionStateClosing
						err = transport.Send(&v1.End{
							FrameMeta: v1.FrameMeta{Channel: session.channel},
							Error: &v1.Error{
								Condition:   string(v1.InternalErrorAMQPError),
								Description: err.Error(),
							},
						})
						if err != nil {
							return errors.Wrap(err, "send end failed")
						}
					}
				}
			}
		}
	}
}

func (s *Server) forceCloseAMQPv1(transport *v1.Transport, condition string, description error) error {
	err := transport.Send(&v1.Close{Error: &v1.Error{
		Condition:   condition,
		Description: description.Error(),
	}})
	return errors.Wrap(err, "force close failed")
}

const (
	sessionStateReady   = 0
	sessionStateClosing = 1
)

type sessionAMQPv1 struct {
	server               *Server
	transport            *v1.Transport
	state                int
	remoteChannel        uint16
	channel              uint16
	nextIncomingID       v1.TransferNumber
	incomingWindow       uint32
	nextOutgoingID       v1.TransferNumber
	outgoingWindow       uint32
	remoteIncomingWindow uint32
	remoteOutgoingWindow uint32
	namespace            string
	links                map[v1.Handle]*linkAMQPv1
}

func (s *sessionAMQPv1) Send(frame v1.Frame) error {
	meta := frame.GetFrameMeta()
	meta.Channel = s.channel
	return s.transport.Send(frame)
}

func (s *sessionAMQPv1) Close() error {
	return nil
}

func (s *sessionAMQPv1) Process(ctx context.Context, frame v1.Frame) (err error) {
	var handle v1.Handle

	switch frame := frame.(type) {
	case *v1.Attach:
		return s.Attach(ctx, frame)
	case *v1.Detach:
		return s.Detach(ctx, frame)
	case *v1.Flow:
		link, ok := s.links[frame.Handle]
		if !ok {
			handle = frame.Handle
			goto EndHandleNotFound
		}
		err = link.Flow(ctx, frame)
		if err != nil {
			goto EndErrantLink
		}
		return nil
	case *v1.Disposition:
		return s.Disposition(ctx, frame)
	case *v1.Transfer:
		link, ok := s.links[frame.Handle]
		if !ok {
			handle = frame.Handle
			goto EndHandleNotFound
		}
		err = link.Transfer(ctx, frame)
		if err != nil {
			goto EndErrantLink
		}
		return nil
	default:
		return errors.Errorf("unexpected frame %T", frame)
	}

EndHandleNotFound:
	s.state = sessionStateClosing
	return s.Send(&v1.End{
		Error: &v1.Error{
			Condition:   string(v1.UnattachedHandleSessionError),
			Description: errors.Errorf("link handle %v not found", handle).Error(),
		},
	})

EndErrantLink:
	s.state = sessionStateClosing
	return s.Send(&v1.End{
		Error: &v1.Error{
			Condition:   string(v1.ErrantLinkSessionError),
			Description: err.Error(),
		},
	})
}

type linkAMQPv1 struct {
	session       *sessionAMQPv1
	handle        v1.Handle
	role          v1.Role
	deliveryCount v1.SequenceNo
	linkCredit    uint32
	available     uint32
	drain         bool
	namespace     string
	topic         string
}

func (l *linkAMQPv1) Close() error {
	return nil
}
