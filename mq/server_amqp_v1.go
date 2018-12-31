package mq

import (
	"context"
	"log"
	"math"
	"time"

	"eventter.io/mq/about"
	"eventter.io/mq/amqp/v1"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

func (s *Server) ServeAMQPv1(ctx context.Context, transport *v1.Transport) error {
	var clientOpen *v1.Open
	err := transport.Expect(&clientOpen)
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
		err = transport.Send(&v1.Close{Error: &v1.Error{Condition: "client timeout too short"}})
		return errors.Wrap(err, "close failed")
	} else if clientOpen.IdleTimeOut > 3600*1000 {
		err = transport.Send(serverOpen)
		if err != nil {
			return errors.Wrap(err, "send open failed")
		}
		err = transport.Send(&v1.Close{Error: &v1.Error{Condition: "client timeout too long"}})
		return errors.Wrap(err, "close failed")
	} else {
		// use client-proposed idle timeout
		serverOpen.IdleTimeOut = clientOpen.IdleTimeOut
		err = transport.Send(serverOpen)
		if err != nil {
			return errors.Wrap(err, "send open failed")
		}
	}

	heartbeat := time.Duration(clientOpen.IdleTimeOut) * time.Millisecond
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
				frames <- frame
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
			return s.forceCloseAMQPv1(transport, errors.New("shutdown"))
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
						errors.Errorf("received begin on channel %d, but session already begun", frame.FrameMeta.Channel),
					)
				}
				session := &sessionAMQPv1{
					remoteChannel:        frame.FrameMeta.Channel,
					remoteNextOutgoingID: frame.NextOutgoingID,
					remoteIncomingWindow: frame.IncomingWindow,
					remoteOutgoingWindow: frame.OutgoingWindow,
					channel:              frame.FrameMeta.Channel, // just use the same channel as client
					nextOutgoingID:       v1.TransferNumber(0),
					incomingWindow:       math.MaxUint16,
					outgoingWindow:       math.MaxUint16,
				}
				sessions[session.remoteChannel] = session
				err = transport.Send(&v1.Begin{
					FrameMeta:      v1.FrameMeta{Channel: session.channel},
					RemoteChannel:  session.remoteChannel,
					NextOutgoingID: session.nextOutgoingID,
					IncomingWindow: session.incomingWindow,
					OutgoingWindow: session.outgoingWindow,
					HandleMax:      math.MaxUint32,
				})
				if err != nil {
					return errors.Wrap(err, "send begin failed")
				}

			case *v1.End:
				session, ok := sessions[frame.FrameMeta.Channel]
				if !ok {
					return s.forceCloseAMQPv1(
						transport,
						errors.Errorf("received end on channel %d, but no session begun", frame.FrameMeta.Channel),
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
					endError = &v1.Error{Condition: errors.Wrap(err, "session close failed").Error()}
				}

				err = transport.Send(&v1.End{FrameMeta: v1.FrameMeta{Channel: session.channel}, Error: endError})
				if err != nil {
					return errors.Wrap(err, "send end failed")
				}

			default:
				panic("wip")
			}
		case <-heartbeats.C:
			err = transport.Send(nil)
			if err != nil {
				return errors.Wrap(err, "send heartbeat failed")
			}
		}
	}
}

func (s *Server) forceCloseAMQPv1(transport *v1.Transport, reason error) error {
	err := transport.Send(&v1.Close{Error: &v1.Error{Condition: reason.Error()}})
	return errors.Wrap(err, "force close failed")
}

type sessionAMQPv1 struct {
	remoteChannel        uint16
	remoteNextOutgoingID v1.TransferNumber
	remoteIncomingWindow uint32
	remoteOutgoingWindow uint32
	channel              uint16
	nextOutgoingID       v1.TransferNumber
	incomingWindow       uint32
	outgoingWindow       uint32
}

func (s *sessionAMQPv1) Close() error {
	return nil
}
