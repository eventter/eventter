package mq

import (
	"context"
	"log"
	"math"
	"time"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"eventter.io/mq/structvalue"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

type connectionAMQPv1 struct {
	server        *Server
	transport     *v1.Transport
	heartbeat     time.Duration
	frames        chan v1.Frame
	receiveErrors chan error
	sessions      map[uint16]*sessionAMQPv1
}

func (c *connectionAMQPv1) Close() error {
	for _, session := range c.sessions {
		err := session.Close()
		if err != nil {
			log.Printf("session close failed with: %v", err)
		}
	}
	return nil
}

func (c *connectionAMQPv1) Read(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			frame, err := c.transport.Receive()
			if err != nil {
				c.receiveErrors <- err
				return
			}
			if frame != nil { // do not send empty (heartbeat) frames
				c.frames <- frame
			}
		}
	}
}

func (c *connectionAMQPv1) Send(frame v1.Frame) error {
	return c.transport.Send(frame)
}

func (c *connectionAMQPv1) force(condition string, description error) error {
	err := c.transport.Send(&v1.Close{Error: &v1.Error{
		Condition:   condition,
		Description: description.Error(),
	}})
	return errors.Wrap(err, "force close failed")
}

func (c *connectionAMQPv1) Run(ctx context.Context) (err error) {
	heartbeats := time.NewTicker(c.heartbeat)
	defer heartbeats.Stop()

	s := c

	for {
		select {
		case <-c.server.closed:
			return s.force(string(v1.ConnectionForcedConnectionError), errors.New("shutdown"))

		case <-heartbeats.C:
			err = c.Send(nil)
			if err != nil {
				return errors.Wrap(err, "send heartbeat failed")
			}

		case receiveErr := <-c.receiveErrors:
			return s.force(string(v1.FramingErrorConnectionError), errors.Wrap(receiveErr, "receive failed"))

		case frame := <-c.frames:
			switch frame := frame.(type) {
			case *v1.Close:
				err = c.Send(&v1.Close{})
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
				if _, ok := c.sessions[frame.FrameMeta.Channel]; ok {
					return s.force(string(v1.IllegalStateAMQPError), errors.Errorf("received begin on channel %d, but session already begun", frame.FrameMeta.Channel))
				}
				namespace, err := structvalue.String((*types.Struct)(frame.Properties), "namespace", emq.DefaultNamespace)
				if err != nil {
					return s.force(string(v1.InvalidFieldAMQPError), errors.Wrapf(err, "get namespace failed"))
				}
				session := &sessionAMQPv1{
					connection:           c,
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
				c.sessions[session.remoteChannel] = session
				err = c.Send(&v1.Begin{
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
				session, ok := c.sessions[frame.FrameMeta.Channel]
				if !ok {
					return s.force(string(v1.IllegalStateAMQPError), errors.Errorf("received end on channel %d, but no session begun",
						frame.FrameMeta.Channel))
				}

				if frame.Error != nil {
					log.Printf(
						"received session error from client [condition=%s, description=%s]",
						frame.Error.Condition,
						frame.Error.Description,
					)
				}

				delete(c.sessions, frame.FrameMeta.Channel)
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
					err = c.Send(&v1.End{FrameMeta: v1.FrameMeta{Channel: session.channel}, Error: endError})
					if err != nil {
						return errors.Wrap(err, "send end failed")
					}
				}

			default:
				meta := frame.GetFrameMeta()
				session, ok := c.sessions[meta.Channel]
				if !ok {
					return s.force(string(v1.IllegalStateAMQPError), errors.Errorf("received attach on channel %d, but no session begun", meta.Channel))
				}

				if session.state != sessionStateClosing {
					err = session.Process(ctx, frame)
					if err != nil {
						session.state = sessionStateClosing
						err = c.Send(&v1.End{
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
