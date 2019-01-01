package mq

import (
	"context"
	"log"
	"time"

	"eventter.io/mq/amqp/v1"
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

func newConnectionAMQPv1(ctx context.Context, server *Server, transport *v1.Transport, heartbeat time.Duration) *connectionAMQPv1 {
	c := &connectionAMQPv1{
		server:        server,
		transport:     transport,
		heartbeat:     heartbeat,
		frames:        make(chan v1.Frame, 64),
		receiveErrors: make(chan error, 1),
		sessions:      make(map[uint16]*sessionAMQPv1),
	}
	go c.read(ctx)
	return c
}

func (c *connectionAMQPv1) read(ctx context.Context) {
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

func (c *connectionAMQPv1) Close() error {
	for _, session := range c.sessions {
		err := session.Close()
		if err != nil {
			log.Printf("session close failed with: %v", err)
		}
	}
	return nil
}

func (c *connectionAMQPv1) Send(frame v1.Frame) error {
	return c.transport.Send(frame)
}

func (c *connectionAMQPv1) forceClose(condition v1.ErrorCondition, description error) error {
	err := c.Send(&v1.Close{Error: &v1.Error{
		Condition:   condition,
		Description: description.Error(),
	}})
	return errors.Wrap(err, "force close failed")
}

func (c *connectionAMQPv1) Run(ctx context.Context) (err error) {
	heartbeats := time.NewTicker(c.heartbeat)
	defer heartbeats.Stop()

	for {
		select {
		case <-c.server.closed:
			return c.forceClose(v1.ConnectionForcedConnectionError, errors.New("shutdown"))

		case <-heartbeats.C:
			err = c.Send(nil)
			if err != nil {
				return errors.Wrap(err, "send heartbeat failed")
			}

		case receiveErr := <-c.receiveErrors:
			return c.forceClose(v1.FramingErrorConnectionError, errors.Wrap(receiveErr, "receive failed"))

		case frame := <-c.frames:
			switch frame := frame.(type) {
			case *v1.Close:
				return c.RespondClose(ctx, frame)
			case *v1.Begin:
				err = c.Begin(ctx, frame)
				if err != nil {
					return err
				}
			case *v1.End:
				err = c.End(ctx, frame)
				if err != nil {
					return err
				}
			default:
				meta := frame.GetFrameMeta()
				session, ok := c.sessions[meta.Channel]
				if !ok {
					return c.forceClose(
						v1.IllegalStateAMQPError,
						errors.Errorf("received frame %T on channel %d, but no session begun", frame, meta.Channel),
					)
				}

				if session.state != sessionStateClosing {
					err = session.Process(ctx, frame)
					if err != nil {
						session.state = sessionStateClosing
						err = c.Send(&v1.End{
							FrameMeta: v1.FrameMeta{Channel: session.channel},
							Error: &v1.Error{
								Condition:   v1.InternalErrorAMQPError,
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
