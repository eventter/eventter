package mq

import (
	"context"
	"log"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (c *connectionAMQPv1) End(ctx context.Context, frame *v1.End) (err error) {
	session, ok := c.sessions[frame.FrameMeta.Channel]
	if !ok {
		return c.forceClose(string(v1.IllegalStateAMQPError), errors.Errorf("received end on channel %d, but no session begun",
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

	return nil
}
