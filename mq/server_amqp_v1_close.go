package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (c *connectionAMQPv1) RespondClose(ctx context.Context, frame *v1.Close) (err error) {
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
}
