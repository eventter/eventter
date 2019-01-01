package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (l *linkAMQPv1) Detach(ctx context.Context, frame *v1.Detach) error {
	if frame.Error != nil {
		return errors.Errorf(
			"received link error from client [condition=%s, description=%s]",
			frame.Error.Condition,
			frame.Error.Description,
		)
	}

	delete(l.session.links, l.handle)
	err := l.Close()
	var detachError *v1.Error
	if err != nil {
		detachError = &v1.Error{
			Condition:   v1.InternalErrorAMQPError,
			Description: err.Error(),
		}
	}

	err = l.session.Send(&v1.Detach{
		Handle: l.handle,
		Closed: true,
		Error:  detachError,
	})
	return errors.Wrap(err, "send detach failed")
}
