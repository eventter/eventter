package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (s *sessionAMQPv1) Attach(ctx context.Context, frame *v1.Attach) error {
	return errors.New("attach not implemented")
}
