package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (s *sessionAMQPv1) Disposition(ctx context.Context, frame *v1.Disposition) error {
	return errors.New("disposition not implemented")
}
