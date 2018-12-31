package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (s *sessionAMQPv1) Detach(ctx context.Context, frame *v1.Detach) error {
	return errors.New("detach not implemented")
}
