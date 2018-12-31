package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (s *sessionAMQPv1) Flow(ctx context.Context, frame *v1.Flow) error {
	return errors.New("flow not implemented")
}
