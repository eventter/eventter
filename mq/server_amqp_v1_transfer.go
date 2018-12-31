package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (l *linkAMQPv1) Transfer(ctx context.Context, frame *v1.Transfer) error {
	return errors.New("transfer not implemented")
}
