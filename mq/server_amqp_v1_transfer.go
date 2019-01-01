package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (l *linkAMQPv1) Transfer(ctx context.Context, frame *v1.Transfer) error {
	if l.role != v1.ReceiverRole {
		return l.session.Send(&v1.Detach{
			Handle: l.handle,
			Closed: true,
			Error: &v1.Error{
				Condition:   v1.TransferLimitExceededLinkError,
				Description: "not receiving endpoint of the link",
			},
		})
	}

	return errors.New("transfer not implemented")
}
