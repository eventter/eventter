package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (l *linkAMQPv1) Flow(ctx context.Context, frame *v1.Flow) error {
	l.session.nextIncomingID = frame.NextOutgoingID
	l.session.remoteOutgoingWindow = frame.OutgoingWindow
	l.session.remoteIncomingWindow = uint32(frame.NextIncomingID) + frame.IncomingWindow - uint32(l.session.nextOutgoingID)
	return errors.New("flow not implemented")
}
