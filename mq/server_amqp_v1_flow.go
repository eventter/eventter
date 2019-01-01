package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (s *sessionAMQPv1) Flow(ctx context.Context, frame *v1.Flow) error {
	if s.nextOutgoingID != v1.TransferNumberNull {
		s.nextIncomingID = frame.NextOutgoingID
	}
	s.remoteOutgoingWindow = frame.OutgoingWindow
	s.remoteIncomingWindow = uint32(frame.NextIncomingID) + frame.IncomingWindow - uint32(s.nextOutgoingID)
	return errors.New("flow not implemented")
}

func (l *linkAMQPv1) Flow(ctx context.Context, frame *v1.Flow) error {
	return errors.New("flow not implemented")
}
