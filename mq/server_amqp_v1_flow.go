package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
)

func (s *sessionAMQPv1) Flow(ctx context.Context, frame *v1.Flow) error {
	s.nextIncomingID = frame.NextOutgoingID
	s.remoteOutgoingWindow = frame.OutgoingWindow
	if frame.NextIncomingID == v1.TransferNumberNull {
		s.remoteIncomingWindow = uint32(s.initialOutgoingID) + frame.IncomingWindow - uint32(s.nextOutgoingID)
	} else {
		s.remoteIncomingWindow = uint32(frame.NextIncomingID) + frame.IncomingWindow - uint32(s.nextOutgoingID)
	}
	return nil
}

func (l *linkAMQPv1) Flow(ctx context.Context, frame *v1.Flow) error {
	if l.role == v1.SenderRole {
		l.linkCredit = uint32(frame.DeliveryCount) + frame.LinkCredit - uint32(l.deliveryCount)
	}
	return nil
}
