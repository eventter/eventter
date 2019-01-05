package mq

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (s *sessionAMQPv1) Flow(ctx context.Context, frame *v1.Flow) error {
	s.mutex.Lock()
	s.nextIncomingID = frame.NextOutgoingID
	s.remoteOutgoingWindow = frame.OutgoingWindow
	if frame.NextIncomingID == v1.TransferNumberNull {
		s.remoteIncomingWindow = uint32(s.initialOutgoingID) + frame.IncomingWindow - uint32(s.nextOutgoingID)
	} else {
		s.remoteIncomingWindow = uint32(frame.NextIncomingID) + frame.IncomingWindow - uint32(s.nextOutgoingID)
	}
	s.cond.Broadcast()
	s.mutex.Unlock()
	return nil
}

func (l *topicLinkAMQPV1) Flow(ctx context.Context, frame *v1.Flow) error {
	return errors.New("did not expect flow on topic link")
}

func (l *consumerGroupLinkAMQPv1) Flow(ctx context.Context, frame *v1.Flow) error {
	l.mutex.Lock()
	l.base.linkCredit = uint32(frame.DeliveryCount) + frame.LinkCredit - uint32(l.base.deliveryCount)
	l.cond.Broadcast()
	l.mutex.Unlock()
	return nil
}
