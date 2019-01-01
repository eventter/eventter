package mq

import (
	"eventter.io/mq/amqp/v1"
)

type linkAMQPv1 struct {
	session       *sessionAMQPv1
	handle        v1.Handle
	role          v1.Role
	deliveryCount v1.SequenceNo
	linkCredit    uint32
	available     uint32
	drain         bool
	namespace     string
	topic         string
}

func (l *linkAMQPv1) Close() error {
	return nil
}
