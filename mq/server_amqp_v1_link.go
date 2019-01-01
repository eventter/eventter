package mq

import (
	"bytes"

	"eventter.io/mq/amqp/v1"
)

const (
	linkStateReady     = 0
	linkStateDetaching = 1
)

type linkAMQPv1 struct {
	state             int
	session           *sessionAMQPv1
	handle            v1.Handle
	role              v1.Role
	deliveryCount     v1.SequenceNo
	initialLinkCredit uint32
	linkCredit        uint32
	available         uint32
	drain             bool
	namespace         string
	topic             string
	currentTransfer   *v1.Transfer
	buf               bytes.Buffer
}

func (l *linkAMQPv1) Close() error {
	return nil
}
