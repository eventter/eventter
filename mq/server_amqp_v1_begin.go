package mq

import (
	"context"
	"math"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"eventter.io/mq/structvalue"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

func (c *connectionAMQPv1) Begin(ctx context.Context, frame *v1.Begin) (err error) {
	if _, ok := c.sessions[frame.FrameMeta.Channel]; ok {
		return c.forceClose(v1.IllegalStateAMQPError, errors.Errorf("received begin on channel %d, but session already begun", frame.FrameMeta.Channel))
	}

	namespace, err := structvalue.String((*types.Struct)(frame.Properties), "namespace", emq.DefaultNamespace)
	if err != nil {
		return c.forceClose(v1.InvalidFieldAMQPError, errors.Wrapf(err, "get namespace failed"))
	}

	session := &sessionAMQPv1{
		connection:           c,
		state:                sessionStateReady,
		remoteChannel:        frame.FrameMeta.Channel,
		nextIncomingID:       frame.NextOutgoingID,
		incomingWindow:       math.MaxUint16,
		nextOutgoingID:       v1.TransferNumber(0),
		outgoingWindow:       math.MaxUint16,
		remoteIncomingWindow: frame.IncomingWindow,
		remoteOutgoingWindow: frame.OutgoingWindow,
		channel:              frame.FrameMeta.Channel, // just use the same channel as client
		namespace:            namespace,
		links:                make(map[v1.Handle]*linkAMQPv1),
	}
	session.initialIncomingWindow = session.incomingWindow

	c.sessions[session.remoteChannel] = session
	err = c.Send(&v1.Begin{
		FrameMeta:      v1.FrameMeta{Channel: session.channel},
		RemoteChannel:  session.remoteChannel,
		NextOutgoingID: session.nextOutgoingID,
		IncomingWindow: session.incomingWindow,
		OutgoingWindow: session.outgoingWindow,
		HandleMax:      v1.HandleMax,
	})
	if err != nil {
		return errors.Wrap(err, "send begin failed")
	}

	return nil
}
