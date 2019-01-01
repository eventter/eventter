package mq

import (
	"context"
	"math"
	"time"

	"eventter.io/mq/about"
	"eventter.io/mq/amqp/v1"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

func (s *Server) ServeAMQPv1(ctx context.Context, transport *v1.Transport) (err error) {
	var clientOpen *v1.Open
	err = transport.Expect(&clientOpen)
	if err != nil {
		return errors.Wrap(err, "expect open failed")
	}

	if clientOpen.MaxFrameSize == 0 {
		clientOpen.MaxFrameSize = math.MaxUint32
	}

	transport.SetFrameMax(clientOpen.MaxFrameSize)

	serverOpen := &v1.Open{
		ContainerID:  about.Name,
		MaxFrameSize: clientOpen.MaxFrameSize,
		ChannelMax:   v1.ChannelMax,
		IdleTimeOut:  v1.Milliseconds(60000),
		Properties: &v1.Fields{Fields: map[string]*types.Value{
			"product": {Kind: &types.Value_StringValue{StringValue: about.Name}},
			"version": {Kind: &types.Value_StringValue{StringValue: about.Version}},
		}},
	}
	if clientOpen.IdleTimeOut < 1000 {
		err = transport.Send(serverOpen)
		if err != nil {
			return errors.Wrap(err, "send open failed")
		}
		err = transport.Send(&v1.Close{Error: &v1.Error{
			Condition:   v1.ResourceLimitExceededAMQPError,
			Description: "client timeout too short",
		}})
		return errors.Wrap(err, "close failed")
	} else if clientOpen.IdleTimeOut > 3600*1000 {
		err = transport.Send(serverOpen)
		if err != nil {
			return errors.Wrap(err, "send open failed")
		}
		err = transport.Send(&v1.Close{Error: &v1.Error{
			Condition:   v1.ResourceLimitExceededAMQPError,
			Description: "client timeout too long",
		}})
		return errors.Wrap(err, "close failed")
	} else {
		// use client-proposed idle timeout
		serverOpen.IdleTimeOut = clientOpen.IdleTimeOut
		err = transport.Send(serverOpen)
		if err != nil {
			return errors.Wrap(err, "send open failed")
		}
	}

	heartbeat := time.Duration(clientOpen.IdleTimeOut) * time.Millisecond / 2
	err = transport.SetReceiveTimeout(heartbeat * 2)
	if err != nil {
		return errors.Wrap(err, "set receive timeout failed")
	}
	err = transport.SetSendTimeout(heartbeat / 2)
	if err != nil {
		return errors.Wrap(err, "set send timeout failed")
	}

	connection := newConnectionAMQPv1(ctx, s, transport, heartbeat)
	defer connection.Close()

	return connection.Run(ctx)
}
