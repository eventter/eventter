package mq

import (
	"context"

	"eventter.io/mq/client"
	"google.golang.org/grpc/metadata"
)

type subscribeConsumer struct {
	C             chan subscribeDelivery
	closeDelivery bool
	channel       uint16
	consumerTag   string
	ctx           context.Context
	cancel        func()
}

type subscribeDelivery struct {
	Channel     uint16
	ConsumerTag string
	Response    *client.SubscribeResponse
}

func newSubscribeConsumer(ctx context.Context, channel uint16, consumerTag string, deliveries chan subscribeDelivery) *subscribeConsumer {
	ctx, cancel := context.WithCancel(ctx)

	closeDelivery := false

	if deliveries == nil {
		deliveries = make(chan subscribeDelivery)
		closeDelivery = true
	}

	return &subscribeConsumer{
		C:             deliveries,
		closeDelivery: closeDelivery,
		channel:       channel,
		consumerTag:   consumerTag,
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (s *subscribeConsumer) Send(response *client.SubscribeResponse) error {
	s.C <- subscribeDelivery{
		Channel:     s.channel,
		ConsumerTag: s.consumerTag,
		Response:    response,
	}
	return nil
}

func (s *subscribeConsumer) SetHeader(metadata.MD) error {
	panic("local")
}

func (s *subscribeConsumer) SendHeader(metadata.MD) error {
	panic("local")
}

func (s *subscribeConsumer) SetTrailer(metadata.MD) {
	panic("local")
}

func (s *subscribeConsumer) Context() context.Context {
	return s.ctx
}

func (s *subscribeConsumer) SendMsg(m interface{}) error {
	panic("local")
}

func (s *subscribeConsumer) RecvMsg(m interface{}) error {
	panic("local")
}

func (s *subscribeConsumer) Close() error {
	s.cancel()
	if s.closeDelivery {
		close(s.C)
	}
	return nil
}
