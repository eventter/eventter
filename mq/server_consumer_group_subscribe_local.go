package mq

import (
	"context"

	"eventter.io/mq/client"
	"google.golang.org/grpc/metadata"
)

type SubscribeStreamLocal struct {
	C      chan *client.SubscribeResponse
	ctx    context.Context
	cancel func()
}

func NewSubscribeStreamLocal(ctx context.Context) *SubscribeStreamLocal {
	ctx, cancel := context.WithCancel(ctx)
	return &SubscribeStreamLocal{
		C:      make(chan *client.SubscribeResponse),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *SubscribeStreamLocal) Send(m *client.SubscribeResponse) error {
	s.C <- m
	return nil
}

func (s *SubscribeStreamLocal) SetHeader(metadata.MD) error {
	panic("local")
}

func (s *SubscribeStreamLocal) SendHeader(metadata.MD) error {
	panic("local")
}

func (s *SubscribeStreamLocal) SetTrailer(metadata.MD) {
	panic("local")
}

func (s *SubscribeStreamLocal) Context() context.Context {
	return s.ctx
}

func (s *SubscribeStreamLocal) SendMsg(m interface{}) error {
	panic("local")
}

func (s *SubscribeStreamLocal) RecvMsg(m interface{}) error {
	panic("local")
}

func (s *SubscribeStreamLocal) Close() error {
	s.cancel()
	return nil
}
