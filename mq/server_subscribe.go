package mq

import (
	"context"

	"eventter.io/mq/client"
)

func (s *Server) Subscribe(request *client.SubscribeRequest, stream client.EventterMQ_SubscribeServer) error {
	panic("implement me")
}

func (s *Server) Ack(context.Context, *client.AckRequest) (*client.AckResponse, error) {
	panic("implement me")
}

func (s *Server) Nack(context.Context, *client.NackRequest) (*client.NackResponse, error) {
	panic("implement me")
}
