package mq

import (
	"context"

	"eventter.io/mq/client"
)

func (s *Server) Publish(ctx context.Context, request *client.PublishRequest) (*client.PublishResponse, error) {
	panic("implement me")
}
