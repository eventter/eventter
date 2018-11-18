package mq

import (
	"context"
)

func (s *Server) taskConsumerGroup(ctx context.Context, namespace string, name string) error {
	<-ctx.Done()
	return nil
}
