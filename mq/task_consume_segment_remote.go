package mq

import (
	"context"

	"eventter.io/mq/consumers"
	"github.com/pkg/errors"
)

func (s *Server) taskConsumeSegmentRemote(ctx context.Context, group *consumers.Group, state *ClusterState, segment *ClusterSegment, nodeID uint64, startOffset int64) error {
	node := state.GetNode(nodeID)
	if node == nil {
		return errors.Errorf("node %d not found", nodeID)
	}
	topic := state.GetTopic(segment.Owner.Namespace, segment.Owner.Name)

	_ = topic

	// TODO

	<-ctx.Done()

	return ctx.Err()
}
