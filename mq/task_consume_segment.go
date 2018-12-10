package mq

import (
	"context"

	"eventter.io/mq/consumers"
	"github.com/pkg/errors"
)

func (s *Server) taskConsumeSegment(ctx context.Context, group *consumers.Group, segmentID uint64, offset int64) error {
	state := s.clusterState.Current()

	segment := state.GetSegment(segmentID)
	if segment.Type != ClusterSegment_TOPIC {
		return errors.Errorf("unexpected type [%s] of segment [%d]", segment.Type, segmentID)
	}
	topic := state.GetTopic(segment.Owner.Namespace, segment.Owner.Name)

	_ = topic

	// TODO

	<-ctx.Done()

	return nil
}
