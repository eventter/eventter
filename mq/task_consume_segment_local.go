package mq

import (
	"context"
	"io"
	"time"

	"eventter.io/mq/consumers"
	"eventter.io/mq/segments"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func (s *Server) taskConsumeSegmentLocal(ctx context.Context, group *consumers.Group, state *ClusterState, segment *ClusterSegment, startOffset int64) error {
	topic := state.GetTopic(segment.Owner.Namespace, segment.Owner.Name)
	if topic == nil {
		return errors.Errorf(notFoundErrorFormat, entityTopic, segment.Owner.Namespace, segment.Owner.Name)
	}

	segmentHandle, err := s.segmentDir.Open(segment.ID)
	if err != nil {
		return errors.Wrap(err, "segment open failed")
	}
	defer s.segmentDir.Release(segmentHandle)

	var iterator *segments.Iterator
	if startOffset > 0 {
		iterator, err = segmentHandle.ReadAt(startOffset, segment.ClosedAt.IsZero())
	} else {
		iterator, err = segmentHandle.Read(segment.ClosedAt.IsZero())
	}
	if err != nil {
		return errors.Wrap(err, "segment read failed")
	}

	go func() {
		<-ctx.Done()
		iterator.Close()
	}()

	for {
		data, _, commitOffset, err := iterator.Next()
		if err == io.EOF {
			return nil
		} else if err == segments.ErrIteratorClosed && ctx.Err() != nil {
			return ctx.Err()
		} else if err != nil {
			return errors.Wrap(err, "iterator next failed")
		}

		publishing := Publishing{}
		if err := proto.Unmarshal(data, &publishing); err != nil {
			return errors.Wrap(err, "unmarshal failed")
		}

		// TODO: filter according to topic type

		err = group.Offer(&consumers.Message{
			Topic:        segment.Owner,
			SegmentID:    segment.ID,
			CommitOffset: commitOffset,
			Time:         segment.OpenedAt.Add(time.Duration(publishing.Delta)),
			Message:      publishing.Message,
		})
		if err != nil {
			return errors.Wrap(err, "offer failed")
		}
	}
}
