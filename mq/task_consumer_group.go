package mq

import (
	"context"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func (s *Server) taskConsumerGroup(ctx context.Context, namespaceName string, consumerGroupName string, segmentID uint64) error {
	// 1) init offset commits from cluster state

	offsets := make(map[uint64]int64)

	state := s.clusterState.Current()

	consumerGroup := state.GetConsumerGroup(namespaceName, consumerGroupName)
	if consumerGroup == nil {
		return errors.Errorf("consumer group %s/%s not found", namespaceName, consumerGroupName)
	}

	for _, commit := range consumerGroup.OffsetCommits {
		offsets[commit.SegmentID] = commit.Offset
	}

	// 2) find all segments with offset commits, update from messages

	segmentHandle, err := s.segmentDir.Open(segmentID)
	if err != nil {
		return errors.Wrap(err, "segment open failed")
	}
	defer s.segmentDir.Release(segmentHandle)

	iterator, err := segmentHandle.Read(false)
	if err != nil {
		return errors.Wrap(err, "segment read failed")
	}

	for {
		buf, _, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "segment next failed")
		}

		commit := ClusterConsumerGroup_OffsetCommit{}
		if err := proto.Unmarshal(buf, &commit); err != nil {
			return errors.Wrap(err, "unmarshal failed")
		}

		if offset, ok := offsets[commit.SegmentID]; ok && commit.Offset > offset {
			offsets[commit.SegmentID] = commit.Offset
		}
	}

	// 3) register as consumer group

	// 4) main loop

	// 4a) process incoming requests to get messages
	// 4b) check for cluster state changes and update offset commits => start/cancel segment consumption
	// 4c) read publishings from segments, add to heap => order by priority & time the message was written to segment

	<-ctx.Done()
	return nil
}
