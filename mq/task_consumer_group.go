package mq

import (
	"context"
	"crypto/sha1"
	"io"
	"log"
	"time"

	"eventter.io/mq/client"
	"eventter.io/mq/consumers"
	"eventter.io/mq/segments"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func (s *Server) taskConsumerGroup(ctx context.Context, namespaceName string, consumerGroupName string, segmentID uint64) error {
	// 1) init offset commits from cluster state

	committedOffsets := make(map[uint64]int64)

	state := s.clusterState.Current()

	consumerGroup := state.GetConsumerGroup(namespaceName, consumerGroupName)
	if consumerGroup == nil {
		return errors.Errorf("consumer group %s/%s not found", namespaceName, consumerGroupName)
	}
	for _, commit := range consumerGroup.OffsetCommits {
		committedOffsets[commit.SegmentID] = commit.Offset
	}

	// 2) find all segments with offset commits, update from messages

	segmentHandle, err := s.segmentDir.Open(segmentID)
	if err != nil {
		return errors.Wrap(err, "segment open failed")
	}
	defer func() {
		if segmentHandle != nil { // check to prevent double-free
			s.segmentDir.Release(segmentHandle)
		}
	}()

	iterator, err := segmentHandle.Read(false)
	if err != nil {
		return errors.Wrap(err, "segment read failed")
	}
	commit := ClusterConsumerGroup_OffsetCommit{}
	for {
		buf, off, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "segment next failed")
		}

		if err := proto.Unmarshal(buf, &commit); err != nil {
			return errors.Wrapf(err, "unmarshal failed in segment %d at %d", segmentID, off)
		}

		if offset, ok := committedOffsets[commit.SegmentID]; ok && commit.Offset > offset {
			committedOffsets[commit.SegmentID] = commit.Offset
		}
	}
	iterator = nil

	// 3) register as consumer group

	group, err := consumers.NewGroup(int(consumerGroup.Size_))
	if err != nil {
		return errors.Wrap(err, "group create failed")
	}

	s.groupMutex.Lock()
	mapKey := namespaceName + "/" + consumerGroupName
	s.groupMap[mapKey] = group
	s.groupMutex.Unlock()
	defer func() {
		group.Close()

		s.groupMutex.Lock()
		delete(s.groupMap, mapKey)
		s.groupMutex.Unlock()
	}()

	// 4) main loop

	ticker := time.NewTicker(100 * time.Millisecond)
	state = nil

	for {
		select {
		case <-ticker.C:
			newState := s.clusterState.Current()
			if newState != state {
				state = newState
				consumerGroup = state.GetConsumerGroup(namespaceName, consumerGroupName)
				for _, commit := range consumerGroup.OffsetCommits {
					if offset, ok := committedOffsets[commit.SegmentID]; !ok || commit.Offset > offset {
						committedOffsets[commit.SegmentID] = commit.Offset
						log.Printf(
							"consumer group %s/%s added/updated commit offset from cluster state for segment %d: %d",
							namespaceName,
							consumerGroupName,
							commit.SegmentID,
							commit.Offset,
						)
					}
				}
			}

			// TODO: (re)start segment consumption

		case ack := <-group.Ack:
			if ack.Offset > committedOffsets[ack.SegmentID] {
				committedOffsets[ack.SegmentID] = ack.Offset
			}

			commit.Reset()
			commit.SegmentID = ack.SegmentID
			commit.Offset = ack.Offset

			buf, err := proto.Marshal(&commit)
			if err != nil {
				return errors.Wrap(err, "marshal failed")
			}

		WRITE:
			if err := segmentHandle.Write(buf); err == segments.ErrFull {
				sha1Sum, size, err := segmentHandle.Sum(sha1.New(), segments.SumAll)
				if err != nil {
					return errors.Wrap(err, "sum failed")
				}
				var offsetCommitsUpdate []*ClusterConsumerGroup_OffsetCommit
				for segmentID, offset := range committedOffsets {
					offsetCommitsUpdate = append(offsetCommitsUpdate, &ClusterConsumerGroup_OffsetCommit{
						SegmentID: segmentID,
						Offset:    offset,
					})
				}
				response, err := s.SegmentRotate(ctx, &SegmentCloseRequest{
					OffsetCommitsUpdate: &ClusterUpdateOffsetCommitsCommand{
						ConsumerGroup: client.NamespaceName{
							Namespace: namespaceName,
							Name:      consumerGroupName,
						},
						OffsetCommits: offsetCommitsUpdate,
					},
					NodeID:    s.nodeID,
					SegmentID: segmentID,
					Size_:     size,
					Sha1:      sha1Sum,
				})
				if response.PrimaryNodeID != s.nodeID {
					log.Printf(
						"consumer group %s/%s rotated segment (%d->%d) assigned to different node: %d",
						namespaceName,
						consumerGroupName,
						segmentID,
						response.SegmentID,
						response.PrimaryNodeID,
					)
					return nil
				}
				s.segmentDir.Release(segmentHandle)
				segmentHandle = nil // nilled to prevent double-free
				segmentID = response.SegmentID
				segmentHandle, err = s.segmentDir.Open(segmentID)
				if err != nil {
					return errors.Wrap(err, "rotated segment open failed")
				}
				goto WRITE

			} else if err != nil {
				return errors.Wrap(err, "segment write failed")
			}

		case <-ctx.Done():
			return nil
		}
	}
}
