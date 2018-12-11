package mq

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"math/rand"
	"runtime"
	"time"

	"eventter.io/mq/client"
	"eventter.io/mq/consumers"
	"eventter.io/mq/segments"
	"eventter.io/mq/tasks"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func (s *Server) taskConsumerGroup(ctx context.Context, namespaceName string, consumerGroupName string, segmentID uint64) error {
	// 1) init offset commits from cluster state

	committedOffsets := make(map[uint64]int64)

	state := s.clusterState.Current()

	consumerGroup := state.GetConsumerGroup(namespaceName, consumerGroupName)
	if consumerGroup == nil {
		return errors.Errorf(notFoundErrorFormat, entityConsumerGroup, namespaceName, consumerGroupName)
	}
	for _, commit := range consumerGroup.OffsetCommits {
		committedOffsets[commit.SegmentID] = commit.Offset
	}

	// 2) find segment with offset commits, update from messages

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
		buf, off, _, err := iterator.Next()
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

	taskManager := tasks.NewManager(ctx, fmt.Sprintf("consumer group %s/%s", namespaceName, consumerGroupName))
	defer taskManager.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	state = nil // !!! force re-read of state and start of consumption tasks
	running := make(map[uint64]*tasks.Task)

	for {
		var ack consumers.MessageAck
		select {
		case <-ticker.C:
			newState := s.clusterState.Current()
			if newState == state {
				continue
			}

			state = newState
			consumerGroup = state.GetConsumerGroup(namespaceName, consumerGroupName)

			nextCommittedOffsets := make(map[uint64]int64)
			for _, commit := range consumerGroup.OffsetCommits {
				nextCommittedOffsets[commit.SegmentID] = commit.Offset
				if offset, ok := committedOffsets[commit.SegmentID]; ok && offset > commit.Offset {
					nextCommittedOffsets[commit.SegmentID] = offset
				}
			}
			committedOffsets = nextCommittedOffsets

			for offsetSegmentID, task := range running {
				if _, ok := committedOffsets[offsetSegmentID]; !ok {
					task.Cancel()
				}
			}

			for offsetSegmentID, offset := range committedOffsets {
				if _, ok := running[offsetSegmentID]; ok {
					continue
				}

				segment := state.GetSegment(offsetSegmentID)
				if !segment.ClosedAt.IsZero() && offset >= segment.Size_ {
					continue
				}

				if segment.Type != ClusterSegment_TOPIC {
					panic("must not happen")
				}

				local := false
				if segment.Nodes.PrimaryNodeID == s.nodeID {
					local = true
				} else {
					for _, nodeID := range segment.Nodes.DoneNodeIDs {
						if nodeID == s.nodeID {
							local = true
							break
						}
					}
				}

				topic := state.GetTopic(segment.Owner.Namespace, segment.Owner.Name)
				if topic == nil {
					return errors.Errorf(
						"segment %d references unknown topic %s/%s",
						segment.ID,
						segment.Owner.Namespace,
						segment.Owner.Name,
					)
				}

				taskName := fmt.Sprintf("consume segment %d", offsetSegmentID)
				if local {
					running[offsetSegmentID] = taskManager.Start(
						taskName,
						(func(state *ClusterState, consumerGroup *ClusterConsumerGroup, topic *ClusterTopic, segment *ClusterSegment, offset int64) func(context.Context) error {
							return func(ctx context.Context) error {
								return s.taskConsumeSegmentLocal(ctx, state, namespaceName, consumerGroup, topic, segment, group, offset)
							}
						})(state, consumerGroup, topic, segment, offset),
						offsetSegmentID,
					)
				} else if segment.ClosedAt.IsZero() {
					running[offsetSegmentID] = taskManager.Start(
						taskName,
						(func(state *ClusterState, consumerGroup *ClusterConsumerGroup, topic *ClusterTopic, segment *ClusterSegment, nodeID uint64, offset int64) func(context.Context) error {
							return func(ctx context.Context) error {
								return s.taskConsumeSegmentRemote(ctx, state, namespaceName, consumerGroup, topic, segment, group, nodeID, offset)
							}
						})(state, consumerGroup, topic, segment, segment.Nodes.PrimaryNodeID, offset),
						offsetSegmentID,
					)
				} else {
					running[offsetSegmentID] = taskManager.Start(
						taskName,
						(func(state *ClusterState, consumerGroup *ClusterConsumerGroup, topic *ClusterTopic, segment *ClusterSegment, nodeID uint64, offset int64) func(context.Context) error {
							return func(ctx context.Context) error {
								return s.taskConsumeSegmentRemote(ctx, state, namespaceName, consumerGroup, topic, segment, group, nodeID, offset)
							}
						})(state, consumerGroup, topic, segment, segment.Nodes.DoneNodeIDs[rand.Intn(len(segment.Nodes.DoneNodeIDs))], offset),
						offsetSegmentID,
					)
				}
			}

		case completed := <-taskManager.Completed:
			offsetSegmentID := completed.Data.(uint64)

			if t, ok := running[offsetSegmentID]; ok && t.ID == completed.ID {
				delete(running, offsetSegmentID)
				if completed.Err == nil {
					// task completed without error => busy wait for segment to close
					state := s.clusterState.Current()
					segment := state.GetClosedSegment(segmentID)
					for segment == nil {
						select {
						case <-ctx.Done():
							return ctx.Err()
						default:
							runtime.Gosched()
						}
						state = s.clusterState.Current()
						segment = state.GetClosedSegment(segmentID)
					}
					// then commit the whole segment
					ack = consumers.MessageAck{
						SegmentID:    offsetSegmentID,
						CommitOffset: segment.Size_,
					}
					goto COMMIT
				} else {
					state = nil // !!! force re-read of state and possibly restart the task
				}
			}

		case ack = <-group.Ack:
			goto COMMIT

		case <-ctx.Done():
			return ctx.Err()
		}

		continue

	COMMIT:
		if ack.CommitOffset > committedOffsets[ack.SegmentID] {
			committedOffsets[ack.SegmentID] = ack.CommitOffset
		}

		commit := ClusterConsumerGroup_OffsetCommit{}
		commit.SegmentID = ack.SegmentID
		commit.Offset = ack.CommitOffset
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
			if err := s.segmentDir.Release(segmentHandle); err != nil {
				return errors.Wrap(err, "release of old segment failed")
			}
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
	}
}
