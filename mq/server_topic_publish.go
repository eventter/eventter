package mq

import (
	"context"
	"crypto/sha1"
	"runtime"
	"sync/atomic"
	"time"

	"eventter.io/mq/emq"
	"eventter.io/mq/segments"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func (s *Server) Publish(ctx context.Context, request *emq.PublishRequest) (*emq.PublishResponse, error) {
	if err := request.Validate(); err != nil {
		return nil, errors.Wrap(err, "validation failed")
	}

	state := s.clusterState.Current()

	namespace, _ := state.FindNamespace(request.Topic.Namespace)
	if namespace == nil {
		return nil, errors.Errorf(namespaceNotFoundErrorFormat, request.Topic.Namespace)
	}

	topic, _ := namespace.FindTopic(request.Topic.Name)
	if topic == nil {
		return nil, errors.Errorf(notFoundErrorFormat, entityTopic, request.Topic.Namespace, request.Topic.Name)
	}

	var (
		localSegmentID uint64
		forwardNodeID  uint64
	)

	openSegments := state.FindOpenSegmentsFor(ClusterSegment_TOPIC, request.Topic.Namespace, request.Topic.Name)
	for _, openSegment := range openSegments {
		if openSegment.Nodes.PrimaryNodeID == s.nodeID {
			localSegmentID = openSegment.ID
			break
		}
	}

	if localSegmentID == 0 {
		if topic.Shards > 0 && uint32(len(openSegments)) >= topic.Shards {
			n := atomic.AddUint32(&s.publishForwardRR, 1)
			n %= uint32(len(openSegments))
			segment := openSegments[n]
			forwardNodeID = segment.Nodes.PrimaryNodeID
			goto Forward

		} else {
			response, err := s.SegmentOpen(ctx, &SegmentOpenRequest{
				NodeID: s.nodeID,
				Owner:  request.Topic,
				Type:   ClusterSegment_TOPIC,
			})
			if err != nil {
				return nil, errors.Wrap(err, "segment open failed")
			}

			if response.PrimaryNodeID != s.nodeID {
				forwardNodeID = response.PrimaryNodeID
				goto Forward
			}

			localSegmentID = response.SegmentID
		}
	}

	{
		segment := state.GetOpenSegment(localSegmentID)
		// cluster state from leader wasn't applied yet to this node => busy wait for new state
		for segment == nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				runtime.Gosched()
			}
			state = s.clusterState.Current()
			segment = state.GetOpenSegment(localSegmentID)
		}

		segmentHandle, err := s.segmentDir.Open(localSegmentID)
		if err != nil {
			return nil, errors.Wrap(err, "segment open failed")
		}
		defer func() {
			if segmentHandle != nil { // check to prevent double-free
				s.segmentDir.Release(segmentHandle)
			}
		}()

		publishing := Publishing{
			Message: request.Message,
			Delta:   int64(time.Now().Sub(segment.CreatedAt)),
		}

		// possible clock skew => move time to segment open time
		if publishing.Delta < 0 {
			publishing.Delta = 0
		}

		buf, err := proto.Marshal(&publishing)
		if err != nil {
			return nil, errors.Wrap(err, "marshal failed")
		}

	Write:
		if err := segmentHandle.Write(buf); err == segments.ErrFull {
			sha1Sum, size, err := segmentHandle.Sum(sha1.New(), segments.SumAll)
			if err != nil {
				return nil, errors.Wrap(err, "segment sum failed")
			}
			response, err := s.SegmentRotate(ctx, &SegmentCloseRequest{
				NodeID:    s.nodeID,
				SegmentID: localSegmentID,
				Size_:     size,
				Sha1:      sha1Sum,
			})
			if err != nil {
				return nil, errors.Wrap(err, "segment rotate failed")
			}

			if response.PrimaryNodeID != s.nodeID {
				forwardNodeID = response.PrimaryNodeID
				goto Forward
			}

			s.segmentDir.Release(segmentHandle)
			segmentHandle = nil // nilled to prevent double-free
			localSegmentID = response.SegmentID
			segmentHandle, err = s.segmentDir.Open(localSegmentID)
			if err != nil {
				return nil, errors.Wrap(err, "rotated segment open failed")
			}
			goto Write

		} else if err != nil {
			return nil, errors.Wrap(err, "segment write failed")
		}

		if segmentHandle.IsFull() {
			sha1Sum, size, err := segmentHandle.Sum(sha1.New(), segments.SumAll)
			if err != nil {
				return nil, errors.Wrap(err, "segment sum failed")
			}
			_, err = s.SegmentClose(ctx, &SegmentCloseRequest{
				NodeID:    s.nodeID,
				SegmentID: localSegmentID,
				Size_:     size,
				Sha1:      sha1Sum,
			})
			if err != nil {
				return nil, errors.Wrap(err, "segment close failed")
			}
		}

		return &emq.PublishResponse{
			OK: true,
		}, nil
	}

Forward:
	{
		if request.DoNotForward {
			return nil, errWontForward
		}

		node := state.GetNode(forwardNodeID)
		// cluster state from leader wasn't applied yet to this node => busy wait for new state
		for node == nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				runtime.Gosched()
			}
			state = s.clusterState.Current()
			node = state.GetNode(forwardNodeID)
		}

		if node.State != ClusterNode_ALIVE {
			return nil, errForwardNodeDead
		}

		conn, err := s.pool.Get(ctx, node.Address)
		if err != nil {
			return nil, err
		}
		defer s.pool.Put(conn)

		request.DoNotForward = true
		return emq.NewEventterMQClient(conn).Publish(ctx, request)
	}
}
