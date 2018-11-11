package mq

import (
	"context"
	"sync/atomic"

	"eventter.io/mq/client"
	"eventter.io/mq/segmentfile"
	"github.com/pkg/errors"
)

func (s *Server) Publish(ctx context.Context, request *client.PublishRequest) (*client.PublishResponse, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}

	state := s.clusterState.Current()

	topic := state.GetTopic(request.Topic.Namespace, request.Topic.Name)
	if topic == nil {
		return nil, errors.Errorf(notFoundErrorFormat, entityTopic, request.Topic.Namespace, request.Topic.Name)
	}

	var (
		localMessageID = s.idGenerator.New()
		localSegmentID uint64
		forwardNodeID  uint64
	)

	openSegments := state.FindOpenSegmentsFor(request.Topic.Namespace, request.Topic.Name)
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
			goto FORWARD

		} else {
			response, err := s.OpenSegment(ctx, &OpenSegmentRequest{
				NodeID:         s.nodeID,
				Topic:          request.Topic,
				FirstMessageID: localMessageID.Bytes(),
			})
			if err != nil {
				return nil, err
			}

			if response.PrimaryNodeID != s.nodeID {
				forwardNodeID = response.PrimaryNodeID
				goto FORWARD
			}

			localSegmentID = response.SegmentID
		}
	}

WRITE:
	{
		segment, err := s.segmentDir.Open(localSegmentID)
		if err != nil {
			return nil, err
		}
		defer s.segmentDir.Release(segment)

		err = segment.Write(localMessageID, &request.Message)
		if err == segmentfile.ErrReadOnly {
			response, err := s.OpenSegment(ctx, &OpenSegmentRequest{
				NodeID: s.nodeID,
				Topic:  request.Topic,
			})
			if err != nil {
				return nil, err
			}

			if response.PrimaryNodeID != s.nodeID {
				forwardNodeID = response.PrimaryNodeID
				goto FORWARD
			}

			localSegmentID = response.SegmentID
			goto WRITE

		} else if err == segmentfile.ErrFull {
			sha1, size, lastMessageID, err := segment.Complete()
			if err != nil {
				return nil, err
			}
			response, err := s.RotateSegment(ctx, &RotateSegmentRequest{
				NodeID:           s.nodeID,
				OldSegmentID:     localSegmentID,
				OldLastMessageID: lastMessageID.Bytes(),
				OldSize:          uint64(size),
				OldSha1:          sha1,
			})
			if err != nil {
				return nil, err
			}

			if response.PrimaryNodeID != s.nodeID {
				forwardNodeID = response.PrimaryNodeID
				goto FORWARD
			}

			localSegmentID = response.SegmentID
			goto WRITE

		} else if err != nil {
			return nil, err
		}

		return &client.PublishResponse{
			OK:        true,
			MessageID: localMessageID.Bytes(),
		}, nil
	}

FORWARD:
	{
		if request.DoNotForward {
			return nil, errWontForward
		}
		_ = forwardNodeID
		return &client.PublishResponse{
			OK: false,
		}, nil
	}
}
