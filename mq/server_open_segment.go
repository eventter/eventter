package mq

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) OpenSegment(ctx context.Context, request *OpenSegmentRequest) (*OpenSegmentResponse, error) {
	if s.raftNode.State() != raft.Leader {
		return nil, errNotALeader
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	topic := s.clusterState.GetTopic(request.Topic.Namespace, request.Topic.Name)
	if topic == nil {
		return nil, errors.Errorf("topic %s/%s does not exist", request.Topic.Namespace, request.Topic.Name)
	}

	openSegments := s.clusterState.FindOpenSegmentsFor(request.Topic.Namespace, request.Topic.Name)

	if topic.Shards > 0 && uint32(len(openSegments)) >= topic.Shards {
		segment := openSegments[s.rnd.Intn(len(openSegments))]
		return &OpenSegmentResponse{
			SegmentID:     segment.ID,
			PrimaryNodeID: segment.Nodes.PrimaryNodeID,
		}, nil
	}

	segmentID := s.clusterState.NextSegmentID()

	buf, err := proto.Marshal(&OpenSegmentCommand{
		ID:             segmentID,
		Topic:          request.Topic,
		FirstMessageID: request.FirstMessageID,
		PrimaryNodeID:  request.NodeID,
	})
	if err != nil {
		return nil, err
	}

	future := s.raftNode.Apply(buf, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &OpenSegmentResponse{
		SegmentID:     segmentID,
		PrimaryNodeID: request.NodeID,
	}, nil
}
