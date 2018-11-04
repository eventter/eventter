package mq

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) RotateSegment(ctx context.Context, request *RotateSegmentRequest) (*OpenSegmentResponse, error) {
	if s.raftNode.State() != raft.Leader {
		return nil, errNotALeader
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	segment := s.clusterState.GetOpenSegment(request.OldSegmentID)
	if segment == nil {
		return nil, errors.Errorf("segment %d not found", request.OldSegmentID)
	}

	if segment.Nodes.PrimaryNodeID != request.NodeID {
		return nil, errors.Errorf("node %d is not primary for segment %d", request.NodeID, request.OldSegmentID)
	}

	buf, err := proto.Marshal(&CloseSegmentCommand{
		ID:            segment.ID,
		DoneNodeID:    request.NodeID,
		LastMessageID: request.OldLastMessageID,
		Size_:         request.OldSize,
		Sha1:          request.OldSha1,
	})
	if err != nil {
		return nil, err
	}

	future := s.raftNode.Apply(buf, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	newSegmentID := s.clusterState.NextSegmentID()

	buf2, err := proto.Marshal(&OpenSegmentCommand{
		ID:             newSegmentID,
		Topic:          segment.Topic,
		FirstMessageID: request.NewFirstMessageID,
		PrimaryNodeID:  request.NodeID,
	})
	if err != nil {
		return nil, err
	}

	future = s.raftNode.Apply(buf2, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &OpenSegmentResponse{
		SegmentID:     newSegmentID,
		PrimaryNodeID: request.NodeID,
	}, nil
}
