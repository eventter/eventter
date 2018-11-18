package mq

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) SegmentReplicaClose(ctx context.Context, request *SegmentCloseRequest) (*SegmentCloseResponse, error) {
	if s.raftNode.State() != raft.Leader {
		if request.LeaderOnly {
			return nil, errNotALeader
		}
		leader := s.raftNode.Leader()
		if leader == "" {
			return nil, errNoLeaderElected
		}

		conn, err := s.pool.Get(ctx, string(leader))
		if err != nil {
			return nil, errors.Wrap(err, couldNotDialLeaderError)
		}
		defer s.pool.Put(conn)

		request.LeaderOnly = true
		return NewNodeRPCClient(conn).SegmentReplicaClose(ctx, request)
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	state := s.clusterState.Current()
	segment := state.GetClosedSegment(request.SegmentID)
	if segment == nil {
		return nil, errors.Errorf("segment %d not found", request.SegmentID)
	}

	if request.Size_ != segment.Size_ || !bytes.Equal(request.Sha1, segment.Sha1) {
		return nil, errors.Errorf(
			"segment replica corrupted, expected size: %d, got size: %d, expected sha1: %s, got sha1: %s",
			segment.Size_,
			request.Size_,
			hex.EncodeToString(segment.Sha1),
			hex.EncodeToString(request.Sha1),
		)
	}

	cmd := &ClusterUpdateSegmentNodesCommand{
		ID:    segment.ID,
		Which: ClusterUpdateSegmentNodesCommand_CLOSED,
	}

	cmd.Nodes.ReplicatingNodeIDs = make([]uint64, 0, len(segment.Nodes.ReplicatingNodeIDs)-1)
	for _, nodeID := range segment.Nodes.ReplicatingNodeIDs {
		if nodeID != request.NodeID {
			cmd.Nodes.ReplicatingNodeIDs = append(cmd.Nodes.ReplicatingNodeIDs, nodeID)
		}
	}

	cmd.Nodes.DoneNodeIDs = make([]uint64, 0, len(segment.Nodes.DoneNodeIDs)+1)
	for _, nodeID := range segment.Nodes.DoneNodeIDs {
		if nodeID != request.NodeID {
			cmd.Nodes.DoneNodeIDs = append(cmd.Nodes.DoneNodeIDs, nodeID)
		}
	}
	cmd.Nodes.DoneNodeIDs = append(cmd.Nodes.DoneNodeIDs, request.NodeID)

	if _, err := s.Apply(cmd); err != nil {
		return nil, err
	}

	return &SegmentCloseResponse{}, nil
}
