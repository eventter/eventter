package mq

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) SegmentClose(ctx context.Context, request *SegmentCloseRequest) (*SegmentCloseResponse, error) {
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
		return NewNodeRPCClient(conn).SegmentClose(ctx, request)
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	if err := s.txSegmentClose(s.clusterState.Current(), request); err != nil {
		return nil, err
	}

	return &SegmentCloseResponse{}, nil
}

func (s *Server) txSegmentClose(state *ClusterState, request *SegmentCloseRequest) error {
	oldSegment := state.GetOpenSegment(request.SegmentID)
	if oldSegment == nil {
		oldSegment = state.GetClosedSegment(request.SegmentID)
		if oldSegment == nil {
			return errors.Errorf("segment %d not found", request.SegmentID)
		}

	} else {
		if oldSegment.Nodes.PrimaryNodeID != request.NodeID {
			return errors.Errorf("node %d is not primary for segment %d", request.NodeID, request.SegmentID)
		}

		cmd := &CloseSegmentCommand{
			ID:         oldSegment.ID,
			DoneNodeID: request.NodeID,
			ClosedAt:   time.Now(),
			Size_:      request.Size_,
			Sha1:       request.Sha1,
		}
		if cmd.ClosedAt.Before(oldSegment.OpenedAt) {
			// possible clock skew => move closed time to opened time
			cmd.ClosedAt = oldSegment.OpenedAt
		}
		_, err := s.Apply(cmd)
		if err != nil {
			return err
		}
	}

	return nil
}
