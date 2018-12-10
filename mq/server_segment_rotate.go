package mq

import (
	"context"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) SegmentRotate(ctx context.Context, request *SegmentCloseRequest) (*SegmentOpenResponse, error) {
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
		return NewNodeRPCClient(conn).SegmentRotate(ctx, request)
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	if request.OffsetCommitsUpdate != nil {
		if _, err := s.Apply(request.OffsetCommitsUpdate); err != nil {
			return nil, errors.Wrap(err, "offset commit failed")
		}
	}

	state := s.clusterState.Current()

	if err := s.txSegmentClose(state, request); err != nil {
		return nil, err
	}

	if err := s.raftNode.Barrier(barrierTimeout).Error(); err != nil {
		return nil, err
	}

	state = s.clusterState.Current()
	oldSegment := state.GetClosedSegment(request.SegmentID)
	if oldSegment == nil {
		return nil, errors.New("segment deleted in between")
	}

	return s.txSegmentOpen(state, request.NodeID, oldSegment.Owner, oldSegment.Type)
}
