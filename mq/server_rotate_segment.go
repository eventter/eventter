package mq

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) RotateSegment(ctx context.Context, request *RotateSegmentRequest) (*OpenSegmentResponse, error) {
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
		return NewNodeRPCClient(conn).RotateSegment(ctx, request)
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	oldSegment := s.clusterState.GetOpenSegment(request.OldSegmentID)
	if oldSegment == nil {
		oldSegment = s.clusterState.GetClosedSegment(request.OldSegmentID)
		if oldSegment == nil {
			return nil, errors.Errorf("segment %d not found", request.OldSegmentID)
		}

	} else {
		if oldSegment.Nodes.PrimaryNodeID != request.NodeID {
			return nil, errors.Errorf("node %d is not primary for segment %d", request.NodeID, request.OldSegmentID)
		}

		buf, err := proto.Marshal(&Command{
			Command: &Command_CloseSegment{
				CloseSegment: &CloseSegmentCommand{
					ID:            oldSegment.ID,
					DoneNodeID:    request.NodeID,
					LastMessageID: request.OldLastMessageID,
					Size_:         request.OldSize,
					Sha1:          request.OldSha1,
				},
			},
		})
		if err != nil {
			return nil, err
		}

		future := s.raftNode.Apply(buf, 0)
		if err := future.Error(); err != nil {
			return nil, err
		}

		barrierFuture := s.raftNode.Barrier(10 * time.Second)
		if err := barrierFuture.Error(); err != nil {
			return nil, err
		}
	}

	return s.doOpenSegment(request.NodeID, oldSegment.Topic, request.NewFirstMessageID)
}
