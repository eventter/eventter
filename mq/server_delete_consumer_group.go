package mq

import (
	"context"

	"eventter.io/mq/client"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) DeleteConsumerGroup(ctx context.Context, request *client.DeleteConsumerGroupRequest) (*client.DeleteConsumerGroupResponse, error) {
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
		return client.NewEventterMQClient(conn).DeleteConsumerGroup(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	// TODO: access control

	if !s.clusterState.Current().ConsumerGroupExists(request.ConsumerGroup.Namespace, request.ConsumerGroup.Name) {
		return nil, errors.Errorf(notFoundErrorFormat, entityConsumerGroup, request.ConsumerGroup.Namespace, request.ConsumerGroup.Name)
	}

	if request.IfEmpty {
		return nil, errors.New("if empty not implemented")
	}

	if request.IfUnused {
		return nil, errors.New("if unused not implemented")
	}

	buf, err := proto.Marshal(&Command{
		Command: &Command_DeleteConsumerGroup{
			DeleteConsumerGroup: request,
		},
	})
	if err != nil {
		return nil, err
	}

	future := s.raftNode.Apply(buf, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &client.DeleteConsumerGroupResponse{
		OK:    true,
		Index: future.Index(),
	}, nil
}
