package mq

import (
	"context"

	"eventter.io/mq/client"
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

	if !s.clusterState.Current().ConsumerGroupExists(request.ConsumerGroup.Namespace, request.ConsumerGroup.Name) {
		return nil, errors.Errorf(notFoundErrorFormat, entityConsumerGroup, request.ConsumerGroup.Namespace, request.ConsumerGroup.Name)
	}

	index, err := s.Apply(request)
	if err != nil {
		return nil, err
	}

	return &client.DeleteConsumerGroupResponse{
		OK:    true,
		Index: index,
	}, nil
}
