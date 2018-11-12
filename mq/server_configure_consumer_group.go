package mq

import (
	"context"

	"eventter.io/mq/client"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) ConfigureConsumerGroup(ctx context.Context, request *client.ConfigureConsumerGroupRequest) (*client.ConfigureConsumerGroupResponse, error) {
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
		return client.NewEventterMQClient(conn).ConfigureConsumerGroup(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	state := s.clusterState.Current()

	for _, binding := range request.Bindings {
		if !state.TopicExists(request.ConsumerGroup.Namespace, binding.TopicName) {
			return nil, errors.Errorf(notFoundErrorFormat, entityTopic, request.ConsumerGroup.Namespace, binding.TopicName)
		}
	}

	index, err := s.apply(request)
	if err != nil {
		return nil, err
	}

	return &client.ConfigureConsumerGroupResponse{
		OK:    true,
		Index: index,
	}, nil
}
