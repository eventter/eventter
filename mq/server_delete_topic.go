package mq

import (
	"context"

	"eventter.io/mq/client"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) DeleteTopic(ctx context.Context, request *client.DeleteTopicRequest) (*client.DeleteTopicResponse, error) {
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
		return client.NewEventterMQClient(conn).DeleteTopic(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	state := s.clusterState.Current()

	if !state.TopicExists(request.Topic.Namespace, request.Topic.Name) {
		return nil, errors.Errorf(notFoundErrorFormat, entityTopic, request.Topic.Namespace, request.Topic.Name)
	}

	if request.IfUnused {
		if state.AnyConsumerGroupReferencesTopic(request.Topic.Namespace, request.Topic.Name) {
			return nil, errors.Errorf("topic %s/%s is referenced by some consumer group(s)", request.Topic.Namespace, request.Topic.Name)
		}
	}

	index, err := s.Apply(request)
	if err != nil {
		return nil, err
	}

	return &client.DeleteTopicResponse{
		OK:    true,
		Index: index,
	}, nil
}
