package mq

import (
	"context"

	"eventter.io/mq/client"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) ListTopics(ctx context.Context, request *client.ListTopicsRequest) (*client.ListTopicsResponse, error) {
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
		return client.NewEventterMQClient(conn).ListTopics(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	index, t := s.clusterState.Current().ListTopics(request.Topic.Namespace, request.Topic.Name)

	var topics []*client.Topic

	for _, t := range t {
		topics = append(topics, &client.Topic{
			Topic: client.NamespaceName{
				Namespace: request.Topic.Namespace,
				Name:      t.Name,
			},
			Type:      t.Type,
			Shards:    t.Shards,
			Retention: t.Retention,
		})
	}

	return &client.ListTopicsResponse{
		OK:     true,
		Index:  index,
		Topics: topics,
	}, nil
}
