package mq

import (
	"context"

	"eventter.io/mq/client"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) CreateTopic(ctx context.Context, request *client.CreateTopicRequest) (*client.CreateTopicResponse, error) {
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
		return client.NewEventterMQClient(conn).CreateTopic(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	state := s.clusterState.Current()

	topic := state.GetTopic(request.Topic.Name.Namespace, request.Topic.Name.Name)
	if topic != nil {
		if request.Topic.Type != topic.Type {
			return nil, errors.Errorf(
				"topic %s/%s already exists, type cannot be changed - request: %s, cluster: %s",
				request.Topic.Name.Namespace,
				request.Topic.Name.Name,
				request.Topic.Type,
				topic.Type,
			)
		}
	}

	cmd := &ClusterCommandTopicCreate{
		Namespace: request.Topic.Name.Namespace,
		Topic: &ClusterTopic{
			Name:              request.Topic.Name.Name,
			Type:              request.Topic.Type,
			Shards:            request.Topic.Shards,
			ReplicationFactor: request.Topic.ReplicationFactor,
			Retention:         request.Topic.Retention,
		},
	}

	if cmd.Topic.ReplicationFactor == 0 {
		cmd.Topic.ReplicationFactor = defaultReplicationFactor
	}

	index, err := s.Apply(cmd)
	if err != nil {
		return nil, err
	}

	return &client.CreateTopicResponse{
		OK:    true,
		Index: index,
	}, nil
}
