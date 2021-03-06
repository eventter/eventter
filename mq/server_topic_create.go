package mq

import (
	"context"

	"eventter.io/mq/emq"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) CreateTopic(ctx context.Context, request *emq.TopicCreateRequest) (*emq.TopicCreateResponse, error) {
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
		return emq.NewEventterMQClient(conn).CreateTopic(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	if err := s.beginTransaction(); err != nil {
		return nil, errors.Wrap(err, "tx begin failed")
	}
	defer s.releaseTransaction()

	state := s.clusterState.Current()

	namespace, _ := state.FindNamespace(request.Topic.Namespace)
	if namespace == nil {
		return nil, errors.Errorf(namespaceNotFoundErrorFormat, request.Topic.Namespace)
	}

	topic := state.GetTopic(request.Topic.Namespace, request.Topic.Name)
	if topic != nil {
		if request.Topic.DefaultExchangeType != topic.DefaultExchangeType {
			return nil, errors.Errorf(
				"topic %s/%s already exists, type cannot be changed - request: %s, cluster: %s",
				request.Topic.Namespace,
				request.Topic.Name,
				request.Topic.DefaultExchangeType,
				topic.DefaultExchangeType,
			)
		}
	}

	cmd := &ClusterCommandTopicCreate{
		Namespace: request.Topic.Namespace,
		Topic: &ClusterTopic{
			Name:                request.Topic.Name,
			DefaultExchangeType: request.Topic.DefaultExchangeType,
			Shards:              request.Topic.Shards,
			ReplicationFactor:   request.Topic.ReplicationFactor,
			Retention:           request.Topic.Retention,
		},
	}

	if cmd.Topic.ReplicationFactor == 0 {
		cmd.Topic.ReplicationFactor = defaultReplicationFactor
	}

	index, err := s.Apply(cmd)
	if err != nil {
		return nil, err
	}

	return &emq.TopicCreateResponse{
		OK:    true,
		Index: index,
	}, nil
}
