package mq

import (
	"context"

	"eventter.io/mq/emq"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) DeleteTopic(ctx context.Context, request *emq.TopicDeleteRequest) (*emq.TopicDeleteResponse, error) {
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
		return emq.NewEventterMQClient(conn).DeleteTopic(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	state := s.clusterState.Current()

	namespace, _ := state.FindNamespace(request.Topic.Namespace)
	if namespace == nil {
		return nil, errors.Errorf(namespaceNotFoundErrorFormat, request.Topic.Namespace)
	}

	if topic, _ := namespace.FindTopic(request.Topic.Name); topic == nil {
		return nil, errors.Errorf(notFoundErrorFormat, entityTopic, request.Topic.Namespace, request.Topic.Name)
	}

	if request.IfUnused {
		if namespace.AnyConsumerGroupReferencesTopic(request.Topic.Name) {
			return nil, errors.Errorf("topic %s/%s is referenced by some consumer group(s)", request.Topic.Namespace, request.Topic.Name)
		}
	}

	index, err := s.Apply(&ClusterCommandTopicDelete{
		Namespace: request.Topic.Namespace,
		Name:      request.Topic.Name,
	})
	if err != nil {
		return nil, err
	}

	return &emq.TopicDeleteResponse{
		OK:    true,
		Index: index,
	}, nil
}
