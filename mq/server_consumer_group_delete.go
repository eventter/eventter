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
		return nil, errors.Wrap(err, "validation failed")
	}

	if err := s.beginTransaction(); err != nil {
		return nil, errors.Wrap(err, "begin failed")
	}
	defer s.releaseTransaction()

	state := s.clusterState.Current()

	namespace, _ := state.FindNamespace(request.ConsumerGroup.Namespace)
	if namespace == nil {
		return nil, errors.Errorf(namespaceNotFoundErrorFormat, request.ConsumerGroup.Namespace)
	}

	if consumerGroup, _ := namespace.FindConsumerGroup(request.ConsumerGroup.Name); consumerGroup == nil {
		return nil, errors.Errorf(notFoundErrorFormat, entityConsumerGroup, request.ConsumerGroup.Namespace, request.ConsumerGroup.Name)
	}

	index, err := s.Apply(&ClusterCommandConsumerGroupDelete{
		Namespace: request.ConsumerGroup.Namespace,
		Name:      request.ConsumerGroup.Name,
	})
	if err != nil {
		return nil, errors.Wrap(err, "consumer group delete failed")
	}

	segments := state.FindOpenSegmentsFor(
		ClusterSegment_CONSUMER_GROUP_OFFSET_COMMITS,
		request.ConsumerGroup.Namespace,
		request.ConsumerGroup.Name,
	)
	for _, segment := range segments {
		index, err = s.Apply(&ClusterCommandSegmentDelete{
			ID:    segment.ID,
			Which: ClusterCommandSegmentDelete_OPEN,
		})
		if err != nil {
			return nil, errors.Wrap(err, "segment delete failed")
		}
	}

	return &client.DeleteConsumerGroupResponse{
		OK:    true,
		Index: index,
	}, nil
}
