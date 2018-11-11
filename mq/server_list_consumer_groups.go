package mq

import (
	"context"

	"eventter.io/mq/client"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) ListConsumerGroups(ctx context.Context, request *client.ListConsumerGroupsRequest) (*client.ListConsumerGroupsResponse, error) {
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
		return client.NewEventterMQClient(conn).ListConsumerGroups(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	index, cgs := s.clusterState.Current().ListConsumerGroups(request.ConsumerGroup.Namespace, request.ConsumerGroup.Name)

	var consumerGroups []*client.ConsumerGroup

	for _, cg := range cgs {
		var bindings []*client.ConsumerGroup_Binding

		for _, b := range cg.Bindings {
			bindings = append(bindings, &client.ConsumerGroup_Binding{
				TopicName:  b.TopicName,
				RoutingKey: b.RoutingKey,
			})
		}

		consumerGroups = append(consumerGroups, &client.ConsumerGroup{
			ConsumerGroup: client.NamespaceName{
				Namespace: request.ConsumerGroup.Namespace,
				Name:      cg.Name,
			},
			Bindings: bindings,
			Shards:   cg.Shards,
		})
	}

	return &client.ListConsumerGroupsResponse{
		OK:             true,
		Index:          index,
		ConsumerGroups: consumerGroups,
	}, nil
}
