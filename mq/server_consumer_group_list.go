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

	state := s.clusterState.Current()

	namespace, _ := state.FindNamespace(request.ConsumerGroup.Namespace)
	if namespace == nil {
		return nil, errors.Errorf(namespaceNotFoundErrorFormat, request.ConsumerGroup.Namespace)
	}

	var consumerGroups []*client.ConsumerGroup

	for _, cg := range namespace.ListConsumerGroups(request.ConsumerGroup.Namespace, request.ConsumerGroup.Name) {
		var clientBindings []*client.ConsumerGroup_Binding

		for _, binding := range cg.Bindings {
			clientBindings = append(clientBindings, s.convertBinding(binding))
		}

		consumerGroups = append(consumerGroups, &client.ConsumerGroup{
			Name: client.NamespaceName{
				Namespace: request.ConsumerGroup.Namespace,
				Name:      cg.Name,
			},
			Bindings: clientBindings,
			Size_:    cg.Size_,
		})
	}

	return &client.ListConsumerGroupsResponse{
		OK:             true,
		Index:          state.Index,
		ConsumerGroups: consumerGroups,
	}, nil
}

func (s *Server) convertBinding(clusterBinding *ClusterConsumerGroup_Binding) *client.ConsumerGroup_Binding {
	clientBinding := &client.ConsumerGroup_Binding{
		TopicName: clusterBinding.TopicName,
	}
	switch by := clusterBinding.By.(type) {
	case nil:
		// do nothing
	case *ClusterConsumerGroup_Binding_RoutingKey:
		clientBinding.By = &client.ConsumerGroup_Binding_RoutingKey{
			RoutingKey: by.RoutingKey,
		}
	case *ClusterConsumerGroup_Binding_HeadersAll:
		clientBinding.By = &client.ConsumerGroup_Binding_HeadersAll{
			HeadersAll: by.HeadersAll,
		}
	case *ClusterConsumerGroup_Binding_HeadersAny:
		clientBinding.By = &client.ConsumerGroup_Binding_HeadersAny{
			HeadersAny: by.HeadersAny,
		}
	default:
		panic("unhandled binding by")
	}

	return clientBinding
}
