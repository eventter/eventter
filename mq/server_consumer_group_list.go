package mq

import (
	"context"

	"eventter.io/mq/emq"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) ListConsumerGroups(ctx context.Context, request *emq.ConsumerGroupListRequest) (*emq.ConsumerGroupListResponse, error) {
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
		return emq.NewEventterMQClient(conn).ListConsumerGroups(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	state := s.clusterState.Current()

	namespace, _ := state.FindNamespace(request.Namespace)
	if namespace == nil {
		return nil, errors.Errorf(namespaceNotFoundErrorFormat, request.Namespace)
	}

	var consumerGroups []*emq.ConsumerGroup

	for _, cg := range namespace.ListConsumerGroups(request.Namespace, request.Name) {
		var clientBindings []*emq.ConsumerGroup_Binding

		for _, binding := range cg.Bindings {
			clientBindings = append(clientBindings, s.convertClusterBinding(binding))
		}

		consumerGroups = append(consumerGroups, &emq.ConsumerGroup{
			Namespace: namespace.Name,
			Name:      cg.Name,
			Bindings:  clientBindings,
			Size_:     cg.Size_,
			Since:     cg.Since,
		})
	}

	return &emq.ConsumerGroupListResponse{
		OK:             true,
		Index:          state.Index,
		ConsumerGroups: consumerGroups,
	}, nil
}

func (s *Server) convertClusterBinding(clusterBinding *ClusterConsumerGroup_Binding) *emq.ConsumerGroup_Binding {
	clientBinding := &emq.ConsumerGroup_Binding{
		TopicName:    clusterBinding.TopicName,
		ExchangeType: clusterBinding.ExchangeType,
	}
	switch by := clusterBinding.By.(type) {
	case nil:
		// do nothing
	case *ClusterConsumerGroup_Binding_RoutingKey:
		clientBinding.By = &emq.ConsumerGroup_Binding_RoutingKey{
			RoutingKey: by.RoutingKey,
		}
	case *ClusterConsumerGroup_Binding_HeadersAll:
		clientBinding.By = &emq.ConsumerGroup_Binding_HeadersAll{
			HeadersAll: by.HeadersAll,
		}
	case *ClusterConsumerGroup_Binding_HeadersAny:
		clientBinding.By = &emq.ConsumerGroup_Binding_HeadersAny{
			HeadersAny: by.HeadersAny,
		}
	default:
		panic("unhandled binding by")
	}

	return clientBinding
}
