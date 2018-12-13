package mq

import (
	"context"
	"time"

	"eventter.io/mq/client"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) CreateConsumerGroup(ctx context.Context, request *client.CreateConsumerGroupRequest) (*client.CreateConsumerGroupResponse, error) {
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
		return client.NewEventterMQClient(conn).CreateConsumerGroup(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, errors.Wrap(err, "validation failed")
	}

	if err := s.beginTransaction(); err != nil {
		return nil, errors.Wrap(err, "tx begin failed")
	}
	defer s.releaseTransaction()

	state := s.clusterState.Current()

	namespace, _ := state.FindNamespace(request.ConsumerGroup.Name.Namespace)
	if namespace == nil {
		return nil, errors.Errorf(namespaceNotFoundErrorFormat, request.ConsumerGroup.Name.Namespace)
	}

	cmd := &ClusterCommandConsumerGroupCreate{
		Namespace: request.ConsumerGroup.Name.Namespace,
		ConsumerGroup: &ClusterConsumerGroup{
			Name:      request.ConsumerGroup.Name.Name,
			Size_:     request.ConsumerGroup.Size_,
			CreatedAt: time.Now(),
		},
	}

	for _, binding := range request.ConsumerGroup.Bindings {
		topic := state.GetTopic(request.ConsumerGroup.Name.Namespace, binding.TopicName)
		if topic == nil {
			return nil, errors.Errorf(notFoundErrorFormat, entityTopic, request.ConsumerGroup.Name.Namespace, binding.TopicName)
		}

		clusterBinding := &ClusterConsumerGroup_Binding{
			TopicName: binding.TopicName,
		}
		switch topic.Type {
		case client.ExchangeTypeDirect:
			fallthrough
		case client.ExchangeTypeTopic:
			switch by := binding.By.(type) {
			case *client.ConsumerGroup_Binding_RoutingKey:
				clusterBinding.By = &ClusterConsumerGroup_Binding_RoutingKey{
					RoutingKey: by.RoutingKey,
				}
			default:
				return nil, errors.Errorf(
					"trying to bind to %s %s/%s of type %s, but no routing key set",
					entityTopic,
					request.ConsumerGroup.Name.Namespace,
					binding.TopicName,
					topic.Type,
				)
			}
		case client.ExchangeTypeHeaders:
			switch by := binding.By.(type) {
			case *client.ConsumerGroup_Binding_HeadersAny:
				clusterBinding.By = &ClusterConsumerGroup_Binding_HeadersAny{
					HeadersAny: by.HeadersAny,
				}
			case *client.ConsumerGroup_Binding_HeadersAll:
				clusterBinding.By = &ClusterConsumerGroup_Binding_HeadersAll{
					HeadersAll: by.HeadersAll,
				}
			default:
				return nil, errors.Errorf(
					"trying to bind to %s %s/%s of type %s, but no headers set",
					entityTopic,
					request.ConsumerGroup.Name.Namespace,
					binding.TopicName,
					topic.Type,
				)
			}
		case client.ExchangeTypeFanout:
			// leave by to null
		default:
			return nil, errors.Errorf("unhandled topic type: %s", topic.Type)
		}

		cmd.ConsumerGroup.Bindings = append(cmd.ConsumerGroup.Bindings, clusterBinding)
	}

	if cmd.ConsumerGroup.Size_ == 0 {
		cmd.ConsumerGroup.Size_ = defaultConsumerGroupSize
	}

	index, err := s.Apply(cmd)
	if err != nil {
		return nil, errors.Wrap(err, "apply failed")
	}

	if err := s.raftNode.Barrier(barrierTimeout).Error(); err != nil {
		return nil, errors.Wrap(err, "barrier failed")
	}

	// !!! reload state after barrier
	state = s.clusterState.Current()
	namespace, _ = state.FindNamespace(request.ConsumerGroup.Name.Namespace)
	consumerGroup, _ := namespace.FindConsumerGroup(request.ConsumerGroup.Name.Name)

	if newIndex := s.reconciler.ReconcileConsumerGroupOffsetCommits(state, namespace, consumerGroup); newIndex > 0 {
		index = newIndex
	}

	return &client.CreateConsumerGroupResponse{
		OK:    true,
		Index: index,
	}, nil
}
