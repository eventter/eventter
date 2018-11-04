package mq

import (
	"context"

	"eventter.io/mq/client"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) ConfigureConsumerGroup(ctx context.Context, request *client.ConfigureConsumerGroupRequest) (*client.ConfigureConsumerGroupResponse, error) {
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
			return nil, errors.Wrap(err, "could not dial leader")
		}
		defer s.pool.Put(conn)

		request.LeaderOnly = true
		return client.NewEventterMQClient(conn).ConfigureConsumerGroup(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	for _, binding := range request.Bindings {
		if !s.clusterState.TopicExists(request.ConsumerGroup.Namespace, binding.TopicName) {
			return nil, errors.Errorf("topic %s/%s does not exist", request.ConsumerGroup.Namespace, binding.TopicName)
		}
	}

	// TODO: access control

	buf, err := proto.Marshal(&Command{
		Command: &Command_ConfigureConsumerGroup{
			ConfigureConsumerGroup: request,
		},
	})
	if err != nil {
		return nil, err
	}

	future := s.raftNode.Apply(buf, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &client.ConfigureConsumerGroupResponse{
		OK:    true,
		Index: future.Index(),
	}, nil
}