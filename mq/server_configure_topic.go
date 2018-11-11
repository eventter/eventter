package mq

import (
	"context"

	"eventter.io/mq/client"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) ConfigureTopic(ctx context.Context, request *client.ConfigureTopicRequest) (*client.ConfigureTopicResponse, error) {
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
		return client.NewEventterMQClient(conn).ConfigureTopic(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	buf, err := proto.Marshal(&Command{
		Command: &Command_ConfigureTopic{
			ConfigureTopic: request,
		},
	})
	if err != nil {
		return nil, err
	}

	future := s.raftNode.Apply(buf, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &client.ConfigureTopicResponse{
		OK:    true,
		Index: future.Index(),
	}, nil
}
