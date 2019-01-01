package mq

import (
	"context"

	"eventter.io/mq/emq"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) CreateNamespace(ctx context.Context, request *emq.NamespaceCreateRequest) (*emq.NamespaceCreateResponse, error) {
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
		return emq.NewEventterMQClient(conn).CreateNamespace(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, errors.Wrap(err, "validation failed")
	}

	if err := s.beginTransaction(); err != nil {
		return nil, errors.Wrap(err, "tx begin failed")
	}
	defer s.releaseTransaction()

	index, err := s.Apply(&ClusterCommandNamespaceCreate{Namespace: request.Namespace})
	if err != nil {
		return nil, errors.Wrap(err, "apply failed")
	}

	return &emq.NamespaceCreateResponse{
		OK:    true,
		Index: index,
	}, nil
}
