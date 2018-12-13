package mq

import (
	"context"

	"eventter.io/mq/client"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) DeleteNamespace(ctx context.Context, request *client.DeleteNamespaceRequest) (*client.DeleteNamespaceResponse, error) {
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
		return client.NewEventterMQClient(conn).DeleteNamespace(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, errors.Wrap(err, "validation failed")
	}

	if err := s.beginTransaction(); err != nil {
		return nil, errors.Wrap(err, "tx begin failed")
	}
	defer s.releaseTransaction()

	state := s.clusterState.Current()

	namespace, _ := state.FindNamespace(request.Namespace)
	if namespace == nil {
		return nil, errors.Errorf(namespaceNotFoundErrorFormat, request.Namespace)
	}

	index, err := s.Apply(&ClusterCommandNamespaceDelete{Namespace: request.Namespace})
	if err != nil {
		return nil, errors.Wrap(err, "apply failed")
	}

	return &client.DeleteNamespaceResponse{
		OK:    true,
		Index: index,
	}, nil
}
