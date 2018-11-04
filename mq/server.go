package mq

import (
	"context"

	"eventter.io/mq/client"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

var (
	errNoLeaderElected = errors.New("no leader elected")
	errNotALeader      = errors.New("not a leader")
)

type Server struct {
	serverID     uint64
	raftNode     *raft.Raft
	pool         *ClientConnPool
	clusterState *ClusterStateStore
}

func NewServer(serverID uint64, raftNode *raft.Raft, pool *ClientConnPool, clusterState *ClusterStateStore) *Server {
	return &Server{
		serverID:     serverID,
		raftNode:     raftNode,
		pool:         pool,
		clusterState: clusterState,
	}
}

var _ client.EventterMQServer = (*Server)(nil)

func (s *Server) Publish(ctx context.Context, request *client.PublishRequest) (*client.PublishResponse, error) {
	panic("implement me")
}

func (s *Server) Consume(stream client.EventterMQ_ConsumeServer) error {
	panic("implement me")
}
