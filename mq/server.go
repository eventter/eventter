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
	raftNode     *raft.Raft
	pool         *ClientConnPool
	clusterState *ClusterStateStore
}

func NewServer(raftNode *raft.Raft, pool *ClientConnPool, clusterState *ClusterStateStore) *Server {
	return &Server{
		raftNode:     raftNode,
		pool:         pool,
		clusterState: clusterState,
	}
}

var _ client.EventterMQServer = (*Server)(nil)

func (s *Server) DeleteTopic(ctx context.Context, request *client.DeleteTopicRequest) (*client.DeleteTopicResponse, error) {
	panic("implement me")
}

func (s *Server) ConfigureConsumerGroup(ctx context.Context, request *client.ConfigureConsumerGroupRequest) (*client.ConfigureConsumerGroupResponse, error) {
	panic("implement me")
}

func (s *Server) ListConsumerGroups(ctx context.Context, request *client.ListConsumerGroupsRequest) (*client.ListConsumerGroupsResponse, error) {
	panic("implement me")
}

func (s *Server) DeleteConsumerGroup(ctx context.Context, request *client.DeleteConsumerGroupRequest) (*client.DeleteConsumerGroupResponse, error) {
	panic("implement me")
}

func (s *Server) Publish(ctx context.Context, request *client.PublishRequest) (*client.PublishResponse, error) {
	panic("implement me")
}

func (s *Server) Consume(stream client.EventterMQ_ConsumeServer) error {
	panic("implement me")
}
