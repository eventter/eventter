package mq

import (
	"sync"
	"time"

	"eventter.io/mq/client"
	"eventter.io/mq/segments"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

const (
	notFoundErrorFormat             = "%s %s/%s not found"
	couldNotDialLeaderError         = "could not dial leader"
	entityTopic                     = "topic"
	entityConsumerGroup             = "consumer group"
	applyTimeout                    = 10 * time.Second
	defaultSegmentReplicationFactor = 3
)

var (
	errNoLeaderElected = errors.New("no leader elected")
	errNotALeader      = errors.New("request would be forwarded to leader node, however, leader_only flag was set")
	errWontForward     = errors.New("request would be forwarded to another node, however, do_not_forward flag was set")
	errForwardNodeDead = errors.New("forward node is dead")
)

type Server struct {
	nodeID           uint64
	members          *memberlist.Memberlist
	raftNode         *raft.Raft
	pool             *ClientConnPool
	clusterState     *ClusterStateStore
	segmentDir       *segments.Dir
	tx               sync.Mutex
	publishForwardRR uint32
	closeC           chan struct{}
}

var (
	_ client.EventterMQServer = (*Server)(nil)
	_ NodeRPCServer           = (*Server)(nil)
)

func NewServer(nodeID uint64, members *memberlist.Memberlist, raftNode *raft.Raft, pool *ClientConnPool, clusterState *ClusterStateStore, segmentDir *segments.Dir) *Server {
	return &Server{
		nodeID:       nodeID,
		members:      members,
		raftNode:     raftNode,
		pool:         pool,
		clusterState: clusterState,
		segmentDir:   segmentDir,
		closeC:       make(chan struct{}),
	}
}

func (s *Server) beginTransaction() (err error) {
	s.tx.Lock()
	defer func() {
		if err != nil {
			s.tx.Unlock()
		}
	}()

	future := s.raftNode.Barrier(10 * time.Second)
	return future.Error()
}

func (s *Server) releaseTransaction() {
	s.tx.Unlock()
}

func (s *Server) Close() error {
	close(s.closeC)
	return nil
}
