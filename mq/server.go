package mq

import (
	"math/rand"
	"sync"
	"time"

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
	tx           sync.Mutex
	rnd          *rand.Rand
}

var (
	_ client.EventterMQServer = (*Server)(nil)
	_ NodeRPCServer           = (*Server)(nil)
)

func NewServer(serverID uint64, raftNode *raft.Raft, pool *ClientConnPool, clusterState *ClusterStateStore) *Server {
	return &Server{
		serverID:     serverID,
		raftNode:     raftNode,
		pool:         pool,
		clusterState: clusterState,
		rnd:          rand.New(rand.NewSource(time.Now().UnixNano())),
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
	if err := future.Error(); err != nil {
		return err
	}

	return nil
}

func (s *Server) releaseTransaction() {
	s.tx.Unlock()
}
