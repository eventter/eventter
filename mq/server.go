package mq

import (
	"math/rand"
	"sync"
	"time"

	"eventter.io/mq/client"
	"eventter.io/mq/msgid"
	"eventter.io/mq/segmentfile"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

const (
	notFoundErrorFormat     = "%s %s/%s not found"
	couldNotDialLeaderError = "could not dial leader"
	entityTopic             = "topic"
	entityConsumerGroup     = "consumer group"
)

var (
	errNoLeaderElected = errors.New("no leader elected")
	errNotALeader      = errors.New("request would be forwarded to another node, however, leader_only flag was set")
	errWontForward     = errors.New("request would be forwarded to another node, however, do_not_forward flag was set")
)

type Server struct {
	nodeID           uint64
	raftNode         *raft.Raft
	pool             *ClientConnPool
	clusterState     *ClusterStateStore
	segmentDir       *segmentfile.Dir
	idGenerator      msgid.Generator
	tx               sync.Mutex
	rng              *rand.Rand
	publishForwardRR uint32
}

var (
	_ client.EventterMQServer = (*Server)(nil)
	_ NodeRPCServer           = (*Server)(nil)
)

func NewServer(nodeID uint64, raftNode *raft.Raft, pool *ClientConnPool, clusterState *ClusterStateStore, segmentDir *segmentfile.Dir, idGenerator msgid.Generator) *Server {
	return &Server{
		nodeID:       nodeID,
		raftNode:     raftNode,
		pool:         pool,
		clusterState: clusterState,
		segmentDir:   segmentDir,
		idGenerator:  idGenerator,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
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
