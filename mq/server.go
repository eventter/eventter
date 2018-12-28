package mq

import (
	"sync"
	"time"

	"eventter.io/mq/consumers"
	"eventter.io/mq/emq"
	"eventter.io/mq/segments"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

const (
	notFoundErrorFormat          = "%s %s/%s not found"
	namespaceNotFoundErrorFormat = "namespace %s not found"
	couldNotDialLeaderError      = "could not dial leader"
	entityTopic                  = "topic"
	entityConsumerGroup          = "consumer group"
	applyTimeout                 = 10 * time.Second
	barrierTimeout               = 10 * time.Second
	defaultReplicationFactor     = 3
	defaultConsumerGroupSize     = 1024
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
	closed           chan struct{}
	groupMutex       sync.RWMutex
	groups           map[string]*consumers.Group
	subscriptions    map[uint64]*consumers.Subscription
	reconciler       *Reconciler
}

var (
	_ emq.EventterMQServer = (*Server)(nil)
	_ NodeRPCServer        = (*Server)(nil)
)

func NewServer(nodeID uint64, members *memberlist.Memberlist, raftNode *raft.Raft, pool *ClientConnPool, clusterState *ClusterStateStore, segmentDir *segments.Dir) *Server {
	s := &Server{
		nodeID:        nodeID,
		members:       members,
		raftNode:      raftNode,
		pool:          pool,
		clusterState:  clusterState,
		segmentDir:    segmentDir,
		closed:        make(chan struct{}),
		groups:        make(map[string]*consumers.Group),
		subscriptions: make(map[uint64]*consumers.Subscription),
	}
	s.reconciler = NewReconciler(s)
	return s
}

func (s *Server) beginTransaction() (err error) {
	s.tx.Lock()
	defer func() {
		if err != nil {
			s.tx.Unlock()
		}
	}()

	return s.raftNode.Barrier(barrierTimeout).Error()
}

func (s *Server) releaseTransaction() {
	s.tx.Unlock()
}

func (s *Server) makeConsumerGroupMapKey(namespace string, name string) string {
	return namespace + "/" + name
}

func (s *Server) Close() error {
	close(s.closed)
	return nil
}
