package mq

import (
	"context"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) Loop(memberEventsC chan memberlist.NodeEvent) {
	isLeader := s.raftNode.State() == raft.Leader
	var leaderTickC <-chan time.Time
	leaderTicker := time.NewTicker(10 * time.Second)

	var state *ClusterState = nil
	nodeTicker := time.NewTicker(100 * time.Millisecond)

	reconciler := NewReconciler(s)

LOOP:
	for {
		select {
		case becameLeader := <-s.raftNode.LeaderCh():
			log.Printf("leadership status changed: before=%t, now=%t", isLeader, becameLeader)

			if becameLeader {
				// tick now
				c := make(chan time.Time, 1)
				c <- time.Now()
				leaderTickC = c
			}

			isLeader = becameLeader

		case <-leaderTickC:
			// always re-assign back to the ticker
			leaderTickC = leaderTicker.C

			if !isLeader {
				continue
			}

			reconciler.ReconcileNodes(s.clusterState.Current())

			// barrier before segments reconciliation
			if err := s.raftNode.Barrier(10 * time.Second).Error(); err != nil {
				log.Printf("could not add barrier: %v", err)
				continue
			}

			reconciler.ReconcileSegments(s.clusterState.Current())

		case <-nodeTicker.C:
			newState := s.clusterState.Current()
			if newState == state {
				continue
			}

			state = newState

			// TODO: create / cancel node tasks, e.g. replication

		case ev := <-memberEventsC:
			if !isLeader {
				continue
			}

			cmd := &UpdateNodeCommand{
				ID:      MustIDFromString(ev.Node.Name),
				Address: ev.Node.Address(),
			}

			if ev.Event == memberlist.NodeJoin || ev.Event == memberlist.NodeUpdate {
				cmd.State = ClusterNode_ALIVE
			} else {
				cmd.State = ClusterNode_DEAD
				now := time.Now()
				cmd.LastSeenAlive = &now
			}

			_, err := s.Apply(cmd)
			if err != nil {
				log.Printf("could not Apply update node by members event: %v", err)
				continue
			}

			log.Printf("updated node by members event: %s", cmd.String())

			if cmd.State == ClusterNode_ALIVE {
				future := s.raftNode.AddVoter(raft.ServerID(ev.Node.Name), raft.ServerAddress(cmd.Address), 0, 10*time.Second)
				if err := future.Error(); err != nil {
					log.Printf("could not add peer: %v", err)
					continue
				}
			}

		case <-s.closeC:
			break LOOP
		}
	}
}

func (s *Server) Members() []*memberlist.Node {
	return s.members.Members()
}

func (s *Server) AddVoter(id string, addr string) error {
	future := s.raftNode.AddVoter(
		raft.ServerID(id),
		raft.ServerAddress(addr),
		0,
		applyTimeout,
	)
	return future.Error()
}

func (s *Server) GetSegmentSizeFromNode(ctx context.Context, segmentID uint64, nodeID uint64, nodeAddr string) (size int64, err error) {
	request := &SegmentGetSizeRequest{SegmentID: segmentID}
	var response *SegmentGetSizeResponse
	if nodeID == s.nodeID {
		response, err = s.SegmentGetSize(ctx, request)
	} else {
		conn, err := s.pool.Get(ctx, nodeAddr)
		if err != nil {
			return -1, errors.Wrap(err, couldNotDialLeaderError)
		}
		defer s.pool.Put(conn)

		response, err = NewNodeRPCClient(conn).SegmentGetSize(ctx, request)
	}

	if err != nil {
		return -1, err
	}

	return response.Size_, nil
}
