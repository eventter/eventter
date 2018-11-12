package mq

import (
	"log"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
)

func (s *Server) Loop(memberEventsC chan memberlist.NodeEvent) {
	isLeader := s.raftNode.State() == raft.Leader

	var leaderTickC <-chan time.Time
	leaderTicker := time.NewTicker(10 * time.Second)

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

			s.reconcileNodes()

			// barrier before segments reconciliation
			if err := s.raftNode.Barrier(10 * time.Second).Error(); err != nil {
				log.Printf("could not add barrier: %v", err)
				continue
			}

			state := s.clusterState.Current()
			nodeSegmentCounts := state.CountSegmentsPerNode()
			nodeMap := make(map[uint64]*ClusterNode)
			allCandidateNodeIDs := make([]uint64, 0, len(state.Nodes))
			for _, node := range state.Nodes {
				nodeMap[node.ID] = node
				if node.State == ClusterNode_ALIVE {
					allCandidateNodeIDs = append(allCandidateNodeIDs, node.ID)
				}
			}

			s.reconcileOpenSegments(state, nodeSegmentCounts, nodeMap, allCandidateNodeIDs)
			s.reconcileClosedSegments(state, nodeSegmentCounts, nodeMap, allCandidateNodeIDs)

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

			_, err := s.apply(cmd)
			if err != nil {
				log.Printf("could not apply update node by members event: %v", err)
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
			return
		}
	}
}

func (s *Server) reconcileNodes() {
	state := s.clusterState.Current()
	alive := make(map[uint64]bool)

	for _, member := range s.members.Members() {
		id := MustIDFromString(member.Name)
		alive[id] = true
		var node *ClusterNode
		for _, n := range state.Nodes {
			if n.ID == id {
				node = n
				break
			}
		}

		if node == nil || node.Address != member.Address() || node.State != ClusterNode_ALIVE {
			cmd := &UpdateNodeCommand{
				ID:      id,
				Address: member.Address(),
				State:   ClusterNode_ALIVE,
			}
			_, err := s.apply(cmd)
			if err != nil {
				log.Printf("could not apply update node: %v", err)
				continue
			} else {
				log.Printf("updated node: %s", cmd.String())
				if cmd.State == ClusterNode_ALIVE {
					future := s.raftNode.AddVoter(raft.ServerID(member.Name), raft.ServerAddress(cmd.Address), 0, 10*time.Second)
					if err := future.Error(); err != nil {
						log.Printf("could not add peer: %v", err)
						continue
					}
				}
			}
		}
	}

	for _, node := range state.Nodes {
		if alive[node.ID] || node.State == ClusterNode_DEAD {
			continue
		}

		now := time.Now()

		cmd := &UpdateNodeCommand{
			ID:            node.ID,
			Address:       node.Address,
			State:         ClusterNode_DEAD,
			LastSeenAlive: &now,
		}
		_, err := s.apply(cmd)
		if err != nil {
			log.Printf("could not apply update node: %v", err)
			return
		}

		log.Printf("updated node: %s", cmd.String())
	}

	// TODO: remove nodes dead for more than X minutes from voters
}

func (s *Server) reconcileOpenSegments(state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
	for _, segment := range state.OpenSegments {
		s.reconcileOpenSegment(segment, state, nodeSegmentCounts, nodeMap, allCandidateNodeIDs)
	}
}

func (s *Server) reconcileOpenSegment(segment *ClusterSegment, state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
	primaryNode := nodeMap[segment.Nodes.PrimaryNodeID]
	if primaryNode.State == ClusterNode_ALIVE {
		s.reconcileOpenSegmentWithAlivePrimary(segment, state, nodeSegmentCounts, nodeMap, allCandidateNodeIDs)
	} else if primaryNode.State == ClusterNode_DEAD {
		s.reconcileOpenSegmentWithDeadPrimary(segment, state, nodeSegmentCounts, nodeMap, allCandidateNodeIDs)
	} else {
		panic("unhandled primary node state: " + primaryNode.State.String())
	}
}

func (s *Server) reconcileOpenSegmentWithAlivePrimary(segment *ClusterSegment, state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
	topic := state.GetTopic(segment.Topic.Namespace, segment.Topic.Name)
	if topic.ReplicationFactor < 1 {
		panic("replication factor is zero")
	}

	aliveReplicas := uint32(1) // 1 for primary
	for _, nodeID := range segment.Nodes.ReplicatingNodeIDs {
		if nodeMap[nodeID].State == ClusterNode_ALIVE {
			aliveReplicas++
		}
	}

	if aliveReplicas > topic.ReplicationFactor {
		cmd := &UpdateSegmentNodesCommand{
			ID:    segment.ID,
			Which: UpdateSegmentNodesCommand_OPEN,
		}
		cmd.Nodes.PrimaryNodeID = segment.Nodes.PrimaryNodeID
		if topic.ReplicationFactor-1 > 0 {
			cmd.Nodes.ReplicatingNodeIDs = make([]uint64, 0, topic.ReplicationFactor-1)
			for _, nodeID := range segment.Nodes.ReplicatingNodeIDs {
				if nodeMap[nodeID].State == ClusterNode_ALIVE {
					cmd.Nodes.ReplicatingNodeIDs = append(cmd.Nodes.ReplicatingNodeIDs, nodeID)
					if uint32(1+len(cmd.Nodes.ReplicatingNodeIDs)) >= topic.ReplicationFactor {
						break
					}
				}
			}
		}

		_, err := s.apply(cmd)
		if err != nil {
			log.Printf("could not remove segment replica(s): %v", err)
			return
		}
		log.Printf(
			"open segment %d (of topic %s/%s) was over-replicated, removed replica(s)",
			segment.ID,
			segment.Topic.Namespace,
			segment.Topic.Name,
		)

	} else if aliveReplicas < topic.ReplicationFactor {
		cmd := &UpdateSegmentNodesCommand{
			ID:    segment.ID,
			Which: UpdateSegmentNodesCommand_OPEN,
		}
		cmd.Nodes.PrimaryNodeID = segment.Nodes.PrimaryNodeID
		cmd.Nodes.ReplicatingNodeIDs = make([]uint64, 0, topic.ReplicationFactor-1)
		for _, nodeID := range segment.Nodes.ReplicatingNodeIDs {
			if nodeMap[nodeID].State == ClusterNode_ALIVE {
				cmd.Nodes.ReplicatingNodeIDs = append(cmd.Nodes.ReplicatingNodeIDs, nodeID)
			}
		}

		candidateNodeIDs := make([]uint64, 0, len(allCandidateNodeIDs))

	OUTER:
		for _, candidateNodeID := range allCandidateNodeIDs {
			if candidateNodeID == cmd.Nodes.PrimaryNodeID {
				continue
			}
			for _, replicatingNodeID := range cmd.Nodes.ReplicatingNodeIDs {
				if candidateNodeID == replicatingNodeID {
					continue OUTER
				}
			}
			candidateNodeIDs = append(candidateNodeIDs, candidateNodeID)
		}

		added := false

		for len(candidateNodeIDs) > 0 && uint32(len(cmd.Nodes.ReplicatingNodeIDs)) < topic.ReplicationFactor-1 {
			var candidateIndex = -1
			for i, candidateNodeID := range candidateNodeIDs {
				if candidateIndex == -1 || nodeSegmentCounts[candidateNodeID] < nodeSegmentCounts[candidateNodeIDs[candidateIndex]] {
					candidateIndex = i
				}
			}

			cmd.Nodes.ReplicatingNodeIDs = append(cmd.Nodes.ReplicatingNodeIDs, candidateNodeIDs[candidateIndex])
			copy(candidateNodeIDs[candidateIndex:], candidateNodeIDs[candidateIndex+1:])
			candidateNodeIDs = candidateNodeIDs[:len(candidateNodeIDs)-1]
			added = true
		}

		if added {
			_, err := s.apply(cmd)
			if err != nil {
				log.Printf("could not add segment replica(s): %v", err)
				return
			}
			log.Printf(
				"open segment %d (of topic %s/%s) was under-replicated, added replica(s)",
				segment.ID,
				segment.Topic.Namespace,
				segment.Topic.Name,
			)
		}
	}
}

func (s *Server) reconcileOpenSegmentWithDeadPrimary(segment *ClusterSegment, state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
	// TODO
}

func (s *Server) reconcileClosedSegments(state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
	for _, segment := range state.ClosedSegments {
		s.reconcileClosedSegment(segment, state, nodeSegmentCounts, nodeMap, allCandidateNodeIDs)
	}
}

func (s *Server) reconcileClosedSegment(segment *ClusterSegment, state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
	topic := state.GetTopic(segment.Topic.Namespace, segment.Topic.Name)

	if topic.Retention > 0 {
		retainTill := time.Now().Add(-topic.Retention)
		if segment.ClosedAt.Before(retainTill) {
			_, err := s.apply(&DeleteSegmentCommand{ID: segment.ID})
			if err != nil {
				log.Printf("could not delete closed segment after retention period: %v", err)
				return
			}

			log.Printf(
				"closed segment %d (of topic %s/%s) fell off retention period, deleted",
				segment.ID,
				segment.Topic.Namespace,
				segment.Topic.Name,
			)

			return
		}
	}

	aliveReplicas := uint32(0)
	aliveDone := uint32(0)
	for _, nodeID := range segment.Nodes.DoneNodeIDs {
		if nodeMap[nodeID].State == ClusterNode_ALIVE {
			aliveReplicas++
			aliveDone++
		}
	}
	for _, nodeID := range segment.Nodes.ReplicatingNodeIDs {
		if nodeMap[nodeID].State == ClusterNode_ALIVE {
			aliveReplicas++
		}
	}

	if topic.ReplicationFactor < 1 {
		panic("replication factor is zero")
	}

	if aliveReplicas > topic.ReplicationFactor {
		if aliveDone == 0 {
			log.Printf(
				"closed segment %d (of topic %s/%s) over-replicated, but none alive done replica found, won't do anything for now",
				segment.ID,
				segment.Topic.Namespace,
				segment.Topic.Name,
			)
			return
		}

		cmd := &UpdateSegmentNodesCommand{
			ID:    segment.ID,
			Which: UpdateSegmentNodesCommand_CLOSED,
		}
		cmd.Nodes.DoneNodeIDs = make([]uint64, 0, len(segment.Nodes.DoneNodeIDs))
		for _, nodeID := range segment.Nodes.DoneNodeIDs {
			if nodeMap[nodeID].State == ClusterNode_ALIVE {
				cmd.Nodes.DoneNodeIDs = append(cmd.Nodes.DoneNodeIDs, nodeID)
				if uint32(len(cmd.Nodes.DoneNodeIDs)) >= topic.ReplicationFactor {
					break
				}
			}
		}
		if uint32(len(cmd.Nodes.DoneNodeIDs)) < topic.ReplicationFactor {
			cmd.Nodes.ReplicatingNodeIDs = make([]uint64, 0, topic.ReplicationFactor-uint32(len(cmd.Nodes.DoneNodeIDs)))
			for _, nodeID := range segment.Nodes.ReplicatingNodeIDs {
				if nodeMap[nodeID].State == ClusterNode_ALIVE {
					cmd.Nodes.ReplicatingNodeIDs = append(cmd.Nodes.ReplicatingNodeIDs, nodeID)
					if uint32(len(cmd.Nodes.DoneNodeIDs)+len(cmd.Nodes.ReplicatingNodeIDs)) >= topic.ReplicationFactor {
						break
					}
				}
			}
		}

		_, err := s.apply(cmd)
		if err != nil {
			log.Printf("could not add segment replica(s): %v", err)
			return
		}
		log.Printf(
			"closed segment %d (of topic %s/%s) was over-replicated, removed replica(s)",
			segment.ID,
			segment.Topic.Namespace,
			segment.Topic.Name,
		)

	} else if aliveReplicas < topic.ReplicationFactor {
		if aliveDone == 0 {
			log.Printf(
				"closed segment %d (of topic %s/%s) under-replicated, but none alive done replica found, won't do anything for now",
				segment.ID,
				segment.Topic.Namespace,
				segment.Topic.Name,
			)
			return
		}

		cmd := &UpdateSegmentNodesCommand{
			ID:    segment.ID,
			Which: UpdateSegmentNodesCommand_CLOSED,
		}
		cmd.Nodes.DoneNodeIDs = make([]uint64, 0, len(segment.Nodes.DoneNodeIDs))
		for _, nodeID := range segment.Nodes.DoneNodeIDs {
			if nodeMap[nodeID].State == ClusterNode_ALIVE {
				cmd.Nodes.DoneNodeIDs = append(cmd.Nodes.DoneNodeIDs, nodeID)
			}
		}
		cmd.Nodes.ReplicatingNodeIDs = make([]uint64, 0, topic.ReplicationFactor-uint32(len(cmd.Nodes.DoneNodeIDs)))
		for _, nodeID := range segment.Nodes.ReplicatingNodeIDs {
			if nodeMap[nodeID].State == ClusterNode_ALIVE {
				cmd.Nodes.ReplicatingNodeIDs = append(cmd.Nodes.ReplicatingNodeIDs, nodeID)
			}
		}

		candidateNodeIDs := make([]uint64, 0, len(allCandidateNodeIDs))

	OUTER:
		for _, candidateNodeID := range allCandidateNodeIDs {
			for _, doneNodeID := range cmd.Nodes.DoneNodeIDs {
				if candidateNodeID == doneNodeID {
					continue OUTER
				}
			}
			for _, replicatingNodeID := range cmd.Nodes.ReplicatingNodeIDs {
				if candidateNodeID == replicatingNodeID {
					continue OUTER
				}
			}
			candidateNodeIDs = append(candidateNodeIDs, candidateNodeID)
		}

		added := false

		for len(candidateNodeIDs) > 0 && uint32(len(cmd.Nodes.DoneNodeIDs)+len(cmd.Nodes.ReplicatingNodeIDs)) < topic.ReplicationFactor {
			var candidateIndex = -1
			for i, candidateNodeID := range candidateNodeIDs {
				if candidateIndex == -1 || nodeSegmentCounts[candidateNodeID] < nodeSegmentCounts[candidateNodeIDs[candidateIndex]] {
					candidateIndex = i
				}
			}

			cmd.Nodes.ReplicatingNodeIDs = append(cmd.Nodes.ReplicatingNodeIDs, candidateNodeIDs[candidateIndex])
			copy(candidateNodeIDs[candidateIndex:], candidateNodeIDs[candidateIndex+1:])
			candidateNodeIDs = candidateNodeIDs[:len(candidateNodeIDs)-1]
			added = true
		}

		if added {
			_, err := s.apply(cmd)
			if err != nil {
				log.Printf("could not add segment replica(s): %v", err)
				return
			}
			log.Printf(
				"closed segment %d (of topic %s/%s) was under-replicated, added replica(s)",
				segment.ID,
				segment.Topic.Namespace,
				segment.Topic.Name,
			)
		}
	}
}
