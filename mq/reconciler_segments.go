package mq

import (
	"context"
	"log"
	"time"
)

func (r *Reconciler) ReconcileSegments(state *ClusterState) {
	nodeSegmentCounts := state.CountSegmentsPerNode()
	nodeMap := make(map[uint64]*ClusterNode)
	aliveNodeIDs := make([]uint64, 0, len(state.Nodes))
	for _, node := range state.Nodes {
		nodeMap[node.ID] = node
		if node.State == ClusterNode_ALIVE {
			aliveNodeIDs = append(aliveNodeIDs, node.ID)
		}
	}

	r.reconcileOpenSegments(state, nodeSegmentCounts, nodeMap, aliveNodeIDs)
	r.reconcileClosedSegments(state, nodeSegmentCounts, nodeMap, aliveNodeIDs)
}

func (r *Reconciler) reconcileOpenSegments(state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
	for _, segment := range state.OpenSegments {
		r.reconcileOpenSegment(segment, state, nodeSegmentCounts, nodeMap, allCandidateNodeIDs)
	}
}

func (r *Reconciler) reconcileOpenSegment(segment *ClusterSegment, state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
	topic := state.GetTopic(segment.Topic.Namespace, segment.Topic.Name)

	if topic == nil {
		_, err := r.delegate.Apply(&DeleteSegmentCommand{
			ID:    segment.ID,
			Which: DeleteSegmentCommand_OPEN,
		})
		if err != nil {
			log.Printf("could not delete open segment with non-existent topic: %v", err)
			return
		}
		log.Printf(
			"topic %s/%s does not exist, open segment %d deleted",
			segment.Topic.Namespace,
			segment.Topic.Name,
			segment.ID,
		)
		return
	}

	primaryNode := nodeMap[segment.Nodes.PrimaryNodeID]
	if primaryNode.State == ClusterNode_ALIVE {
		r.reconcileOpenSegmentWithAlivePrimary(segment, topic, state, nodeSegmentCounts, nodeMap, allCandidateNodeIDs)
	} else if primaryNode.State == ClusterNode_DEAD {
		r.reconcileOpenSegmentWithDeadPrimary(segment, state, nodeSegmentCounts, nodeMap, allCandidateNodeIDs)
	} else {
		panic("unhandled primary node state: " + primaryNode.State.String())
	}
}

func (r *Reconciler) reconcileOpenSegmentWithAlivePrimary(segment *ClusterSegment, topic *ClusterTopic, state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
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

		_, err := r.delegate.Apply(cmd)
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
			_, err := r.delegate.Apply(cmd)
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

func (r *Reconciler) reconcileOpenSegmentWithDeadPrimary(segment *ClusterSegment, state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
	if len(segment.Nodes.ReplicatingNodeIDs) == 0 {
		log.Printf(
			"open segment %d (of topic %s/%s) has dead primary and no replica to promote to primary",
			segment.ID,
			segment.Topic.Namespace,
			segment.Topic.Name,
		)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var newPrimaryNodeID uint64 = 0
	var newPrimarySegmentSize int64 = -1
	for _, replicaNodeID := range segment.Nodes.ReplicatingNodeIDs {
		replica := nodeMap[replicaNodeID]
		if replica.State != ClusterNode_ALIVE {
			continue
		}

		replicaSegmentSize, err := r.delegate.GetSegmentSizeFromNode(ctx, segment.ID, replicaNodeID, replica.Address)
		if err != nil {
			log.Printf(
				"could not get size of segment %d from node %d: %v",
				segment.ID,
				replicaNodeID,
				err,
			)
			return
		}

		if replicaSegmentSize > newPrimarySegmentSize {
			newPrimaryNodeID = replicaNodeID
			newPrimarySegmentSize = replicaSegmentSize
		}
	}

	if newPrimaryNodeID == 0 {
		log.Printf(
			"open segment %d (of topic %s/%s) has dead primary and no suitable replica",
			segment.ID,
			segment.Topic.Namespace,
			segment.Topic.Name,
		)
		return
	}

	replicatingNodeIDs := make([]uint64, 0, len(segment.Nodes.ReplicatingNodeIDs)-1)
	for _, nodeID := range segment.Nodes.ReplicatingNodeIDs {
		if nodeID != newPrimaryNodeID && nodeMap[nodeID].State == ClusterNode_ALIVE {
			replicatingNodeIDs = append(replicatingNodeIDs, nodeID)
		}
	}

	cmd := &UpdateSegmentNodesCommand{
		ID:    segment.ID,
		Which: UpdateSegmentNodesCommand_OPEN,
		Nodes: ClusterSegment_Nodes{
			PrimaryNodeID:      newPrimaryNodeID,
			ReplicatingNodeIDs: replicatingNodeIDs,
		},
	}

	_, err := r.delegate.Apply(cmd)
	if err != nil {
		log.Printf("could not change primary of segment %d: %v", segment.ID, err)
		return
	}
	log.Printf(
		"open segment %d (of topic %s/%s) changed primary from %d to %d",
		segment.ID,
		segment.Topic.Namespace,
		segment.Topic.Name,
		segment.Nodes.PrimaryNodeID,
		newPrimaryNodeID,
	)
}

func (r *Reconciler) reconcileClosedSegments(state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
	for _, segment := range state.ClosedSegments {
		r.reconcileClosedSegment(segment, state, nodeSegmentCounts, nodeMap, allCandidateNodeIDs)
	}
}

func (r *Reconciler) reconcileClosedSegment(segment *ClusterSegment, state *ClusterState, nodeSegmentCounts map[uint64]int, nodeMap map[uint64]*ClusterNode, allCandidateNodeIDs []uint64) {
	topic := state.GetTopic(segment.Topic.Namespace, segment.Topic.Name)

	if topic == nil {
		_, err := r.delegate.Apply(&DeleteSegmentCommand{
			ID:    segment.ID,
			Which: DeleteSegmentCommand_CLOSED,
		})
		if err != nil {
			log.Printf("could not delete closed segment with non-existent topic: %v", err)
			return
		}

		log.Printf(
			"topic %s/%s does not exist, closed segment %d deleted",
			segment.Topic.Namespace,
			segment.Topic.Name,
			segment.ID,
		)
		return

	} else if topic.Retention > 0 {
		retainTill := time.Now().Add(-topic.Retention)
		if segment.ClosedAt.Before(retainTill) {
			_, err := r.delegate.Apply(&DeleteSegmentCommand{
				ID:    segment.ID,
				Which: DeleteSegmentCommand_CLOSED,
			})
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

		_, err := r.delegate.Apply(cmd)
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
			_, err := r.delegate.Apply(cmd)
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
