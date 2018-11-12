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

			// mark nodes ALIVE or DEAD, add peers, remove peers
			{
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
						_, err := s.apply(cmd, 10*time.Second)
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
					_, err := s.apply(cmd, 10*time.Second)
					if err != nil {
						log.Printf("could not apply update node: %v", err)
						continue
					}

					log.Printf("updated node: %s", cmd.String())
				}

				// TODO: remove nodes dead for more than X minutes from voters

				if err := s.raftNode.Barrier(10 * time.Second).Error(); err != nil {
					log.Printf("could not add barrier: %v", err)
					continue
				}
			}

			// re-assign under-/over-replicated segments
			{
				state := s.clusterState.Current()
				wontRecoverTimeout := 5 * time.Minute
				wontRecover := time.Now().Add(-wontRecoverTimeout)

				nodeSegmentCounts := state.CountSegmentsPerNode()
				nodeMap := make(map[uint64]*ClusterNode)
				var allCandidateNodeIDs []uint64

				for _, node := range state.Nodes {
					nodeMap[node.ID] = node
					if node.State == ClusterNode_ALIVE {
						allCandidateNodeIDs = append(allCandidateNodeIDs, node.ID)
					}
				}

				for _, segment := range state.OpenSegments {
					primaryNode := nodeMap[segment.Nodes.PrimaryNodeID]
					if primaryNode.State == ClusterNode_ALIVE {
						topic := state.GetTopic(segment.Topic.Namespace, segment.Topic.Name)
						if topic.ReplicationFactor < 1 {
							panic("replication factor is zero")
						}

						currentReplicas := uint32(1 /* primary */ + len(segment.Nodes.ReplicatingNodeIDs))

						if currentReplicas > topic.ReplicationFactor {
							cmd := &UpdateSegmentNodesCommand{
								ID: segment.ID,
							}
							cmd.Nodes.PrimaryNodeID = segment.Nodes.PrimaryNodeID
							if topic.ReplicationFactor-1 > 0 {
								cmd.Nodes.ReplicatingNodeIDs = make([]uint64, topic.ReplicationFactor-1)
								copy(cmd.Nodes.ReplicatingNodeIDs, segment.Nodes.ReplicatingNodeIDs)
							}

							_, err := s.apply(cmd, 10*time.Second)
							if err != nil {
								log.Printf("could not remove segment replica(s): %v", err)
								continue
							}
							log.Printf(
								"open segment %d (of topic %s/%s) was over-replicated, removed replica(s)",
								segment.ID,
								segment.Topic.Namespace,
								segment.Topic.Name,
							)

						} else if currentReplicas < topic.ReplicationFactor {
							cmd := &UpdateSegmentNodesCommand{
								ID: segment.ID,
							}
							cmd.Nodes.PrimaryNodeID = segment.Nodes.PrimaryNodeID
							cmd.Nodes.ReplicatingNodeIDs = make([]uint64, len(segment.Nodes.ReplicatingNodeIDs), topic.ReplicationFactor-1)
							copy(cmd.Nodes.ReplicatingNodeIDs, segment.Nodes.ReplicatingNodeIDs)

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
								_, err := s.apply(cmd, 10*time.Second)
								if err != nil {
									log.Printf("could not add segment replica(s): %v", err)
									continue
								}
								log.Printf(
									"open segment %d (of topic %s/%s) was under-replicated, added replica(s)",
									segment.ID,
									segment.Topic.Namespace,
									segment.Topic.Name,
								)
							}
						}

					} else if primaryNode.State == ClusterNode_DEAD && primaryNode.LastSeenAlive.Before(wontRecover) {
						log.Printf(
							"primary node for segment %d (of topic %s/%s) dead for more than %s",
							segment.ID,
							segment.Topic.Namespace,
							segment.Topic.Name,
							wontRecoverTimeout.String(),
						)

						// TODO
					}

				}
			}

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

			_, err := s.apply(cmd, 10*time.Second)
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
