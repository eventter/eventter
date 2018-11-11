package mq

import (
	"log"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
)

func (s *Server) Loop(memberEventsC chan memberlist.NodeEvent) {
	isLeader := s.raftNode.State() == raft.Leader
	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case becameLeader := <-s.raftNode.LeaderCh():
			log.Printf("leadership status changed: before=%t, now=%t", isLeader, becameLeader)
			isLeader = becameLeader

		case <-ticker.C:
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

						buf, err := proto.Marshal(&Command{
							Command: &Command_UpdateNode{
								UpdateNode: cmd,
							},
						})
						if err != nil {
							log.Printf("could not marshal update node command: %v", err)
							continue
						}

						if err := s.raftNode.Apply(buf, 10*time.Second).Error(); err != nil {
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

					buf, err := proto.Marshal(&Command{
						Command: &Command_UpdateNode{
							UpdateNode: cmd,
						},
					})
					if err != nil {
						log.Printf("could not marshal update node command: %v", err)
						continue
					}

					if err := s.raftNode.Apply(buf, 10*time.Second).Error(); err != nil {
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

			// assign under-replicated segments

			// TODO
			//{
			//	state := s.clusterState.Current()
			//
			//	nodeSegmentCounts := state.CountSegmentsPerNode()
			//	nodeMap := make(map[uint64]*ClusterNode)
			//	var candidateNodeIDs []uint64
			//
			//	for _, node := range state.Nodes {
			//		nodeMap[node.ID] = node
			//		if node.State == ClusterNode_ALIVE {
			//			candidateNodeIDs = append(candidateNodeIDs, node.ID)
			//		}
			//	}
			//
			//	for _, segment := range state.OpenSegments {
			//		primaryNode := nodeMap[segment.Nodes.PrimaryNodeID]
			//		if primaryNode.State == ClusterNode_ALIVE {
			//			topic := state.GetTopic(segment.Topic.Namespace, segment.Topic.Name)
			//			if uint32(1 /* primary */ + len(segment.Nodes.ReplicatingNodeIDs)) >= topic.ReplicationFactor {
			//				continue
			//			}
			//
			//		} else if primaryNode.State == ClusterNode_DEAD && primaryNode.LastSeenAlive.Before(time.Now().Add(-5*time.Minute)) {
			//			for _, replicatingNodeID := range segment.Nodes.ReplicatingNodeIDs {
			//				replicatingNode := state.GetNode(replicatingNodeID)
			//
			//			}
			//		}
			//
			//	}
			//}

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

			buf, err := proto.Marshal(&Command{
				Command: &Command_UpdateNode{
					UpdateNode: cmd,
				},
			})
			if err != nil {
				log.Printf("could not marshal update node command: %v", err)
				continue
			}

			future := s.raftNode.Apply(buf, 10*time.Second)
			if err := future.Error(); err != nil {
				log.Printf("could not apply update node: %v", err)
				continue
			}

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
