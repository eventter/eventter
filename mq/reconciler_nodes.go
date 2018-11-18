package mq

import (
	"log"
	"time"
)

func (r *Reconciler) ReconcileNodes(state *ClusterState) {
	alive := make(map[uint64]bool)

	for _, member := range r.delegate.Members() {
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
			cmd := &ClusterUpdateNodeCommand{
				ID:      id,
				Address: member.Address(),
				State:   ClusterNode_ALIVE,
			}
			_, err := r.delegate.Apply(cmd)
			if err != nil {
				log.Printf("could not Apply update node: %v", err)
				continue
			} else {
				log.Printf("updated node: %s", cmd.String())
				if cmd.State == ClusterNode_ALIVE {
					err := r.delegate.AddVoter(member.Name, cmd.Address)
					if err != nil {
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

		cmd := &ClusterUpdateNodeCommand{
			ID:            node.ID,
			Address:       node.Address,
			State:         ClusterNode_DEAD,
			LastSeenAlive: &now,
		}
		_, err := r.delegate.Apply(cmd)
		if err != nil {
			log.Printf("could not Apply update node: %v", err)
			return
		}

		log.Printf("updated node: %s", cmd.String())
	}

	// TODO: remove nodes dead for more than X minutes from voters
}
