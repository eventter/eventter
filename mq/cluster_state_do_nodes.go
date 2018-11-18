package mq

import (
	"sort"
)

func (s *ClusterState) doUpdateNode(cmd *ClusterUpdateNodeCommand) *ClusterState {
	nodeIndex := -1
	for i, node := range s.Nodes {
		if node.ID == cmd.ID {
			nodeIndex = i
			break
		}
	}

	next := &ClusterState{}
	*next = *s

	nextNode := &ClusterNode{}

	if nodeIndex == -1 {
		next.Nodes = make([]*ClusterNode, len(s.Nodes)+1)
		copy(next.Nodes, s.Nodes)
		next.Nodes[len(s.Nodes)] = nextNode
	} else {
		*nextNode = *s.Nodes[nodeIndex]
		next.Nodes = make([]*ClusterNode, len(s.Nodes))
		copy(next.Nodes[:nodeIndex], s.Nodes[:nodeIndex])
		next.Nodes[nodeIndex] = nextNode
		copy(next.Nodes[nodeIndex+1:], s.Nodes[nodeIndex+1:])
	}

	nextNode.ID = cmd.ID
	nextNode.Address = cmd.Address
	nextNode.State = cmd.State
	nextNode.LastSeenAlive = cmd.LastSeenAlive

	sort.Slice(next.Nodes, func(i, j int) bool {
		return next.Nodes[i].ID < next.Nodes[j].ID
	})

	return next
}
