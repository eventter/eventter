package mq

import (
	"sort"
)

func (s *ClusterState) doOpenSegment(cmd *ClusterOpenSegmentCommand) *ClusterState {
	next := &ClusterState{}
	*next = *s

	if cmd.ID > next.CurrentSegmentID {
		next.CurrentSegmentID = cmd.ID
	}

	// TODO: do binary search, then insert
	next.OpenSegments = make([]*ClusterSegment, len(s.OpenSegments)+1)
	copy(next.OpenSegments, s.OpenSegments)
	next.OpenSegments[len(s.OpenSegments)] = &ClusterSegment{
		ID:       cmd.ID,
		Owner:    cmd.Owner,
		Type:     cmd.Type,
		OpenedAt: cmd.OpenedAt,
		Nodes: ClusterSegment_Nodes{
			PrimaryNodeID:      cmd.PrimaryNodeID,
			ReplicatingNodeIDs: cmd.ReplicatingNodeIDs,
		},
	}

	sort.Slice(next.OpenSegments, func(i, j int) bool {
		return next.OpenSegments[i].ID < next.OpenSegments[j].ID
	})

	return next
}

func (s *ClusterState) doCloseSegment(cmd *ClusterCloseSegmentCommand) *ClusterState {
	// TODO: do binary search
	segmentIndex := -1
	for i, segment := range s.OpenSegments {
		if segment.ID == cmd.ID {
			segmentIndex = i
			break
		}
	}

	if segmentIndex == -1 {
		return s
	}

	nextSegment := &ClusterSegment{}
	*nextSegment = *s.OpenSegments[segmentIndex]

	next := &ClusterState{}
	*next = *s

	next.OpenSegments = make([]*ClusterSegment, len(s.OpenSegments)-1)
	copy(next.OpenSegments[:segmentIndex], s.OpenSegments[:segmentIndex])
	copy(next.OpenSegments[segmentIndex:], s.OpenSegments[segmentIndex+1:])

	// TODO: do binary search, then insert
	next.ClosedSegments = make([]*ClusterSegment, len(s.ClosedSegments)+1)
	copy(next.ClosedSegments, s.ClosedSegments)
	next.ClosedSegments[len(s.ClosedSegments)] = nextSegment

	nextSegment.ClosedAt = cmd.ClosedAt
	nextSegment.Size_ = cmd.Size_
	nextSegment.Sha1 = cmd.Sha1
	nextSegment.Nodes.PrimaryNodeID = 0
	nextSegment.Nodes.DoneNodeIDs = []uint64{cmd.DoneNodeID}

	sort.Slice(next.ClosedSegments, func(i, j int) bool {
		return next.ClosedSegments[i].ID < next.ClosedSegments[j].ID
	})

	return next
}

func (s *ClusterState) doUpdateSegmentNodes(cmd *ClusterUpdateSegmentNodesCommand) *ClusterState {
	if cmd.Which == ClusterUpdateSegmentNodesCommand_OPEN {
		i := sort.Search(len(s.OpenSegments), func(i int) bool { return s.OpenSegments[i].ID >= cmd.ID })
		if i < len(s.OpenSegments) && s.OpenSegments[i].ID == cmd.ID {
			next := &ClusterState{}
			*next = *s
			next.OpenSegments = s.doUpdateSegmentNodesIn(s.OpenSegments, i, cmd)
			return next
		}

	} else if cmd.Which == ClusterUpdateSegmentNodesCommand_CLOSED {
		i := sort.Search(len(s.ClosedSegments), func(i int) bool { return s.ClosedSegments[i].ID >= cmd.ID })
		if i < len(s.ClosedSegments) && s.ClosedSegments[i].ID == cmd.ID {
			next := &ClusterState{}
			*next = *s
			next.ClosedSegments = s.doUpdateSegmentNodesIn(s.ClosedSegments, i, cmd)
			return next
		}

	} else {
		panic("unhandled which: " + cmd.Which.String())
	}

	return s
}

func (s *ClusterState) doUpdateSegmentNodesIn(segments []*ClusterSegment, segmentIndex int, cmd *ClusterUpdateSegmentNodesCommand) []*ClusterSegment {
	nextSegments := make([]*ClusterSegment, len(segments))
	copy(nextSegments, segments)

	nextSegment := &ClusterSegment{}
	*nextSegment = *segments[segmentIndex]
	nextSegments[segmentIndex] = nextSegment

	nextSegment.Nodes = cmd.Nodes

	return nextSegments
}

func (s *ClusterState) doDeleteSegment(cmd *ClusterDeleteSegmentCommand) *ClusterState {
	if cmd.Which == ClusterDeleteSegmentCommand_OPEN {
		i := sort.Search(len(s.OpenSegments), func(i int) bool { return s.OpenSegments[i].ID >= cmd.ID })
		if i < len(s.OpenSegments) && s.OpenSegments[i].ID == cmd.ID {
			next := &ClusterState{}
			*next = *s
			next.OpenSegments = make([]*ClusterSegment, len(s.OpenSegments)-1)
			copy(next.OpenSegments[:i], s.OpenSegments[:i])
			copy(next.OpenSegments[i:], s.OpenSegments[i+1:])

			return next
		}

	} else if cmd.Which == ClusterDeleteSegmentCommand_CLOSED {
		i := sort.Search(len(s.ClosedSegments), func(i int) bool { return s.ClosedSegments[i].ID >= cmd.ID })
		if i < len(s.ClosedSegments) && s.ClosedSegments[i].ID == cmd.ID {
			next := &ClusterState{}
			*next = *s
			next.ClosedSegments = make([]*ClusterSegment, len(s.ClosedSegments)-1)
			copy(next.ClosedSegments[:i], s.ClosedSegments[:i])
			copy(next.ClosedSegments[i:], s.ClosedSegments[i+1:])

			return next
		}

	} else {
		panic("unhandled which: " + cmd.Which.String())
	}

	return s
}
