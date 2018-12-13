package mq

import (
	"sort"
)

func (s *ClusterState) doCreateConsumerGroup(cmd *ClusterCommandConsumerGroupCreate) *ClusterState {
	next := &ClusterState{}
	*next = *s

	namespace, namespaceIndex := s.FindNamespace(cmd.Namespace)
	if namespace == nil {
		panic("namespace must exist")
	}

	nextNamespace := &ClusterNamespace{}
	*nextNamespace = *namespace

	next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces))
	copy(next.Namespaces[:namespaceIndex], s.Namespaces[:namespaceIndex])
	next.Namespaces[namespaceIndex] = nextNamespace
	copy(next.Namespaces[namespaceIndex+1:], s.Namespaces[namespaceIndex+1:])

	consumerGroup, consumerGroupIndex := namespace.FindConsumerGroup(cmd.ConsumerGroup.Name)
	var nextConsumerGroup *ClusterConsumerGroup
	if consumerGroup == nil {
		nextConsumerGroup = &ClusterConsumerGroup{
			Name: cmd.ConsumerGroup.Name,
		}

		nextNamespace.ConsumerGroups = make([]*ClusterConsumerGroup, len(namespace.ConsumerGroups)+1)
		copy(nextNamespace.ConsumerGroups, namespace.ConsumerGroups)
		nextNamespace.ConsumerGroups[len(namespace.ConsumerGroups)] = nextConsumerGroup

	} else {
		nextConsumerGroup = &ClusterConsumerGroup{}
		*nextConsumerGroup = *consumerGroup

		nextNamespace.ConsumerGroups = make([]*ClusterConsumerGroup, len(namespace.ConsumerGroups))
		copy(nextNamespace.ConsumerGroups[:consumerGroupIndex], namespace.ConsumerGroups[:consumerGroupIndex])
		nextNamespace.ConsumerGroups[consumerGroupIndex] = nextConsumerGroup
		copy(nextNamespace.ConsumerGroups[consumerGroupIndex+1:], namespace.ConsumerGroups[consumerGroupIndex+1:])
	}

	nextConsumerGroup.Bindings = cmd.ConsumerGroup.Bindings
	nextConsumerGroup.Size_ = cmd.ConsumerGroup.Size_
	if nextConsumerGroup.CreatedAt.IsZero() {
		nextConsumerGroup.CreatedAt = cmd.ConsumerGroup.CreatedAt
	}

	return next
}

func (s *ClusterState) doDeleteConsumerGroup(cmd *ClusterCommandConsumerGroupDelete) *ClusterState {
	namespace, namespaceIndex := s.FindNamespace(cmd.Namespace)
	if namespace == nil {
		panic("namespace must exist")
	}

	_, consumerGroupIndex := namespace.FindConsumerGroup(cmd.Name)
	if consumerGroupIndex == -1 {
		return s
	}

	next := &ClusterState{}
	*next = *s
	next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces))
	copy(next.Namespaces, s.Namespaces)

	nextNamespace := &ClusterNamespace{}
	*nextNamespace = *namespace
	next.Namespaces[namespaceIndex] = nextNamespace

	// remove consumer group from namespace
	nextNamespace.ConsumerGroups = make([]*ClusterConsumerGroup, len(namespace.ConsumerGroups)-1)
	copy(nextNamespace.ConsumerGroups[:consumerGroupIndex], namespace.ConsumerGroups[:consumerGroupIndex])
	copy(nextNamespace.ConsumerGroups[consumerGroupIndex:], namespace.ConsumerGroups[consumerGroupIndex+1:])

	// remove offset commit segments
	nextOpenSegments := make([]*ClusterSegment, 0, len(s.OpenSegments))
	openSegmentsChanged := false
	for _, segment := range s.OpenSegments {
		if segment.Type == ClusterSegment_CONSUMER_GROUP_OFFSET_COMMITS &&
			segment.Owner.Namespace == cmd.Namespace &&
			segment.Owner.Name == cmd.Name {
			openSegmentsChanged = true
		} else {
			nextOpenSegments = append(nextOpenSegments, segment)
		}
	}
	if openSegmentsChanged {
		next.OpenSegments = nextOpenSegments
	}
	nextClosedSegments := make([]*ClusterSegment, 0, len(s.ClosedSegments))
	closedSegmentsChanged := false
	for _, segment := range s.ClosedSegments {
		if segment.Type == ClusterSegment_CONSUMER_GROUP_OFFSET_COMMITS &&
			segment.Owner.Namespace == cmd.Namespace &&
			segment.Owner.Name == cmd.Name {
			closedSegmentsChanged = true
		} else {
			nextClosedSegments = append(nextClosedSegments, segment)
		}
	}
	if closedSegmentsChanged {
		next.ClosedSegments = nextClosedSegments
	}

	return next
}

func (s *ClusterState) doUpdateOffsetCommits(cmd *ClusterCommandConsumerGroupOffsetCommitsUpdate) *ClusterState {
	namespace, namespaceIndex := s.FindNamespace(cmd.Namespace)
	if namespace == nil {
		panic("namespace must exist")
	}

	consumerGroup, consumerGroupIndex := namespace.FindConsumerGroup(cmd.Name)
	if consumerGroupIndex == -1 {
		return s
	}

	next := &ClusterState{}
	*next = *s

	nextNamespace := &ClusterNamespace{}
	*nextNamespace = *namespace
	next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces))
	copy(next.Namespaces, s.Namespaces)
	next.Namespaces[namespaceIndex] = nextNamespace

	nextConsumerGroup := &ClusterConsumerGroup{}
	*nextConsumerGroup = *consumerGroup
	nextNamespace.ConsumerGroups = make([]*ClusterConsumerGroup, len(namespace.ConsumerGroups))
	copy(nextNamespace.ConsumerGroups, namespace.ConsumerGroups)
	nextNamespace.ConsumerGroups[consumerGroupIndex] = nextConsumerGroup

	nextConsumerGroup.OffsetCommits = cmd.OffsetCommits

	sort.Slice(nextConsumerGroup.OffsetCommits, func(i, j int) bool {
		return nextConsumerGroup.OffsetCommits[i].SegmentID < nextConsumerGroup.OffsetCommits[j].SegmentID
	})

	return next
}
