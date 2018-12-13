package mq

func (s *ClusterState) doCreateTopic(cmd *ClusterCommandTopicCreate) *ClusterState {
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

	topic, topicIndex := namespace.FindTopic(cmd.Topic.Name)
	var nextTopic *ClusterTopic
	if topic == nil {
		nextTopic = &ClusterTopic{
			Name: cmd.Topic.Name,
		}

		nextNamespace.Topics = make([]*ClusterTopic, len(namespace.Topics)+1)
		copy(nextNamespace.Topics, namespace.Topics)
		nextNamespace.Topics[len(namespace.Topics)] = nextTopic

	} else {
		nextTopic = &ClusterTopic{}
		*nextTopic = *topic

		nextNamespace.Topics = make([]*ClusterTopic, len(namespace.Topics))
		copy(nextNamespace.Topics[:topicIndex], namespace.Topics[:topicIndex])
		nextNamespace.Topics[topicIndex] = nextTopic
		copy(nextNamespace.Topics[topicIndex+1:], namespace.Topics[topicIndex+1:])
	}

	nextTopic.Type = cmd.Topic.Type
	nextTopic.Shards = cmd.Topic.Shards
	nextTopic.ReplicationFactor = cmd.Topic.ReplicationFactor
	nextTopic.Retention = cmd.Topic.Retention

	return next
}

func (s *ClusterState) doDeleteTopic(cmd *ClusterCommandTopicDelete) *ClusterState {
	namespace, namespaceIndex := s.FindNamespace(cmd.Namespace)
	if namespace == nil {
		panic("namespace must exist")
	}

	_, topicIndex := namespace.FindTopic(cmd.Name)
	if topicIndex == -1 {
		return s
	}

	next := &ClusterState{}
	*next = *s
	next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces))
	copy(next.Namespaces, s.Namespaces)

	nextNamespace := &ClusterNamespace{}
	*nextNamespace = *namespace
	next.Namespaces[namespaceIndex] = nextNamespace

	// remove topic from namespace
	nextNamespace.Topics = make([]*ClusterTopic, len(namespace.Topics)-1)
	copy(nextNamespace.Topics[:topicIndex], namespace.Topics[:topicIndex])
	copy(nextNamespace.Topics[topicIndex:], namespace.Topics[topicIndex+1:])

	// remove consumer group bindings & segment references
	var consumerGroupsChanged = false
	var nextConsumerGroups []*ClusterConsumerGroup
	for _, consumerGroup := range namespace.ConsumerGroups {
		var changed = false
		var nextBindings []*ClusterConsumerGroup_Binding
		for _, binding := range consumerGroup.Bindings {
			if binding.TopicName == cmd.Name {
				changed = true
			} else {
				nextBindings = append(nextBindings, binding)
			}
		}
		var nextOffsetCommits []*ClusterConsumerGroup_OffsetCommit
		for _, commit := range consumerGroup.OffsetCommits {
			segment := s.GetSegment(commit.SegmentID)
			if segment == nil || (segment.Type == ClusterSegment_TOPIC &&
				segment.Owner.Namespace == cmd.Namespace &&
				segment.Owner.Name == cmd.Name) {

				changed = true
			} else {
				nextOffsetCommits = append(nextOffsetCommits, commit)
			}
		}
		if !changed {
			nextConsumerGroups = append(nextConsumerGroups, consumerGroup)
			continue
		}

		nextConsumerGroup := &ClusterConsumerGroup{}
		*nextConsumerGroup = *consumerGroup
		nextConsumerGroup.Bindings = nextBindings
		nextConsumerGroup.OffsetCommits = nextOffsetCommits

		nextConsumerGroups = append(nextConsumerGroups, nextConsumerGroup)
		consumerGroupsChanged = true
	}

	if consumerGroupsChanged {
		nextNamespace.ConsumerGroups = nextConsumerGroups
	}

	// remove topic segments
	nextOpenSegments := make([]*ClusterSegment, 0, len(s.OpenSegments))
	openSegmentsChanged := false
	for _, segment := range s.OpenSegments {
		if segment.Type == ClusterSegment_TOPIC && segment.Owner.Namespace == cmd.Namespace && segment.Owner.Name == cmd.Name {
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
		if segment.Type == ClusterSegment_TOPIC && segment.Owner.Namespace == cmd.Namespace && segment.Owner.Name == cmd.Name {
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
