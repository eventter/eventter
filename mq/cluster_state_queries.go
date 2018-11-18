package mq

func (s *ClusterState) ListTopics(namespaceName string, topicName string) (uint64, []*ClusterTopic) {
	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return s.Index, nil
	}

	if topicName == "" {
		return s.Index, namespace.Topics
	} else {
		topic, _ := namespace.findTopic(topicName)
		if topic == nil {
			return s.Index, nil
		} else {
			return s.Index, []*ClusterTopic{topic}
		}
	}
}

func (s *ClusterState) ListConsumerGroups(namespaceName string, consumerGroupName string) (uint64, []*ClusterConsumerGroup) {
	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return s.Index, nil
	}

	if consumerGroupName == "" {
		return s.Index, namespace.ConsumerGroups
	} else {
		consumerGroup, _ := namespace.findConsumerGroup(consumerGroupName)
		if consumerGroup == nil {
			return s.Index, nil
		} else {
			return s.Index, []*ClusterConsumerGroup{consumerGroup}
		}
	}
}

func (s *ClusterState) AnyConsumerGroupReferencesTopic(namespaceName string, topicName string) bool {
	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return false
	}

	for _, consumerGroup := range namespace.ConsumerGroups {
		for _, binding := range consumerGroup.Bindings {
			if binding.TopicName == topicName {
				return true
			}
		}
	}

	return false
}

func (s *ClusterState) TopicExists(namespaceName string, topicName string) bool {
	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return false
	}

	topic, _ := namespace.findTopic(topicName)
	return topic != nil
}

func (s *ClusterState) GetTopic(namespaceName string, topicName string) *ClusterTopic {
	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return nil
	}

	topic, _ := namespace.findTopic(topicName)
	return topic
}

func (s *ClusterState) ConsumerGroupExists(namespaceName string, consumerGroupName string) bool {
	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return false
	}

	consumerGroup, _ := namespace.findConsumerGroup(consumerGroupName)
	return consumerGroup != nil
}

func (s *ClusterState) GetConsumerGroup(namespaceName string, consumerGroupName string) *ClusterConsumerGroup {
	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return nil
	}

	consumerGroup, _ := namespace.findConsumerGroup(consumerGroupName)
	return consumerGroup
}

func (s *ClusterState) FindOpenSegmentsFor(segmentType ClusterSegment_Type, namespaceName string, name string) []*ClusterSegment {
	var segments []*ClusterSegment

	for _, segment := range s.OpenSegments {
		if segment.Type == segmentType && segment.Owner.Namespace == namespaceName && segment.Owner.Name == name {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (s *ClusterState) FindOpenSegmentsIn(nodeID uint64) []*ClusterSegment {
	var segments []*ClusterSegment

	for _, segment := range s.OpenSegments {
		if segment.Nodes.PrimaryNodeID == nodeID {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (s *ClusterState) GetOpenSegment(id uint64) *ClusterSegment {
	for _, segment := range s.OpenSegments {
		if segment.ID == id {
			return segment
		}
	}

	return nil
}

func (s *ClusterState) GetClosedSegment(id uint64) *ClusterSegment {
	for _, segment := range s.ClosedSegments {
		if segment.ID == id {
			return segment
		}
	}

	return nil
}

func (s *ClusterState) CountSegmentsPerNode() map[uint64]int {
	m := make(map[uint64]int)

	for _, segments := range [][]*ClusterSegment{s.OpenSegments, s.ClosedSegments} {
		for _, segment := range segments {
			if segment.Nodes.PrimaryNodeID > 0 {
				m[segment.Nodes.PrimaryNodeID]++
			}
			for _, nodeID := range segment.Nodes.ReplicatingNodeIDs {
				m[nodeID]++
			}
			for _, nodeID := range segment.Nodes.DoneNodeIDs {
				m[nodeID]++
			}
		}
	}

	return m
}

func (s *ClusterState) GetNode(nodeID uint64) *ClusterNode {
	for _, node := range s.Nodes {
		if node.ID == nodeID {
			return node
		}
	}

	return nil
}

func (s *ClusterState) findNamespace(name string) (*ClusterNamespace, int) {
	for index, namespace := range s.Namespaces {
		if namespace.Name == name {
			return namespace, index
		}
	}
	return nil, -1
}

func (n *ClusterNamespace) findTopic(name string) (*ClusterTopic, int) {
	for index, topic := range n.Topics {
		if topic.Name == name {
			return topic, index
		}
	}
	return nil, -1
}

func (n *ClusterNamespace) findConsumerGroup(name string) (*ClusterConsumerGroup, int) {
	for index, consumerGroup := range n.ConsumerGroups {
		if consumerGroup.Name == name {
			return consumerGroup, index
		}
	}
	return nil, -1
}

func (n *ClusterNamespace) isEmpty() bool {
	return len(n.Topics) == 0 && len(n.ConsumerGroups) == 0
}
