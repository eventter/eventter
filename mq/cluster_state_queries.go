package mq

import (
	"sort"
)

func (n *ClusterNamespace) ListTopics(namespaceName string, topicName string) []*ClusterTopic {
	if topicName == "" {
		return n.Topics
	} else {
		topic, _ := n.FindTopic(topicName)
		if topic == nil {
			return nil
		} else {
			return []*ClusterTopic{topic}
		}
	}
}

func (n *ClusterNamespace) ListConsumerGroups(namespaceName string, consumerGroupName string) []*ClusterConsumerGroup {
	if consumerGroupName == "" {
		return n.ConsumerGroups
	} else {
		consumerGroup, _ := n.FindConsumerGroup(consumerGroupName)
		if consumerGroup == nil {
			return nil
		} else {
			return []*ClusterConsumerGroup{consumerGroup}
		}
	}
}

func (s *ClusterState) AnyConsumerGroupReferencesTopic(namespaceName string, topicName string) bool {
	namespace, _ := s.FindNamespace(namespaceName)
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
	namespace, _ := s.FindNamespace(namespaceName)
	if namespace == nil {
		return false
	}

	topic, _ := namespace.FindTopic(topicName)
	return topic != nil
}

func (s *ClusterState) GetTopic(namespaceName string, topicName string) *ClusterTopic {
	namespace, _ := s.FindNamespace(namespaceName)
	if namespace == nil {
		return nil
	}

	topic, _ := namespace.FindTopic(topicName)
	return topic
}

func (s *ClusterState) ConsumerGroupExists(namespaceName string, consumerGroupName string) bool {
	namespace, _ := s.FindNamespace(namespaceName)
	if namespace == nil {
		return false
	}

	consumerGroup, _ := namespace.FindConsumerGroup(consumerGroupName)
	return consumerGroup != nil
}

func (s *ClusterState) GetConsumerGroup(namespaceName string, consumerGroupName string) *ClusterConsumerGroup {
	namespace, _ := s.FindNamespace(namespaceName)
	if namespace == nil {
		return nil
	}

	consumerGroup, _ := namespace.FindConsumerGroup(consumerGroupName)
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
	i := sort.Search(len(s.OpenSegments), func(i int) bool { return s.OpenSegments[i].ID >= id })

	if i < len(s.OpenSegments) && s.OpenSegments[i].ID == id {
		return s.OpenSegments[i]
	}

	return nil
}

func (s *ClusterState) GetClosedSegment(id uint64) *ClusterSegment {
	i := sort.Search(len(s.ClosedSegments), func(i int) bool { return s.ClosedSegments[i].ID >= id })

	if i < len(s.ClosedSegments) && s.ClosedSegments[i].ID == id {
		return s.ClosedSegments[i]
	}

	return nil
}

func (s *ClusterState) GetSegment(id uint64) *ClusterSegment {
	if segment := s.GetOpenSegment(id); segment != nil {
		return segment
	}
	if segment := s.GetClosedSegment(id); segment != nil {
		return segment
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

func (s *ClusterState) FindNamespace(name string) (*ClusterNamespace, int) {
	for index, namespace := range s.Namespaces {
		if namespace.Name == name {
			return namespace, index
		}
	}
	return nil, -1
}

func (n *ClusterNamespace) FindTopic(name string) (*ClusterTopic, int) {
	for index, topic := range n.Topics {
		if topic.Name == name {
			return topic, index
		}
	}
	return nil, -1
}

func (n *ClusterNamespace) FindConsumerGroup(name string) (*ClusterConsumerGroup, int) {
	for index, consumerGroup := range n.ConsumerGroups {
		if consumerGroup.Name == name {
			return consumerGroup, index
		}
	}
	return nil, -1
}
