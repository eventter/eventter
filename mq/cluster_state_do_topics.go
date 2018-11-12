package mq

import (
	"eventter.io/mq/client"
)

func (s *ClusterState) doConfigureTopic(cmd *client.ConfigureTopicRequest) *ClusterState {
	next := &ClusterState{}
	*next = *s

	namespace, namespaceIndex := s.findNamespace(cmd.Topic.Namespace)
	var (
		nextNamespace *ClusterNamespace
		nextTopic     *ClusterTopic
	)

	if namespace == nil {
		nextNamespace = &ClusterNamespace{
			Name: cmd.Topic.Namespace,
		}

		next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces)+1)
		copy(next.Namespaces, s.Namespaces)
		next.Namespaces[len(s.Namespaces)] = nextNamespace

		nextTopic = &ClusterTopic{
			Name: cmd.Topic.Name,
		}
		nextNamespace.Topics = []*ClusterTopic{nextTopic}

	} else {
		nextNamespace = &ClusterNamespace{}
		*nextNamespace = *namespace

		next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces))
		copy(next.Namespaces[:namespaceIndex], s.Namespaces[:namespaceIndex])
		next.Namespaces[namespaceIndex] = nextNamespace
		copy(next.Namespaces[namespaceIndex+1:], s.Namespaces[namespaceIndex+1:])

		topic, topicIndex := namespace.findTopic(cmd.Topic.Name)
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
	}

	nextTopic.Type = cmd.Type
	nextTopic.Shards = cmd.Shards
	nextTopic.ReplicationFactor = cmd.ReplicationFactor
	nextTopic.Retention = cmd.Retention

	return next
}

func (s *ClusterState) doDeleteTopic(cmd *client.DeleteTopicRequest) *ClusterState {
	namespace, namespaceIndex := s.findNamespace(cmd.Topic.Namespace)
	if namespace == nil {
		return s
	}

	_, topicIndex := namespace.findTopic(cmd.Topic.Name)
	if topicIndex == -1 {
		return s
	}

	next := &ClusterState{}
	*next = *s

	nextNamespace := &ClusterNamespace{}
	*nextNamespace = *namespace

	nextNamespace.Topics = make([]*ClusterTopic, len(namespace.Topics)-1)
	copy(nextNamespace.Topics[:topicIndex], namespace.Topics[:topicIndex])
	copy(nextNamespace.Topics[topicIndex:], namespace.Topics[topicIndex+1:])

	var consumerGroupsChanged = false
	var nextConsumerGroups []*ClusterConsumerGroup

	for _, consumerGroup := range namespace.ConsumerGroups {
		var bindingsChanged = false
		var nextBindings []*ClusterConsumerGroup_Binding
		for _, binding := range consumerGroup.Bindings {
			if binding.TopicName != cmd.Topic.Name {
				nextBindings = append(nextBindings, binding)
			} else {
				bindingsChanged = true
			}
		}
		if !bindingsChanged {
			nextConsumerGroups = append(nextConsumerGroups, consumerGroup)
			continue
		}

		nextConsumerGroup := &ClusterConsumerGroup{}
		*nextConsumerGroup = *consumerGroup
		nextConsumerGroup.Bindings = nextBindings

		nextConsumerGroups = append(nextConsumerGroups, nextConsumerGroup)
		consumerGroupsChanged = true
	}

	if consumerGroupsChanged {
		nextNamespace.ConsumerGroups = nextConsumerGroups
	}

	if nextNamespace.isEmpty() {
		next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces)-1)
		copy(next.Namespaces[:namespaceIndex], s.Namespaces[:namespaceIndex])
		copy(next.Namespaces[namespaceIndex:], s.Namespaces[namespaceIndex+1:])
	}

	return next
}
