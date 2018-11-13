package mq

import (
	"eventter.io/mq/client"
)

func (s *ClusterState) doConfigureConsumerGroup(cmd *client.ConfigureConsumerGroupRequest) *ClusterState {
	next := &ClusterState{}
	*next = *s

	namespace, namespaceIndex := s.findNamespace(cmd.ConsumerGroup.Namespace)
	var (
		nextNamespace     *ClusterNamespace
		nextConsumerGroup *ClusterConsumerGroup
	)

	if namespace == nil {
		nextNamespace = &ClusterNamespace{
			Name: cmd.ConsumerGroup.Namespace,
		}

		next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces)+1)
		copy(next.Namespaces, s.Namespaces)
		next.Namespaces[len(s.Namespaces)] = nextNamespace

		nextConsumerGroup = &ClusterConsumerGroup{
			Name: cmd.ConsumerGroup.Name,
		}
		nextNamespace.ConsumerGroups = []*ClusterConsumerGroup{nextConsumerGroup}

	} else {
		nextNamespace = &ClusterNamespace{}
		*nextNamespace = *namespace

		next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces))
		copy(next.Namespaces[:namespaceIndex], s.Namespaces[:namespaceIndex])
		next.Namespaces[namespaceIndex] = nextNamespace
		copy(next.Namespaces[namespaceIndex+1:], s.Namespaces[namespaceIndex+1:])

		consumerGroup, consumerGroupIndex := namespace.findConsumerGroup(cmd.ConsumerGroup.Name)
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
	}

	var bindings []*ClusterConsumerGroup_Binding
	for _, binding := range cmd.Bindings {
		bindings = append(bindings, &ClusterConsumerGroup_Binding{
			TopicName:  binding.TopicName,
			RoutingKey: binding.RoutingKey,
		})
	}

	nextConsumerGroup.Bindings = bindings

	return next
}

func (s *ClusterState) doDeleteConsumerGroup(cmd *client.DeleteConsumerGroupRequest) *ClusterState {
	namespace, namespaceIndex := s.findNamespace(cmd.ConsumerGroup.Namespace)
	if namespace == nil {
		return s
	}

	_, consumerGroupIndex := namespace.findConsumerGroup(cmd.ConsumerGroup.Name)
	if consumerGroupIndex == -1 {
		return s
	}

	copy(namespace.ConsumerGroups[consumerGroupIndex:], namespace.ConsumerGroups[consumerGroupIndex+1:])
	namespace.ConsumerGroups[len(namespace.ConsumerGroups)-1] = nil
	namespace.ConsumerGroups = namespace.ConsumerGroups[:len(namespace.ConsumerGroups)-1]

	next := &ClusterState{}
	*next = *s

	nextNamespace := &ClusterNamespace{}
	*nextNamespace = *namespace

	nextNamespace.ConsumerGroups = make([]*ClusterConsumerGroup, len(namespace.ConsumerGroups)-1)
	copy(nextNamespace.ConsumerGroups[:consumerGroupIndex], namespace.ConsumerGroups[:consumerGroupIndex])
	copy(nextNamespace.ConsumerGroups[consumerGroupIndex:], namespace.ConsumerGroups[consumerGroupIndex+1:])

	if nextNamespace.isEmpty() {
		next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces)-1)
		copy(next.Namespaces[:namespaceIndex], s.Namespaces[:namespaceIndex])
		copy(next.Namespaces[namespaceIndex:], s.Namespaces[namespaceIndex+1:])
	}

	return next
}
