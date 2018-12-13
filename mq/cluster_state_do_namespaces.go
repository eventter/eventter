package mq

func (s *ClusterState) doCreateNamespace(cmd *ClusterCommandNamespaceCreate) *ClusterState {
	namespace, _ := s.FindNamespace(cmd.Namespace)
	if namespace != nil {
		return s
	}

	next := &ClusterState{}
	*next = *s

	nextNamespace := &ClusterNamespace{
		Name: cmd.Namespace,
	}

	next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces)+1)
	copy(next.Namespaces, s.Namespaces)
	next.Namespaces[len(s.Namespaces)] = nextNamespace

	return next
}

func (s *ClusterState) doDeleteNamespace(cmd *ClusterCommandNamespaceDelete) *ClusterState {
	_, namespaceIndex := s.FindNamespace(cmd.Namespace)
	if namespaceIndex == -1 {
		return s
	}

	next := &ClusterState{}
	*next = *s

	next.Namespaces = make([]*ClusterNamespace, len(s.Namespaces)-1)
	copy(next.Namespaces[:namespaceIndex], s.Namespaces[:namespaceIndex])
	copy(next.Namespaces[namespaceIndex:], s.Namespaces[namespaceIndex+1:])

	return next
}
