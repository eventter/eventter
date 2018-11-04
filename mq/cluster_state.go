package mq

import (
	"io"
	"io/ioutil"
	"sort"
	"sync"

	"eventter.io/mq/client"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

type ClusterStateStore struct {
	mutex sync.RWMutex
	state ClusterState
}

var _ raft.FSM = (*ClusterStateStore)(nil)

func NewClusterStateStore() *ClusterStateStore {
	return &ClusterStateStore{}
}

func (s *ClusterStateStore) String() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return proto.MarshalTextString(&s.state)
}

func (s *ClusterStateStore) Apply(entry *raft.Log) interface{} {
	if entry.Type != raft.LogCommand {
		return nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	var cmd Command

	if err := proto.Unmarshal(entry.Data, &cmd); err != nil {
		panic(err)
	}

	switch cmd := cmd.Command.(type) {
	case *Command_ConfigureTopic:
		s.doConfigureTopic(cmd.ConfigureTopic)
	case *Command_DeleteTopic:
		s.doDeleteTopic(cmd.DeleteTopic)
	case *Command_ConfigureConsumerGroup:
		s.doConfigureConsumerGroup(cmd.ConfigureConsumerGroup)
	case *Command_DeleteConsumerGroup:
		s.doDeleteConsumerGroup(cmd.DeleteConsumerGroup)
	case *Command_OpenSegment:
		s.doOpenSegment(cmd.OpenSegment)
	case *Command_CloseSegment:
		s.doCloseSegment(cmd.CloseSegment)
	default:
		panic(errors.Errorf("unhandled command of type [%T]", cmd))
	}

	s.state.Index = entry.Index

	return nil
}

func (s *ClusterStateStore) doConfigureTopic(cmd *client.ConfigureTopicRequest) {
	namespace, _ := s.findNamespace(cmd.Topic.Namespace)
	if namespace == nil {
		namespace = &ClusterNamespace{
			Name: cmd.Topic.Namespace,
		}
		s.state.Namespaces = append(s.state.Namespaces, namespace)
	}

	topic, _ := namespace.findTopic(cmd.Topic.Name)
	if topic == nil {
		topic = &ClusterTopic{
			Name: cmd.Topic.Name,
		}
		namespace.Topics = append(namespace.Topics, topic)
	}

	topic.Type = cmd.Type
	topic.Shards = cmd.Shards
	topic.ReplicationFactor = cmd.ReplicationFactor
	topic.Retention = cmd.Retention
}

func (s *ClusterStateStore) doDeleteTopic(cmd *client.DeleteTopicRequest) {
	namespace, namespaceIndex := s.findNamespace(cmd.Topic.Namespace)
	if namespace == nil {
		return
	}

	_, topicIndex := namespace.findTopic(cmd.Topic.Name)
	if topicIndex == -1 {
		return
	}

	copy(namespace.Topics[topicIndex:], namespace.Topics[topicIndex+1:])
	namespace.Topics[len(namespace.Topics)-1] = nil
	namespace.Topics = namespace.Topics[:len(namespace.Topics)-1]

	for _, consumerGroup := range namespace.ConsumerGroups {
		var newBindings []*ClusterConsumerGroup_Binding
		for _, binding := range consumerGroup.Bindings {
			if binding.TopicName != cmd.Topic.Name {
				newBindings = append(newBindings, binding)
			}
		}
		consumerGroup.Bindings = newBindings
	}

	if isNamespaceEmpty(namespace) {
		s.deleteNamespace(namespaceIndex)
	}
}

func isNamespaceEmpty(namespace *ClusterNamespace) bool {
	return len(namespace.Topics) == 0 && len(namespace.ConsumerGroups) == 0
}

func (s *ClusterStateStore) deleteNamespace(namespaceIndex int) {
	copy(s.state.Namespaces[namespaceIndex:], s.state.Namespaces[namespaceIndex+1:])
	s.state.Namespaces[len(s.state.Namespaces)-1] = nil
	s.state.Namespaces = s.state.Namespaces[:len(s.state.Namespaces)-1]
}

func (s *ClusterStateStore) doConfigureConsumerGroup(cmd *client.ConfigureConsumerGroupRequest) {
	namespace, _ := s.findNamespace(cmd.ConsumerGroup.Namespace)
	if namespace == nil {
		namespace = &ClusterNamespace{
			Name: cmd.ConsumerGroup.Namespace,
		}
		s.state.Namespaces = append(s.state.Namespaces, namespace)
	}

	consumerGroup, _ := namespace.findConsumerGroup(cmd.ConsumerGroup.Name)
	if consumerGroup == nil {
		consumerGroup = &ClusterConsumerGroup{
			Name: cmd.ConsumerGroup.Name,
		}
		namespace.ConsumerGroups = append(namespace.ConsumerGroups, consumerGroup)
	}

	var bindings []*ClusterConsumerGroup_Binding
	for _, binding := range cmd.Bindings {
		bindings = append(bindings, &ClusterConsumerGroup_Binding{
			TopicName:  binding.TopicName,
			RoutingKey: binding.RoutingKey,
		})
	}

	consumerGroup.Bindings = bindings
	consumerGroup.Shards = cmd.Shards
}

func (s *ClusterStateStore) doDeleteConsumerGroup(cmd *client.DeleteConsumerGroupRequest) {
	namespace, namespaceIndex := s.findNamespace(cmd.ConsumerGroup.Namespace)
	if namespace == nil {
		return
	}

	_, consumerGroupIndex := namespace.findConsumerGroup(cmd.ConsumerGroup.Name)
	if consumerGroupIndex == -1 {
		return
	}

	copy(namespace.ConsumerGroups[consumerGroupIndex:], namespace.ConsumerGroups[consumerGroupIndex+1:])
	namespace.ConsumerGroups[len(namespace.ConsumerGroups)-1] = nil
	namespace.ConsumerGroups = namespace.ConsumerGroups[:len(namespace.ConsumerGroups)-1]

	if isNamespaceEmpty(namespace) {
		s.deleteNamespace(namespaceIndex)
	}
}

func (s *ClusterStateStore) doOpenSegment(cmd *OpenSegmentCommand) {
	if cmd.ID > s.state.CurrentSegmentID {
		s.state.CurrentSegmentID = cmd.ID
	}

	s.state.OpenSegments = append(s.state.OpenSegments, &ClusterSegment{
		ID:             cmd.ID,
		Topic:          cmd.Topic,
		FirstMessageID: cmd.FirstMessageID,
		Nodes: ClusterSegment_Nodes{
			PrimaryNodeID: cmd.PrimaryNodeID,
		},
	})
	sort.Slice(s.state.ClosedSegments, func(i, j int) bool {
		return s.state.ClosedSegments[i].ID < s.state.ClosedSegments[j].ID
	})
}

func (s *ClusterStateStore) doCloseSegment(cmd *CloseSegmentCommand) {
	segmentIndex := -1
	for i, segment := range s.state.OpenSegments {
		if segment.ID == cmd.ID {
			segmentIndex = i
			break
		}
	}

	if segmentIndex == -1 {
		return
	}

	segment := s.state.OpenSegments[segmentIndex]

	copy(s.state.OpenSegments[segmentIndex:], s.state.OpenSegments[segmentIndex+1:])
	s.state.OpenSegments[len(s.state.OpenSegments)-1] = nil
	s.state.OpenSegments = s.state.OpenSegments[:len(s.state.OpenSegments)-1]

	segment.LastMessageID = cmd.LastMessageID
	segment.Size_ = cmd.Size_
	segment.Sha1 = cmd.Sha1

	segment.Nodes.PrimaryNodeID = 0
	segment.Nodes.DoneNodeIDs = []uint64{cmd.DoneNodeID}

	s.state.ClosedSegments = append(s.state.ClosedSegments, segment)
	sort.Slice(s.state.ClosedSegments, func(i, j int) bool {
		return s.state.ClosedSegments[i].ID < s.state.ClosedSegments[j].ID
	})
}

func (s *ClusterStateStore) Snapshot() (raft.FSMSnapshot, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	buf, err := proto.Marshal(&s.state)
	if err != nil {
		return nil, err
	}

	return clusterStateStoreSnapshot(buf), nil
}

func (s *ClusterStateStore) Restore(r io.ReadCloser) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	if err := proto.Unmarshal(buf, &s.state); err != nil {
		return err
	}

	return r.Close()
}

func (s *ClusterStateStore) ListTopics(namespaceName string, topicName string) (uint64, []*ClusterTopic) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return s.state.Index, nil
	}

	if topicName == "" {
		return s.state.Index, namespace.Topics
	} else {
		topic, _ := namespace.findTopic(topicName)
		if topic == nil {
			return s.state.Index, nil
		} else {
			return s.state.Index, []*ClusterTopic{topic}
		}
	}
}

func (s *ClusterStateStore) ListConsumerGroups(namespaceName string, consumerGroupName string) (uint64, []*ClusterConsumerGroup) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return s.state.Index, nil
	}

	if consumerGroupName == "" {
		return s.state.Index, namespace.ConsumerGroups
	} else {
		consumerGroup, _ := namespace.findConsumerGroup(consumerGroupName)
		if consumerGroup == nil {
			return s.state.Index, nil
		} else {
			return s.state.Index, []*ClusterConsumerGroup{consumerGroup}
		}
	}
}

func (s *ClusterStateStore) AnyConsumerGroupReferencesTopic(namespaceName string, topicName string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

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

func (s *ClusterStateStore) TopicExists(namespaceName string, topicName string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return false
	}

	topic, _ := namespace.findTopic(topicName)
	return topic != nil
}

func (s *ClusterStateStore) GetTopic(namespaceName string, topicName string) *ClusterTopic {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return nil
	}

	topic, _ := namespace.findTopic(topicName)
	return topic
}

func (s *ClusterStateStore) ConsumerGroupExists(namespaceName string, consumerGroupName string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	namespace, _ := s.findNamespace(namespaceName)
	if namespace == nil {
		return false
	}

	consumerGroup, _ := namespace.findConsumerGroup(consumerGroupName)
	return consumerGroup != nil
}

func (s *ClusterStateStore) FindOpenSegmentsFor(namespaceName string, topicName string) []*ClusterSegment {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var segments []*ClusterSegment

	for _, segment := range s.state.OpenSegments {
		if segment.Topic.Namespace == namespaceName && segment.Topic.Name == topicName {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (s *ClusterStateStore) FindOpenSegmentsIn(nodeID uint64) []*ClusterSegment {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var segments []*ClusterSegment

	for _, segment := range s.state.OpenSegments {
		if segment.Nodes.PrimaryNodeID == nodeID {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (s *ClusterStateStore) NextSegmentID() uint64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.state.CurrentSegmentID += 1

	return s.state.CurrentSegmentID
}

func (s *ClusterStateStore) GetOpenSegment(id uint64) *ClusterSegment {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for _, segment := range s.state.OpenSegments {
		if segment.ID == id {
			return segment
		}
	}

	return nil
}

func (s *ClusterStateStore) GetClosedSegment(id uint64) *ClusterSegment {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for _, segment := range s.state.ClosedSegments {
		if segment.ID == id {
			return segment
		}
	}

	return nil
}

func (s *ClusterStateStore) findNamespace(name string) (*ClusterNamespace, int) {
	for index, namespace := range s.state.Namespaces {
		if namespace.Name == name {
			return namespace, index
		}
	}
	return nil, -1
}

func (namespace *ClusterNamespace) findTopic(name string) (*ClusterTopic, int) {
	for index, topic := range namespace.Topics {
		if topic.Name == name {
			return topic, index
		}
	}
	return nil, -1
}

func (namespace *ClusterNamespace) findConsumerGroup(name string) (*ClusterConsumerGroup, int) {
	for index, consumerGroup := range namespace.ConsumerGroups {
		if consumerGroup.Name == name {
			return consumerGroup, index
		}
	}
	return nil, -1
}

type clusterStateStoreSnapshot []byte

func (snapshot clusterStateStoreSnapshot) Persist(sink raft.SnapshotSink) (err error) {
	defer func() {
		if err != nil {
			sink.Cancel()
		}
	}()
	if _, err := sink.Write(snapshot); err != nil {
		return err
	}
	return sink.Close()
}

func (snapshot clusterStateStoreSnapshot) Release() {
	// no-op
}

var _ raft.FSMSnapshot = (clusterStateStoreSnapshot)(nil)
