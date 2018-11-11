package mq

import (
	"io"
	"io/ioutil"
	"sort"
	"sync/atomic"
	"unsafe"

	"eventter.io/mq/client"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

type ClusterStateStore struct {
	statePtr unsafe.Pointer
}

var _ raft.FSM = (*ClusterStateStore)(nil)

func NewClusterStateStore() *ClusterStateStore {
	state := &ClusterState{}

	return &ClusterStateStore{
		statePtr: unsafe.Pointer(state),
	}
}

func (s *ClusterStateStore) String() string {
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	return proto.MarshalTextString(state)
}

func (s *ClusterStateStore) Apply(entry *raft.Log) interface{} {
	if entry.Type != raft.LogCommand {
		return nil
	}

	var cmd Command

	if err := proto.Unmarshal(entry.Data, &cmd); err != nil {
		panic(err)
	}

	var (
		state     = (*ClusterState)(atomic.LoadPointer(&s.statePtr))
		nextState *ClusterState
	)

	switch cmd := cmd.Command.(type) {
	case *Command_ConfigureTopic:
		nextState = s.doConfigureTopic(state, cmd.ConfigureTopic)
	case *Command_DeleteTopic:
		nextState = s.doDeleteTopic(state, cmd.DeleteTopic)
	case *Command_ConfigureConsumerGroup:
		nextState = s.doConfigureConsumerGroup(state, cmd.ConfigureConsumerGroup)
	case *Command_DeleteConsumerGroup:
		nextState = s.doDeleteConsumerGroup(state, cmd.DeleteConsumerGroup)
	case *Command_OpenSegment:
		nextState = s.doOpenSegment(state, cmd.OpenSegment)
	case *Command_CloseSegment:
		nextState = s.doCloseSegment(state, cmd.CloseSegment)
	default:
		panic(errors.Errorf("unhandled command of type [%T]", cmd))
	}

	if nextState == state {
		nextState := &ClusterState{}
		*nextState = *state
	}
	nextState.Index = entry.Index

	atomic.SwapPointer(&s.statePtr, unsafe.Pointer(nextState))

	return nil
}

func (s *ClusterStateStore) doConfigureTopic(state *ClusterState, cmd *client.ConfigureTopicRequest) *ClusterState {
	nextState := &ClusterState{}
	*nextState = *state

	namespace, namespaceIndex := state.findNamespace(cmd.Topic.Namespace)
	var (
		nextNamespace *ClusterNamespace
		nextTopic     *ClusterTopic
	)

	if namespace == nil {
		nextNamespace = &ClusterNamespace{
			Name: cmd.Topic.Namespace,
		}

		nextState.Namespaces = make([]*ClusterNamespace, len(state.Namespaces)+1)
		copy(nextState.Namespaces, state.Namespaces)
		nextState.Namespaces[len(state.Namespaces)] = nextNamespace

		nextTopic = &ClusterTopic{
			Name: cmd.Topic.Name,
		}
		nextNamespace.Topics = []*ClusterTopic{nextTopic}

	} else {
		nextNamespace = &ClusterNamespace{}
		*nextNamespace = *namespace

		nextState.Namespaces = make([]*ClusterNamespace, len(state.Namespaces))
		copy(nextState.Namespaces[:namespaceIndex], state.Namespaces[:namespaceIndex])
		nextState.Namespaces[namespaceIndex] = nextNamespace
		copy(nextState.Namespaces[namespaceIndex+1:], state.Namespaces[namespaceIndex+1:])

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

	return nextState
}

func (s *ClusterStateStore) doDeleteTopic(state *ClusterState, cmd *client.DeleteTopicRequest) *ClusterState {
	namespace, namespaceIndex := state.findNamespace(cmd.Topic.Namespace)
	if namespace == nil {
		return state
	}

	_, topicIndex := namespace.findTopic(cmd.Topic.Name)
	if topicIndex == -1 {
		return state
	}

	nextState := &ClusterState{}
	*nextState = *state

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
		nextState.Namespaces = make([]*ClusterNamespace, len(state.Namespaces)-1)
		copy(nextState.Namespaces[:namespaceIndex], state.Namespaces[:namespaceIndex])
		copy(nextState.Namespaces[namespaceIndex:], state.Namespaces[namespaceIndex+1:])
	}

	return nextState
}

func (s *ClusterStateStore) doConfigureConsumerGroup(state *ClusterState, cmd *client.ConfigureConsumerGroupRequest) *ClusterState {
	nextState := &ClusterState{}
	*nextState = *state

	namespace, namespaceIndex := state.findNamespace(cmd.ConsumerGroup.Namespace)
	var (
		nextNamespace     *ClusterNamespace
		nextConsumerGroup *ClusterConsumerGroup
	)

	if namespace == nil {
		nextNamespace = &ClusterNamespace{
			Name: cmd.ConsumerGroup.Namespace,
		}

		nextState.Namespaces = make([]*ClusterNamespace, len(state.Namespaces)+1)
		copy(nextState.Namespaces, state.Namespaces)
		nextState.Namespaces[len(state.Namespaces)] = nextNamespace

		nextConsumerGroup = &ClusterConsumerGroup{
			Name: cmd.ConsumerGroup.Name,
		}
		nextNamespace.ConsumerGroups = []*ClusterConsumerGroup{nextConsumerGroup}

	} else {
		nextNamespace = &ClusterNamespace{}
		*nextNamespace = *namespace

		nextState.Namespaces = make([]*ClusterNamespace, len(state.Namespaces))
		copy(nextState.Namespaces[:namespaceIndex], state.Namespaces[:namespaceIndex])
		nextState.Namespaces[namespaceIndex] = nextNamespace
		copy(nextState.Namespaces[namespaceIndex+1:], state.Namespaces[namespaceIndex+1:])

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
	nextConsumerGroup.Shards = cmd.Shards

	return nextState
}

func (s *ClusterStateStore) doDeleteConsumerGroup(state *ClusterState, cmd *client.DeleteConsumerGroupRequest) *ClusterState {
	namespace, namespaceIndex := state.findNamespace(cmd.ConsumerGroup.Namespace)
	if namespace == nil {
		return state
	}

	_, consumerGroupIndex := namespace.findConsumerGroup(cmd.ConsumerGroup.Name)
	if consumerGroupIndex == -1 {
		return state
	}

	copy(namespace.ConsumerGroups[consumerGroupIndex:], namespace.ConsumerGroups[consumerGroupIndex+1:])
	namespace.ConsumerGroups[len(namespace.ConsumerGroups)-1] = nil
	namespace.ConsumerGroups = namespace.ConsumerGroups[:len(namespace.ConsumerGroups)-1]

	nextState := &ClusterState{}
	*nextState = *state

	nextNamespace := &ClusterNamespace{}
	*nextNamespace = *namespace

	nextNamespace.ConsumerGroups = make([]*ClusterConsumerGroup, len(namespace.ConsumerGroups)-1)
	copy(nextNamespace.ConsumerGroups[:consumerGroupIndex], namespace.ConsumerGroups[:consumerGroupIndex])
	copy(nextNamespace.ConsumerGroups[consumerGroupIndex:], namespace.ConsumerGroups[consumerGroupIndex+1:])

	if nextNamespace.isEmpty() {
		nextState.Namespaces = make([]*ClusterNamespace, len(state.Namespaces)-1)
		copy(nextState.Namespaces[:namespaceIndex], state.Namespaces[:namespaceIndex])
		copy(nextState.Namespaces[namespaceIndex:], state.Namespaces[namespaceIndex+1:])
	}

	return nextState
}

func (s *ClusterStateStore) doOpenSegment(state *ClusterState, cmd *OpenSegmentCommand) *ClusterState {
	nextState := &ClusterState{}
	*nextState = *state

	if cmd.ID > nextState.CurrentSegmentID {
		nextState.CurrentSegmentID = cmd.ID
	}

	nextState.OpenSegments = make([]*ClusterSegment, len(state.OpenSegments)+1)
	copy(nextState.OpenSegments, state.OpenSegments)
	nextState.OpenSegments[len(state.OpenSegments)] = &ClusterSegment{
		ID:             cmd.ID,
		Topic:          cmd.Topic,
		FirstMessageID: cmd.FirstMessageID,
		Nodes: ClusterSegment_Nodes{
			PrimaryNodeID: cmd.PrimaryNodeID,
		},
	}

	sort.Slice(nextState.OpenSegments, func(i, j int) bool {
		return nextState.OpenSegments[i].ID < nextState.OpenSegments[j].ID
	})

	return nextState
}

func (s *ClusterStateStore) doCloseSegment(state *ClusterState, cmd *CloseSegmentCommand) *ClusterState {
	segmentIndex := -1
	for i, segment := range state.OpenSegments {
		if segment.ID == cmd.ID {
			segmentIndex = i
			break
		}
	}

	if segmentIndex == -1 {
		return state
	}

	nextSegment := &ClusterSegment{}
	*nextSegment = *state.OpenSegments[segmentIndex]

	nextState := &ClusterState{}
	*nextState = *state

	nextState.OpenSegments = make([]*ClusterSegment, len(state.OpenSegments)-1)
	copy(nextState.OpenSegments[:segmentIndex], state.OpenSegments[:segmentIndex])
	copy(nextState.OpenSegments[segmentIndex:], state.OpenSegments[segmentIndex+1:])

	nextState.ClosedSegments = make([]*ClusterSegment, len(state.ClosedSegments)+1)
	copy(nextState.ClosedSegments, state.ClosedSegments)
	nextState.ClosedSegments[len(state.ClosedSegments)] = nextSegment

	nextSegment.LastMessageID = cmd.LastMessageID
	nextSegment.Size_ = cmd.Size_
	nextSegment.Sha1 = cmd.Sha1
	nextSegment.Nodes.PrimaryNodeID = 0
	nextSegment.Nodes.DoneNodeIDs = []uint64{cmd.DoneNodeID}

	sort.Slice(nextState.ClosedSegments, func(i, j int) bool {
		return nextState.ClosedSegments[i].ID < nextState.ClosedSegments[j].ID
	})

	return nextState
}

func (s *ClusterStateStore) Snapshot() (raft.FSMSnapshot, error) {
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	buf, err := proto.Marshal(state)
	if err != nil {
		return nil, err
	}

	return clusterStateStoreSnapshot(buf), nil
}

func (s *ClusterStateStore) Restore(r io.ReadCloser) error {
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	state := &ClusterState{}

	if err := proto.Unmarshal(buf, state); err != nil {
		return err
	}

	if err := r.Close(); err != nil {
		return err
	}

	atomic.SwapPointer(&s.statePtr, unsafe.Pointer(state))

	return nil
}

func (s *ClusterStateStore) ListTopics(namespaceName string, topicName string) (uint64, []*ClusterTopic) {
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	namespace, _ := state.findNamespace(namespaceName)
	if namespace == nil {
		return state.Index, nil
	}

	if topicName == "" {
		return state.Index, namespace.Topics
	} else {
		topic, _ := namespace.findTopic(topicName)
		if topic == nil {
			return state.Index, nil
		} else {
			return state.Index, []*ClusterTopic{topic}
		}
	}
}

func (s *ClusterStateStore) ListConsumerGroups(namespaceName string, consumerGroupName string) (uint64, []*ClusterConsumerGroup) {
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	namespace, _ := state.findNamespace(namespaceName)
	if namespace == nil {
		return state.Index, nil
	}

	if consumerGroupName == "" {
		return state.Index, namespace.ConsumerGroups
	} else {
		consumerGroup, _ := namespace.findConsumerGroup(consumerGroupName)
		if consumerGroup == nil {
			return state.Index, nil
		} else {
			return state.Index, []*ClusterConsumerGroup{consumerGroup}
		}
	}
}

func (s *ClusterStateStore) AnyConsumerGroupReferencesTopic(namespaceName string, topicName string) bool {
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	namespace, _ := state.findNamespace(namespaceName)
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
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	namespace, _ := state.findNamespace(namespaceName)
	if namespace == nil {
		return false
	}

	topic, _ := namespace.findTopic(topicName)
	return topic != nil
}

func (s *ClusterStateStore) GetTopic(namespaceName string, topicName string) *ClusterTopic {
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	namespace, _ := state.findNamespace(namespaceName)
	if namespace == nil {
		return nil
	}

	topic, _ := namespace.findTopic(topicName)
	return topic
}

func (s *ClusterStateStore) ConsumerGroupExists(namespaceName string, consumerGroupName string) bool {
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	namespace, _ := state.findNamespace(namespaceName)
	if namespace == nil {
		return false
	}

	consumerGroup, _ := namespace.findConsumerGroup(consumerGroupName)
	return consumerGroup != nil
}

func (s *ClusterStateStore) FindOpenSegmentsFor(namespaceName string, topicName string) []*ClusterSegment {
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	var segments []*ClusterSegment

	for _, segment := range state.OpenSegments {
		if segment.Topic.Namespace == namespaceName && segment.Topic.Name == topicName {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (s *ClusterStateStore) FindOpenSegmentsIn(nodeID uint64) []*ClusterSegment {
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	var segments []*ClusterSegment

	for _, segment := range state.OpenSegments {
		if segment.Nodes.PrimaryNodeID == nodeID {
			segments = append(segments, segment)
		}
	}

	return segments
}

func (s *ClusterStateStore) NextSegmentID() uint64 {
	for {
		statePtr := atomic.LoadPointer(&s.statePtr)
		state := (*ClusterState)(statePtr)
		nextState := &ClusterState{}
		*nextState = *state
		nextState.CurrentSegmentID += 1
		nextStatePtr := unsafe.Pointer(nextState)

		if atomic.CompareAndSwapPointer(&s.statePtr, statePtr, nextStatePtr) {
			return nextState.CurrentSegmentID
		}
	}
}

func (s *ClusterStateStore) GetOpenSegment(id uint64) *ClusterSegment {
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	for _, segment := range state.OpenSegments {
		if segment.ID == id {
			return segment
		}
	}

	return nil
}

func (s *ClusterStateStore) GetClosedSegment(id uint64) *ClusterSegment {
	state := (*ClusterState)(atomic.LoadPointer(&s.statePtr))

	for _, segment := range state.ClosedSegments {
		if segment.ID == id {
			return segment
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

func (namespace *ClusterNamespace) isEmpty() bool {
	return len(namespace.Topics) == 0 && len(namespace.ConsumerGroups) == 0
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
