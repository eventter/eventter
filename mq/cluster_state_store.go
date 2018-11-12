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

	for {
		var (
			statePtr  = atomic.LoadPointer(&s.statePtr)
			state     = (*ClusterState)(statePtr)
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
		case *Command_UpdateNode:
			nextState = s.doUpdateNode(state, cmd.UpdateNode)
		case *Command_UpdateSegmentNodes:
			nextState = s.doUpdateSegmentNodes(state, cmd.UpdateSegmentNodes)
		default:
			panic(errors.Errorf("unhandled command of type [%T]", cmd))
		}

		if nextState == state {
			nextState := &ClusterState{}
			*nextState = *state
		}
		nextState.Index = entry.Index

		if atomic.CompareAndSwapPointer(&s.statePtr, statePtr, unsafe.Pointer(nextState)) {
			return nil
		}
	}
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
		ID:       cmd.ID,
		Topic:    cmd.Topic,
		OpenedAt: cmd.OpenedAt,
		Nodes: ClusterSegment_Nodes{
			PrimaryNodeID:      cmd.PrimaryNodeID,
			ReplicatingNodeIDs: cmd.ReplicatingNodeIDs,
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

	nextSegment.ClosedAt = cmd.ClosedAt
	nextSegment.Size_ = cmd.Size_
	nextSegment.Sha1 = cmd.Sha1
	nextSegment.Nodes.PrimaryNodeID = 0
	nextSegment.Nodes.DoneNodeIDs = []uint64{cmd.DoneNodeID}

	sort.Slice(nextState.ClosedSegments, func(i, j int) bool {
		return nextState.ClosedSegments[i].ID < nextState.ClosedSegments[j].ID
	})

	return nextState
}

func (s *ClusterStateStore) doUpdateNode(state *ClusterState, cmd *UpdateNodeCommand) *ClusterState {
	nodeIndex := -1
	for i, node := range state.Nodes {
		if node.ID == cmd.ID {
			nodeIndex = i
			break
		}
	}

	nextState := &ClusterState{}
	*nextState = *state

	nextNode := &ClusterNode{}

	if nodeIndex == -1 {
		nextState.Nodes = make([]*ClusterNode, len(state.Nodes)+1)
		copy(nextState.Nodes, state.Nodes)
		nextState.Nodes[len(state.Nodes)] = nextNode
	} else {
		*nextNode = *state.Nodes[nodeIndex]
		nextState.Nodes = make([]*ClusterNode, len(state.Nodes))
		copy(nextState.Nodes[:nodeIndex], state.Nodes[:nodeIndex])
		nextState.Nodes[nodeIndex] = nextNode
		copy(nextState.Nodes[nodeIndex+1:], state.Nodes[nodeIndex+1:])
	}

	nextNode.ID = cmd.ID
	nextNode.Address = cmd.Address
	nextNode.State = cmd.State
	nextNode.LastSeenAlive = cmd.LastSeenAlive

	sort.Slice(nextState.Nodes, func(i, j int) bool {
		return nextState.Nodes[i].ID < nextState.Nodes[j].ID
	})

	return nextState
}

func (s *ClusterStateStore) doUpdateSegmentNodes(state *ClusterState, cmd *UpdateSegmentNodesCommand) *ClusterState {
	if len(state.OpenSegments) > 0 {
		i := sort.Search(len(state.OpenSegments), func(i int) bool { return state.OpenSegments[i].ID >= cmd.ID })
		if state.OpenSegments[i].ID == cmd.ID {
			nextState := &ClusterState{}
			*nextState = *state
			nextState.OpenSegments = s.doUpdateSegmentNodesIn(state.OpenSegments, i, cmd)
			return nextState
		}
	}

	if len(state.ClosedSegments) > 0 {
		i := sort.Search(len(state.ClosedSegments), func(i int) bool { return state.ClosedSegments[i].ID >= cmd.ID })
		if state.ClosedSegments[i].ID == cmd.ID {
			nextState := &ClusterState{}
			*nextState = *state
			nextState.ClosedSegments = s.doUpdateSegmentNodesIn(state.ClosedSegments, i, cmd)
			return nextState
		}
	}

	return state
}

func (s *ClusterStateStore) doUpdateSegmentNodesIn(segments []*ClusterSegment, segmentIndex int, cmd *UpdateSegmentNodesCommand) []*ClusterSegment {
	nextSegments := make([]*ClusterSegment, len(segments))
	copy(nextSegments, segments)

	nextSegment := &ClusterSegment{}
	*nextSegment = *segments[segmentIndex]
	nextSegments[segmentIndex] = nextSegment

	nextSegment.Nodes = cmd.Nodes

	return nextSegments
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

func (s *ClusterStateStore) Current() *ClusterState {
	return (*ClusterState)(atomic.LoadPointer(&s.statePtr))
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
