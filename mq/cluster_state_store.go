package mq

import (
	"sync/atomic"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

type ClusterStateStore struct {
	ptr unsafe.Pointer
}

func NewClusterStateStore() *ClusterStateStore {
	return &ClusterStateStore{
		ptr: unsafe.Pointer(&ClusterState{}),
	}
}

func (s *ClusterStateStore) Current() *ClusterState {
	return (*ClusterState)(atomic.LoadPointer(&s.ptr))
}

func (s *ClusterStateStore) NextSegmentID() uint64 {
	next := &ClusterState{}

	for {
		state := (*ClusterState)(atomic.LoadPointer(&s.ptr))
		*next = *state
		next.CurrentSegmentID += 1

		if atomic.CompareAndSwapPointer(&s.ptr, unsafe.Pointer(state), unsafe.Pointer(next)) {
			return next.CurrentSegmentID
		}
	}
}

func (s *ClusterStateStore) String() string {
	state := (*ClusterState)(atomic.LoadPointer(&s.ptr))
	return proto.MarshalTextString(state)
}

func (s *ClusterStateStore) Do(index uint64, cmd *Command) {
	for {
		state := (*ClusterState)(atomic.LoadPointer(&s.ptr))

		var next *ClusterState
		if cmd == nil {
			next = state
		} else {
			switch cmd := cmd.Command.(type) {
			case *Command_ConfigureTopic:
				next = state.doConfigureTopic(cmd.ConfigureTopic)
			case *Command_DeleteTopic:
				next = state.doDeleteTopic(cmd.DeleteTopic)
			case *Command_ConfigureConsumerGroup:
				next = state.doConfigureConsumerGroup(cmd.ConfigureConsumerGroup)
			case *Command_DeleteConsumerGroup:
				next = state.doDeleteConsumerGroup(cmd.DeleteConsumerGroup)
			case *Command_OpenSegment:
				next = state.doOpenSegment(cmd.OpenSegment)
			case *Command_CloseSegment:
				next = state.doCloseSegment(cmd.CloseSegment)
			case *Command_UpdateNode:
				next = state.doUpdateNode(cmd.UpdateNode)
			case *Command_UpdateSegmentNodes:
				next = state.doUpdateSegmentNodes(cmd.UpdateSegmentNodes)
			case *Command_DeleteSegment:
				next = state.doDeleteSegment(cmd.DeleteSegment)
			default:
				panic(errors.Errorf("unhandled command of type [%T]", cmd))
			}
		}

		if next == state {
			next = &ClusterState{}
			*next = *state
		}
		next.Index = index

		if atomic.CompareAndSwapPointer(&s.ptr, unsafe.Pointer(state), unsafe.Pointer(next)) {
			return
		}
	}
}
