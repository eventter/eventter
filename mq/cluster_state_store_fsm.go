package mq

import (
	"io"
	"io/ioutil"
	"sync/atomic"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
)

var _ raft.FSM = (*ClusterStateStore)(nil)

func (s *ClusterStateStore) Apply(entry *raft.Log) interface{} {
	var cmd *Command = nil
	if entry.Type == raft.LogCommand {
		cmd = &Command{}
		if err := proto.Unmarshal(entry.Data, cmd); err != nil {
			panic(err)
		}
	}

	s.Do(entry.Index, cmd)

	return nil
}

func (s *ClusterStateStore) Snapshot() (raft.FSMSnapshot, error) {
	state := (*ClusterState)(atomic.LoadPointer(&s.ptr))

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

	atomic.SwapPointer(&s.ptr, unsafe.Pointer(state))

	return nil
}

type clusterStateStoreSnapshot []byte

var _ raft.FSMSnapshot = (clusterStateStoreSnapshot)(nil)

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
