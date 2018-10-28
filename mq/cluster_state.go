package mq

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
)

type ClusterState struct {
}

func NewClusterState() *ClusterState {
	return &ClusterState{}
}

func (s *ClusterState) Apply(entry *raft.Log) interface{} {
	fmt.Printf("%#v\n", entry)
	return nil
}

func (s *ClusterState) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (s *ClusterState) Restore(r io.ReadCloser) error {
	return nil
}
