package mq

import (
	"io"
	"io/ioutil"
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
	default:
		panic(errors.Errorf("unhandled command of type [%T]", cmd))
	}

	s.state.Index = entry.Index

	return nil
}

func (s *ClusterStateStore) doConfigureTopic(request *client.ConfigureTopicRequest) {
	namespace := s.findNamespace(request.Topic.Namespace)
	if namespace == nil {
		namespace = &ClusterNamespace{
			Name: request.Topic.Namespace,
		}
		s.state.Namespaces = append(s.state.Namespaces, namespace)
	}

	topic := namespace.findTopic(request.Topic.Name)
	if topic == nil {
		topic = &ClusterTopic{
			Name: request.Topic.Name,
		}
		namespace.Topics = append(namespace.Topics, topic)
	}

	topic.Type = request.Type
	topic.Shards = request.Shards
	topic.Retention = request.Retention
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

	namespace := s.findNamespace(namespaceName)
	if namespace == nil {
		return s.state.Index, nil
	}

	if topicName == "" {
		return s.state.Index, namespace.Topics
	} else {
		topic := namespace.findTopic(topicName)
		if topic == nil {
			return s.state.Index, nil
		} else {
			return s.state.Index, []*ClusterTopic{topic}
		}
	}
}

func (s *ClusterStateStore) findNamespace(name string) *ClusterNamespace {
	for _, namespace := range s.state.Namespaces {
		if namespace.Name == name {
			return namespace
		}
	}
	return nil
}

func (namespace *ClusterNamespace) findTopic(name string) *ClusterTopic {
	for _, topic := range namespace.Topics {
		if topic.Name == name {
			return topic
		}
	}
	return nil
}

func (namespace *ClusterNamespace) findConsumerGroup(name string) *ClusterConsumerGroup {
	for _, consumerGroup := range namespace.ConsumerGroups {
		if consumerGroup.Name == name {
			return consumerGroup
		}
	}
	return nil
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
