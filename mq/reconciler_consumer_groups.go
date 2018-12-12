package mq

import (
	"log"
	"math"
	"time"

	"eventter.io/mq/client"
	"github.com/gogo/protobuf/proto"
)

func (r *Reconciler) ReconcileConsumerGroups(state *ClusterState) {
	nodeSegmentCounts := state.CountSegmentsPerNode()

	for _, namespace := range state.Namespaces {
		for _, consumerGroup := range namespace.ConsumerGroups {
			r.reconcileConsumerGroupOffsetCommitsSegment(state, namespace, consumerGroup, nodeSegmentCounts)
			r.reconcileConsumerGroupOffsetCommits(state, namespace, consumerGroup)
		}
	}
}

func (r *Reconciler) reconcileConsumerGroupOffsetCommitsSegment(state *ClusterState, namespace *ClusterNamespace, consumerGroup *ClusterConsumerGroup, nodeSegmentCounts map[uint64]int) {
	openSegments := state.FindOpenSegmentsFor(
		ClusterSegment_CONSUMER_GROUP_OFFSET_COMMITS,
		namespace.Name,
		consumerGroup.Name,
	)

	if len(openSegments) == 1 {
		return
	}

	if len(openSegments) > 1 {
		panic("there must be at most one open segment per consumer group")
	}

	var (
		primaryNodeID       uint64
		primarySegmentCount = math.MaxInt32
	)
	for _, node := range state.Nodes {
		if segmentCount := nodeSegmentCounts[node.ID]; node.State == ClusterNode_ALIVE && segmentCount < primarySegmentCount {
			primaryNodeID = node.ID
			primarySegmentCount = segmentCount
		}
	}

	if primaryNodeID == 0 {
		log.Printf(
			"could not select primary for consumer group %s/%s offsets segment",
			namespace.Name,
			consumerGroup.Name,
		)
		return
	}

	_, err := r.delegate.Apply(&ClusterCommandSegmentOpen{
		ID: r.delegate.NextSegmentID(),
		Owner: client.NamespaceName{
			Namespace: namespace.Name,
			Name:      consumerGroup.Name,
		},
		Type:               ClusterSegment_CONSUMER_GROUP_OFFSET_COMMITS,
		OpenedAt:           time.Now(),
		PrimaryNodeID:      primaryNodeID,
		ReplicatingNodeIDs: nil,
	})
	if err != nil {
		log.Printf(
			"could not open consumer group %s/%s offsets segment",
			namespace.Name,
			consumerGroup.Name,
		)
		return
	}

	nodeSegmentCounts[primaryNodeID]++
}

func (r *Reconciler) reconcileConsumerGroupOffsetCommits(state *ClusterState, namespace *ClusterNamespace, consumerGroup *ClusterConsumerGroup) {
	boundTopicNames := make(map[string]bool)
	for _, binding := range consumerGroup.Bindings {
		boundTopicNames[binding.TopicName] = true
	}

	m := make(map[uint64]bool)
	for _, commit := range consumerGroup.OffsetCommits {
		m[commit.SegmentID] = true
	}

	n := make(map[uint64]bool)

	var newOffsetCommits []*ClusterConsumerGroup_OffsetCommit

	for _, segments := range [][]*ClusterSegment{state.OpenSegments, state.ClosedSegments} {
		for _, segment := range segments {
			if segment.Type != ClusterSegment_TOPIC {
				continue
			}
			if segment.Owner.Namespace != namespace.Name {
				continue
			}
			if !boundTopicNames[segment.Owner.Name] {
				continue
			}
			if _, ok := m[segment.ID]; !ok {
				newOffsetCommits = append(newOffsetCommits, &ClusterConsumerGroup_OffsetCommit{
					SegmentID: segment.ID,
					Offset:    0,
				})
			}

			n[segment.ID] = true
		}
	}

	if newOffsetCommits == nil && len(m) == len(n) {
		return
	}

	commits := make([]*ClusterConsumerGroup_OffsetCommit, 0, len(consumerGroup.OffsetCommits)+len(newOffsetCommits))
	for _, commit := range consumerGroup.OffsetCommits {
		if n[commit.SegmentID] {
			commits = append(commits, commit)
		}
	}
	for _, commit := range newOffsetCommits {
		commits = append(commits, commit)
	}

	cmd := &ClusterCommandConsumerGroupOffsetCommitsUpdate{
		Namespace:     namespace.Name,
		Name:          consumerGroup.Name,
		OffsetCommits: commits,
	}

	_, err := r.delegate.Apply(cmd)
	if err != nil {
		log.Printf(
			"could not update consumer group %s/%s offset commits: %v",
			namespace.Name,
			consumerGroup.Name,
			err,
		)
	} else {
		log.Printf(
			"updated consumer group %s/%s offset commits: %s",
			namespace.Name,
			consumerGroup.Name,
			proto.MarshalTextString(cmd),
		)
	}
}
