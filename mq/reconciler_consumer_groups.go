package mq

import (
	"log"
	"math"
	"time"

	"eventter.io/mq/client"
)

func (r *Reconciler) ReconcileConsumerGroups(state *ClusterState) {
	nodeSegmentCounts := state.CountSegmentsPerNode()

	for _, namespace := range state.Namespaces {
		for _, consumerGroup := range namespace.ConsumerGroups {
			openSegments := state.FindOpenSegmentsFor(
				ClusterSegment_CONSUMER_GROUP_OFFSETS,
				namespace.Name,
				consumerGroup.Name,
			)

			if len(openSegments) == 1 {
				continue
			} else if len(openSegments) > 1 {
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
				continue
			}

			_, err := r.delegate.Apply(&ClusterOpenSegmentCommand{
				ID: r.delegate.NextSegmentID(),
				Owner: client.NamespaceName{
					Namespace: namespace.Name,
					Name:      consumerGroup.Name,
				},
				Type:               ClusterSegment_CONSUMER_GROUP_OFFSETS,
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
				continue
			}

			nodeSegmentCounts[primaryNodeID]++
		}
	}
}
