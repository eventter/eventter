package mq

import (
	"context"
	"log"
	"math/rand"
	"time"

	"eventter.io/mq/client"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) SegmentOpen(ctx context.Context, request *SegmentOpenRequest) (*SegmentOpenResponse, error) {
	if s.raftNode.State() != raft.Leader {
		if request.LeaderOnly {
			return nil, errNotALeader
		}
		leader := s.raftNode.Leader()
		if leader == "" {
			return nil, errNoLeaderElected
		}

		conn, err := s.pool.Get(ctx, string(leader))
		if err != nil {
			return nil, errors.Wrap(err, couldNotDialLeaderError)
		}
		defer s.pool.Put(conn)

		request.LeaderOnly = true
		return NewNodeRPCClient(conn).SegmentOpen(ctx, request)
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	return s.txSegmentOpen(s.clusterState.Current(), request.NodeID, request.Owner, request.Type)
}

func (s *Server) txSegmentOpen(state *ClusterState, primaryNodeID uint64, owner client.NamespaceName, segmentType ClusterSegment_Type) (*SegmentOpenResponse, error) {
	node := state.GetNode(primaryNodeID)
	if node == nil {
		return nil, errors.Errorf("node %d not found", primaryNodeID)
	}

	var (
		shards            uint32 = 1
		replicationFactor uint32 = defaultSegmentReplicationFactor
	)
	if segmentType == ClusterSegment_TOPIC {
		topic := state.GetTopic(owner.Namespace, owner.Name)
		if topic == nil {
			return nil, errors.Errorf(notFoundErrorFormat, entityTopic, owner.Namespace, owner.Name)
		}

		shards = topic.Shards
		replicationFactor = topic.ReplicationFactor

	} else if segmentType == ClusterSegment_CONSUMER_GROUP_OFFSETS {
		consumerGroup := state.GetConsumerGroup(owner.Namespace, owner.Name)
		if consumerGroup == nil {
			return nil, errors.Errorf(notFoundErrorFormat, entityConsumerGroup, owner.Namespace, owner.Name)
		}
	}

	openSegments := state.FindOpenSegmentsFor(segmentType, owner.Namespace, owner.Name)

	// return node's existing segment if it exists
	for _, segment := range openSegments {
		if segment.Nodes.PrimaryNodeID == primaryNodeID {
			return &SegmentOpenResponse{
				SegmentID:     segment.ID,
				PrimaryNodeID: primaryNodeID,
			}, nil
		}
	}

	// return random segment from another node if there would be more shards than configured
	if shards > 0 && uint32(len(openSegments)) >= shards {
		segment := openSegments[rand.Intn(len(openSegments))]
		return &SegmentOpenResponse{
			SegmentID:     segment.ID,
			PrimaryNodeID: segment.Nodes.PrimaryNodeID,
		}, nil
	}

	// open new segment

	// a) find nodes for replicas
	var replicatingNodeIDs []uint64
	if replicationFactor > 1 {
		nodeSegmentCounts := state.CountSegmentsPerNode()
		var candidateNodeIDs []uint64

		for _, node := range state.Nodes {
			if node.ID != primaryNodeID && node.State == ClusterNode_ALIVE {
				candidateNodeIDs = append(candidateNodeIDs, node.ID)
			}
		}

		for len(candidateNodeIDs) > 0 && uint32(len(replicatingNodeIDs)) < replicationFactor-1 {
			var candidateIndex = -1
			for i, candidateNodeID := range candidateNodeIDs {
				if candidateIndex == -1 || nodeSegmentCounts[candidateNodeID] < nodeSegmentCounts[candidateNodeIDs[candidateIndex]] {
					candidateIndex = i
				}
			}

			replicatingNodeIDs = append(replicatingNodeIDs, candidateNodeIDs[candidateIndex])
			copy(candidateNodeIDs[candidateIndex:], candidateNodeIDs[candidateIndex+1:])
			candidateNodeIDs = candidateNodeIDs[:len(candidateNodeIDs)-1]
		}
	}

	// b) create segment
	segmentID := s.clusterState.NextSegmentID()
	cmd := &ClusterOpenSegmentCommand{
		ID:                 segmentID,
		Owner:              owner,
		Type:               segmentType,
		OpenedAt:           time.Now(),
		PrimaryNodeID:      primaryNodeID,
		ReplicatingNodeIDs: replicatingNodeIDs,
	}
	_, err := s.Apply(cmd)
	if err != nil {
		return nil, errors.Wrap(err, "open segment failed")
	}

	// c) update consumer group offset commits if topic segment
	if segmentType == ClusterSegment_TOPIC {
		namespace, _ := state.FindNamespace(owner.Namespace)
		// namespace must not be nil as topic was found earlier and this method is called with tx mutex
		for _, consumerGroup := range namespace.ConsumerGroups {
			bound := false
			for _, binding := range consumerGroup.Bindings {
				if binding.TopicName == owner.Name {
					bound = true
					break
				}
			}
			if !bound {
				continue
			}

			nextCommits := make([]*ClusterConsumerGroup_OffsetCommit, len(consumerGroup.OffsetCommits)+1)
			copy(nextCommits, consumerGroup.OffsetCommits)
			nextCommits[len(consumerGroup.OffsetCommits)] = &ClusterConsumerGroup_OffsetCommit{
				SegmentID: segmentID,
				Offset:    0,
			}

			cmd := &ClusterUpdateOffsetCommitsCommand{
				ConsumerGroup: client.NamespaceName{
					Namespace: namespace.Name,
					Name:      consumerGroup.Name,
				},
				OffsetCommits: nextCommits,
			}
			if _, err := s.Apply(cmd); err != nil {
				log.Printf(
					"could not update offset commits of consumer group %s/%s for newly created segment %d: %v",
					namespace.Name,
					consumerGroup.Name,
					segmentID,
					err,
				)
			}
		}
	}

	return &SegmentOpenResponse{
		SegmentID:     segmentID,
		PrimaryNodeID: primaryNodeID,
	}, nil
}
