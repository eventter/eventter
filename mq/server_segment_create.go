package mq

import (
	"context"
	"log"
	"math/rand"
	"time"

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

	return s.txSegmentOpen(s.clusterState.Current(), request.NodeID, request.Type, request.OwnerNamespace, request.OwnerName)
}

func (s *Server) txSegmentOpen(state *ClusterState, primaryNodeID uint64, segmentType ClusterSegment_Type, ownerNamespace string, ownerName string) (*SegmentOpenResponse, error) {
	node := state.GetNode(primaryNodeID)
	if node == nil {
		return nil, errors.Errorf("node %d not found", primaryNodeID)
	}

	var (
		shards            uint32 = 1
		replicationFactor uint32 = defaultReplicationFactor
	)
	if segmentType == ClusterSegment_TOPIC {
		topic := state.GetTopic(ownerNamespace, ownerName)
		if topic == nil {
			return nil, errors.Errorf(notFoundErrorFormat, entityTopic, ownerNamespace, ownerName)
		}

		shards = topic.Shards
		replicationFactor = topic.ReplicationFactor

	} else if segmentType == ClusterSegment_CONSUMER_GROUP_OFFSET_COMMITS {
		consumerGroup := state.GetConsumerGroup(ownerNamespace, ownerName)
		if consumerGroup == nil {
			return nil, errors.Errorf(notFoundErrorFormat, entityConsumerGroup, ownerNamespace, ownerName)
		}
	}

	openSegments := state.FindOpenSegmentsFor(segmentType, ownerNamespace, ownerName)

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

	// a) find latest generation
	generation := uint32(1)
	generationSegments := make([]*ClusterSegment, 0, shards)

	for _, segments := range [][]*ClusterSegment{state.ClosedSegments, state.OpenSegments} {
		for _, segment := range segments {
			if segment.Type == segmentType && segment.OwnerNamespace == ownerNamespace && segment.OwnerName == ownerName {
				if segment.Generation > generation {
					generation = segment.Generation
					generationSegments = generationSegments[:0]
				}
				if segment.Generation == generation {
					generationSegments = append(generationSegments, segment)
				}
			}
		}
	}

	// b) find first available shard in generation
	shard := uint32(1)
	if shards > 0 && uint32(len(generationSegments)) >= shards {
		// whole previous generation is assigned => start a new one
		generation++
	} else {
		nodeFound := false
		for _, segment := range generationSegments {
			if segment.Nodes.PrimaryNodeID == primaryNodeID {
				nodeFound = true
				break
			}
			for _, nodeID := range segment.Nodes.DoneNodeIDs {
				if nodeID == primaryNodeID {
					nodeFound = true
					break
				}
			}
		}
		if nodeFound {
			// each node must have at most one segment in each generation => start a new one
			generation++
		} else {
			for i := uint32(1); i <= shards; i++ {
				found := false
				for _, segment := range generationSegments {
					if segment.Shard == i {
						found = true
						break
					}
				}
				if !found {
					shard = i
					break
				}
			}
		}
	}

	// c) find nodes for replicas
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

	// d) create segment
	segmentID := s.clusterState.NextSegmentID()
	cmd := &ClusterCommandSegmentCreate{
		ID:                 segmentID,
		OwnerNamespace:     ownerNamespace,
		OwnerName:          ownerName,
		Type:               segmentType,
		Generation:         generation,
		Shard:              shard,
		OpenedAt:           time.Now(),
		PrimaryNodeID:      primaryNodeID,
		ReplicatingNodeIDs: replicatingNodeIDs,
	}
	_, err := s.Apply(cmd)
	if err != nil {
		return nil, errors.Wrap(err, "apply failed")
	}

	// e) update consumer group offset commits if topic segment
	if segmentType == ClusterSegment_TOPIC {
		namespace, _ := state.FindNamespace(ownerNamespace)
		// namespace must not be nil as topic was found earlier and this method is called with tx mutex
		for _, consumerGroup := range namespace.ConsumerGroups {
			bound := false
			for _, binding := range consumerGroup.Bindings {
				if binding.TopicName == ownerName {
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

			cmd := &ClusterCommandConsumerGroupOffsetCommitsUpdate{
				Namespace:     namespace.Name,
				Name:          consumerGroup.Name,
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
