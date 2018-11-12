package mq

import (
	"context"
	"time"

	"eventter.io/mq/client"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) OpenSegment(ctx context.Context, request *OpenSegmentRequest) (*OpenSegmentResponse, error) {
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
		return NewNodeRPCClient(conn).OpenSegment(ctx, request)
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	return s.txOpenSegment(s.clusterState.Current(), request.NodeID, request.Topic)
}

func (s *Server) txOpenSegment(state *ClusterState, primaryNodeID uint64, topicName client.NamespaceName) (*OpenSegmentResponse, error) {
	node := state.GetNode(primaryNodeID)
	if node == nil {
		return nil, errors.Errorf("node %d not found", primaryNodeID)
	}

	topic := state.GetTopic(topicName.Namespace, topicName.Name)
	if topic == nil {
		return nil, errors.Errorf(notFoundErrorFormat, entityTopic, topicName.Namespace, topicName.Name)
	}

	openSegments := state.FindOpenSegmentsFor(topicName.Namespace, topicName.Name)

	// return node's existing segment if it exists
	for _, segment := range openSegments {
		if segment.Nodes.PrimaryNodeID == primaryNodeID {
			return &OpenSegmentResponse{
				SegmentID:     segment.ID,
				PrimaryNodeID: primaryNodeID,
			}, nil
		}
	}

	// return random segment from another node if there would be more shards than configured
	if topic.Shards > 0 && uint32(len(openSegments)) >= topic.Shards {
		segment := openSegments[s.rng.Intn(len(openSegments))]
		return &OpenSegmentResponse{
			SegmentID:     segment.ID,
			PrimaryNodeID: segment.Nodes.PrimaryNodeID,
		}, nil
	}

	// open new segment
	segmentID := s.clusterState.NextSegmentID()

	var replicatingNodeIDs []uint64
	if topic.ReplicationFactor > 1 {
		nodeSegmentCounts := state.CountSegmentsPerNode()
		var candidateNodeIDs []uint64

		for _, node := range state.Nodes {
			if node.ID != primaryNodeID && node.State == ClusterNode_ALIVE {
				candidateNodeIDs = append(candidateNodeIDs, node.ID)
			}
		}

		for len(candidateNodeIDs) > 0 && uint32(len(replicatingNodeIDs)) < topic.ReplicationFactor-1 {
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

	cmd := &OpenSegmentCommand{
		ID:                 segmentID,
		Topic:              topicName,
		OpenedAt:           time.Now(),
		PrimaryNodeID:      primaryNodeID,
		ReplicatingNodeIDs: replicatingNodeIDs,
	}
	_, err := s.Apply(cmd)
	if err != nil {
		return nil, err
	}

	return &OpenSegmentResponse{
		SegmentID:     segmentID,
		PrimaryNodeID: primaryNodeID,
	}, nil
}
