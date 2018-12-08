package mq

import (
	"context"
	"math"

	"eventter.io/mq/client"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func (s *Server) ConfigureConsumerGroup(ctx context.Context, request *client.ConfigureConsumerGroupRequest) (*client.ConfigureConsumerGroupResponse, error) {
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
		return client.NewEventterMQClient(conn).ConfigureConsumerGroup(ctx, request)
	}

	if err := request.Validate(); err != nil {
		return nil, err
	}

	if err := s.beginTransaction(); err != nil {
		return nil, err
	}
	defer s.releaseTransaction()

	state := s.clusterState.Current()

	for _, binding := range request.Bindings {
		if !state.TopicExists(request.ConsumerGroup.Namespace, binding.TopicName) {
			return nil, errors.Errorf(notFoundErrorFormat, entityTopic, request.ConsumerGroup.Namespace, binding.TopicName)
		}
	}

	index, err := s.Apply(request)
	if err != nil {
		return nil, err
	}

	if err := s.raftNode.Barrier(barrierTimeout).Error(); err != nil {
		return nil, err
	}

	openSegments := state.FindOpenSegmentsFor(
		ClusterSegment_CONSUMER_GROUP_OFFSETS,
		request.ConsumerGroup.Namespace,
		request.ConsumerGroup.Name,
	)

	if len(openSegments) == 0 {
		nodeSegmentCounts := state.CountSegmentsPerNode()
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

		if primaryNodeID > 0 {
			_, err = s.txSegmentOpen(state, primaryNodeID, request.ConsumerGroup, ClusterSegment_CONSUMER_GROUP_OFFSETS)
			if err != nil {
				return nil, errors.Wrap(err, "segment open failed")
			}
		}

	} else if len(openSegments) > 1 {
		panic("there must be at most one open segment per consumer group")
	}

	return &client.ConfigureConsumerGroupResponse{
		OK:    true,
		Index: index,
	}, nil
}
