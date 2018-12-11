package mq

import (
	"context"
	"io"
	"time"

	"eventter.io/mq/client"
	"eventter.io/mq/consumers"
	"eventter.io/mq/segments"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func (s *Server) taskConsumeSegmentLocal(ctx context.Context, state *ClusterState, namespaceName string, consumerGroup *ClusterConsumerGroup, topic *ClusterTopic, segment *ClusterSegment, group *consumers.Group, startOffset int64) error {
	segmentHandle, err := s.segmentDir.Open(segment.ID)
	if err != nil {
		return errors.Wrap(err, "segment open failed")
	}
	defer s.segmentDir.Release(segmentHandle)

	var iterator *segments.Iterator
	if startOffset > 0 {
		iterator, err = segmentHandle.ReadAt(startOffset, segment.ClosedAt.IsZero())
	} else {
		iterator, err = segmentHandle.Read(segment.ClosedAt.IsZero())
	}
	if err != nil {
		return errors.Wrap(err, "segment read failed")
	}

	go func() {
		<-ctx.Done()
		iterator.Close()
	}()

	for {
		data, _, commitOffset, err := iterator.Next()
		if err == io.EOF {
			return nil
		} else if err == segments.ErrIteratorClosed && ctx.Err() != nil {
			return ctx.Err()
		} else if err != nil {
			return errors.Wrap(err, "iterator next failed")
		}

		publishing := Publishing{}
		if err := proto.Unmarshal(data, &publishing); err != nil {
			return errors.Wrap(err, "unmarshal failed")
		}

		if newState := s.clusterState.Current(); newState != state {
			state = newState
			consumerGroupName := consumerGroup.Name
			consumerGroup = state.GetConsumerGroup(namespaceName, consumerGroupName)
			if consumerGroup == nil {
				return errors.Errorf(notFoundErrorFormat, entityConsumerGroup, namespaceName, consumerGroupName)
			}
			topicName := topic.Name
			topic = state.GetTopic(namespaceName, topicName)
			if topic == nil {
				return errors.Errorf(notFoundErrorFormat, entityTopic, namespaceName, topicName)
			}
		}

		if isMessageEligibleForConsumerGroup(publishing.Message, topic, consumerGroup) {
			err = group.Offer(&consumers.Message{
				Topic:        segment.Owner,
				SegmentID:    segment.ID,
				CommitOffset: commitOffset,
				Time:         segment.OpenedAt.Add(time.Duration(publishing.Delta)),
				Message:      publishing.Message,
			})
			if err != nil {
				return errors.Wrap(err, "offer failed")
			}
		}
	}
}

func isMessageEligibleForConsumerGroup(message *client.Message, topic *ClusterTopic, consumerGroup *ClusterConsumerGroup) bool {
	switch topic.Type {
	case client.TopicType_DIRECT:
		for _, binding := range consumerGroup.Bindings {
			if binding.TopicName == topic.Name && binding.RoutingKey == message.RoutingKey {
				return true
			}
		}
		return false

	case client.TopicType_FANOUT:
		return true

	case client.TopicType_TOPIC:
		panic("implement me")

	case client.TopicType_HEADERS:
		panic("implement me")

	default:
		panic("unhandled topic type: " + topic.Type)
	}
}
