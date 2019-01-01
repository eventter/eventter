package mq

import (
	"io"

	"eventter.io/mq/consumers"
	"eventter.io/mq/emq"
	"github.com/pkg/errors"
)

func (s *Server) Subscribe(request *emq.ConsumerGroupSubscribeRequest, stream emq.EventterMQ_SubscribeServer) error {
	if err := request.Validate(); err != nil {
		return err
	}

	state := s.clusterState.Current()

	namespace, _ := state.FindNamespace(request.ConsumerGroup.Namespace)
	if namespace == nil {
		return errors.Errorf(namespaceNotFoundErrorFormat, request.ConsumerGroup.Namespace)
	}

	consumerGroup, _ := namespace.FindConsumerGroup(request.ConsumerGroup.Name)
	if consumerGroup == nil {
		return errors.Errorf(
			notFoundErrorFormat,
			entityConsumerGroup,
			request.ConsumerGroup.Namespace,
			request.ConsumerGroup.Name,
		)
	}

	offsetSegments := state.FindOpenSegmentsFor(
		ClusterSegment_CONSUMER_GROUP_OFFSET_COMMITS,
		request.ConsumerGroup.Namespace,
		request.ConsumerGroup.Name,
	)

	if len(offsetSegments) == 0 {
		return errors.New("consumer group not assigned to any node")
	} else if len(offsetSegments) > 1 {
		return errors.New("consumer group assigned to multiple nodes")
	}

	segment := offsetSegments[0]

	ctx := stream.Context()

	if segment.Nodes.PrimaryNodeID != s.nodeID {
		if request.DoNotForward {
			return errWontForward
		}

		node := state.GetNode(segment.Nodes.PrimaryNodeID)

		conn, err := s.pool.Get(ctx, node.Address)
		if err != nil {
			return errors.Wrap(err, "dial failed")
		}
		defer s.pool.Put(conn)

		request.DoNotForward = true
		c, err := emq.NewEventterMQClient(conn).Subscribe(ctx, request)
		if err != nil {
			return errors.Wrap(err, "request failed")
		}

		go func() {
			<-ctx.Done()
			c.CloseSend()
		}()

		for {
			response, err := c.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				return errors.Wrap(err, "receive failed")
			}

			if err := stream.Send(response); err != nil {
				return errors.Wrap(err, "send failed")
			}
		}

		return nil
	}

	mapKey := s.makeConsumerGroupMapKey(request.ConsumerGroup.Namespace, request.ConsumerGroup.Name)
	s.groupMutex.Lock()
	group, ok := s.groups[mapKey]
	s.groupMutex.Unlock()

	if !ok {
		return errors.Errorf(
			"consumer group %s/%s is not running",
			request.ConsumerGroup.Namespace,
			request.ConsumerGroup.Name,
		)
	}

	subscription := group.Subscribe()
	s.groupMutex.Lock()
	s.subscriptions[subscription.ID] = subscription
	s.groupMutex.Unlock()
	defer func() {
		subscription.Close()
		s.groupMutex.Lock()
		delete(s.subscriptions, subscription.ID)
		s.groupMutex.Unlock()
	}()

	if request.Size_ != 0 {
		subscription.SetSize(request.Size_)
	}
	if request.MaxMessages != 0 {
		subscription.SetMaxMessages(request.MaxMessages)
	}
	if request.DoNotBlock {
		subscription.SetBlocking(false)
	}

	go func() {
		<-ctx.Done()
		subscription.Close()
	}()

	for {
		message, err := subscription.Next()
		if err == consumers.ErrSubscriptionClosed || err == consumers.ErrEmpty {
			return nil
		} else if err != nil {
			return errors.Wrap(err, "next failed")
		}

		response := &emq.ConsumerGroupSubscribeResponse{
			Topic:   message.Topic,
			Message: message.Message,
		}
		if !request.AutoAck {
			response.NodeID = s.nodeID
			response.SubscriptionID = subscription.ID
			response.SeqNo = message.SeqNo
		}

		err = stream.Send(response)
		if err != nil {
			return errors.Wrap(err, "send failed")
		}

		if request.AutoAck {
			err = subscription.Ack(message.SeqNo)
			if err != nil {
				return errors.Wrap(err, "ack failed")
			}
		}
	}
}
