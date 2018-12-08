package mq

import (
	"context"

	"eventter.io/mq/client"
	"github.com/pkg/errors"
)

func (s *Server) Nack(ctx context.Context, request *client.NackRequest) (*client.NackResponse, error) {
	if request.NodeID != s.nodeID {
		if request.DoNotForward {
			return nil, errWontForward
		}

		state := s.clusterState.Current()
		node := state.GetNode(request.NodeID)

		conn, err := s.pool.Get(ctx, node.Address)
		if err != nil {
			return nil, errors.Wrap(err, "dial failed")
		}
		defer s.pool.Put(conn)

		request.DoNotForward = true
		return client.NewEventterMQClient(conn).Nack(ctx, request)
	}

	s.groupMutex.RLock()
	subscription, ok := s.subscriptionMap[request.SubscriptionID]
	s.groupMutex.RUnlock()

	if !ok {
		return nil, errors.Errorf("subscription %d not found", request.SubscriptionID)
	}

	if err := subscription.Nack(request.SeqNo); err != nil {
		return nil, errors.Wrap(err, "ack failed")
	}

	return &client.NackResponse{OK: true}, nil
}
