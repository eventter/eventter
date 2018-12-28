package mq

import (
	"context"

	"eventter.io/mq/emq"
	"github.com/pkg/errors"
)

func (s *Server) Nack(ctx context.Context, request *emq.NackRequest) (*emq.NackResponse, error) {
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
		return emq.NewEventterMQClient(conn).Nack(ctx, request)
	}

	s.groupMutex.RLock()
	subscription, ok := s.subscriptions[request.SubscriptionID]
	s.groupMutex.RUnlock()

	if !ok {
		return nil, errors.Errorf("subscription %d not found", request.SubscriptionID)
	}

	if err := subscription.Nack(request.SeqNo); err != nil {
		return nil, errors.Wrap(err, "nack failed")
	}

	return &emq.NackResponse{OK: true}, nil
}
