package mq

import (
	"context"

	"github.com/pkg/errors"
)

func (s *Server) SubscriptionResize(ctx context.Context, request *SubscriptionResizeRequest) (*SubscriptionResizeResponse, error) {
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
		return NewNodeRPCClient(conn).SubscriptionResize(ctx, request)
	}

	s.groupMutex.RLock()
	subscription, ok := s.subscriptions[request.SubscriptionID]
	s.groupMutex.RUnlock()

	if !ok {
		return nil, errors.Errorf("subscription %d not found", request.SubscriptionID)
	}

	subscription.SetSize(request.Size_)

	return nil, nil
}
