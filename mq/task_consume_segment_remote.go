package mq

import (
	"context"
	"io"
	"math"
	"time"

	"eventter.io/mq/consumers"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func (s *Server) taskConsumeSegmentRemote(ctx context.Context, group *consumers.Group, state *ClusterState, segment *ClusterSegment, nodeID uint64, startOffset int64) error {
	node := state.GetNode(nodeID)
	if node == nil {
		return errors.Errorf("node %d not found", nodeID)
	}

	cc, err := s.pool.Get(ctx, node.Address)
	if err != nil {
		return errors.Wrap(err, "dial failed")
	}
	defer s.pool.Put(cc)

	client := NewNodeRPCClient(cc)

	topic := state.GetTopic(segment.Owner.Namespace, segment.Owner.Name)
	if topic == nil {
		return errors.Errorf(notFoundErrorFormat, entityTopic, segment.Owner.Namespace, segment.Owner.Name)
	}

	stream, err := client.SegmentRead(ctx, &SegmentReadRequest{
		SegmentID: segment.ID,
		Offset:    startOffset,
		Wait:      segment.ClosedAt.IsZero(),
	}, grpc.MaxCallRecvMsgSize(math.MaxUint32))
	if err != nil {
		return errors.Wrap(err, "segment read failed")
	}

	go func() {
		<-ctx.Done()
		stream.CloseSend()
	}()

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return errors.Wrap(err, "receive failed")
		}

		publishing := Publishing{}
		if err := proto.Unmarshal(response.Data, &publishing); err != nil {
			return errors.Wrap(err, "unmarshal failed")
		}

		// TODO: filter according to topic type

		err = group.Offer(&consumers.Message{
			Topic:        segment.Owner,
			SegmentID:    segment.ID,
			CommitOffset: response.CommitOffset,
			Time:         segment.OpenedAt.Add(time.Duration(publishing.Delta)),
			Message:      publishing.Message,
		})
		if err != nil {
			return errors.Wrap(err, "offer failed")
		}
	}
}
