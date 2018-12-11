package mq

import (
	"bytes"
	"context"
	"crypto/sha1"
	"io"
	"log"
	"math"
	"runtime"

	"eventter.io/mq/segments"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func (s *Server) taskSegmentReplication(ctx context.Context, segmentID uint64, nodeID uint64, wait bool) error {
	log.Printf("starting to replicate segment %d from node %d", segmentID, nodeID)

	node := s.clusterState.Current().GetNode(nodeID)
	if node == nil {
		return errors.New("node not found")
	}

	segmentHandle, err := s.segmentDir.Open(segmentID)
	if err != nil {
		return errors.Wrap(err, "open failed")
	}
	defer s.segmentDir.Release(segmentHandle)

	cc, err := s.pool.Get(ctx, node.Address)
	if err != nil {
		return errors.Wrap(err, "dial failed")
	}
	defer s.pool.Put(cc)

	sha1Sum, size, err := segmentHandle.Sum(sha1.New(), segments.SumAll)
	if err != nil {
		return errors.Wrap(err, "local sum failed")
	}

	client := NewNodeRPCClient(cc)

	sumResponse, err := client.SegmentSum(ctx, &SegmentSumRequest{
		SegmentID: segmentID,
		Size_:     size,
	})
	if err != nil {
		return errors.Wrap(err, "remote sum failed")
	}

	if !bytes.Equal(sha1Sum, sumResponse.Sha1) {
		size = segments.TruncateAll
	}

	if err := segmentHandle.Truncate(size); err != nil {
		return errors.Wrap(err, "truncate failed")
	}

	stream, err := client.SegmentRead(ctx, &SegmentReadRequest{
		SegmentID: segmentID,
		Offset:    size,
		Wait:      wait,
	}, grpc.MaxCallRecvMsgSize(math.MaxUint32))
	if err != nil {
		return errors.Wrap(err, "request failed")
	}

	go func() {
		<-ctx.Done()
		stream.CloseSend()
	}()

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "receive failed")
		}

		if err := segmentHandle.Write(response.Data); err != nil {
			return errors.Wrap(err, "write failed")
		}
	}

	log.Printf("completed replication of segment %d from node %d", segmentID, nodeID)

	if wait {
		// primary closed the segment => busy wait for state to replicate

		state := s.clusterState.Current()
		closedSegment := state.GetClosedSegment(segmentID)

		for closedSegment == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				runtime.Gosched()
			}
			state = s.clusterState.Current()
			closedSegment = state.GetClosedSegment(segmentID)
		}
	}

	finalSha1Sum, finalSize, err := segmentHandle.Sum(sha1.New(), segments.SumAll)
	if err != nil {
		return errors.Wrap(err, "final sum failed")
	}

	_, err = s.SegmentReplicaClose(ctx, &SegmentCloseRequest{
		NodeID:    s.nodeID,
		SegmentID: segmentID,
		Size_:     finalSize,
		Sha1:      finalSha1Sum,
	})
	if err != nil {
		return errors.Wrap(err, "close replica failed")
	}

	log.Printf("closed replica of segment %d", segmentID)

	return nil
}
