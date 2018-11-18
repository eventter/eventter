package mq

import (
	"bytes"
	"context"
	"crypto/sha1"
	"io"
	"log"
	"math"
	"runtime"

	"eventter.io/mq/segmentfile"
	"google.golang.org/grpc"
)

func (s *Server) taskSegmentReplicate(ctx context.Context, completed func(), segmentID uint64, nodeID uint64, wait bool) {
	defer completed()

	log.Printf("starting to replicate segment %d from node %d", segmentID, nodeID)

	node := s.clusterState.Current().GetNode(nodeID)
	if node == nil {
		log.Printf("cannot replicate segment %d from node %d - node not found", segmentID, nodeID)
		return
	}

	segment, err := s.segmentDir.Open(segmentID)
	if err != nil {
		log.Printf("cannot replicate segment %d from node %d - open failed: %v", segmentID, nodeID, err)
		return
	}
	defer s.segmentDir.Release(segment)

	cc, err := s.pool.Get(ctx, node.Address)
	if err != nil {
		log.Printf("cannot replicate segment %d from node %d - dial failed: %v", segmentID, nodeID, err)
		return
	}
	defer s.pool.Put(cc)

	sha1Sum, size, err := segment.Sum(sha1.New(), segmentfile.SumAll)
	if err != nil {
		log.Printf("cannot replicate segment %d from node %d - local sum failed: %v", segmentID, nodeID, err)
		return
	}

	client := NewNodeRPCClient(cc)

	sumResponse, err := client.SegmentSum(ctx, &SegmentSumRequest{
		SegmentID: segmentID,
		Size_:     size,
	})
	if err != nil {
		log.Printf("cannot replicate segment %d from node %d - remote sum failed: %v", segmentID, nodeID, err)
		return
	}

	if !bytes.Equal(sha1Sum, sumResponse.Sha1) {
		size = segmentfile.TruncateAll
	}

	if err := segment.Truncate(size); err != nil {
		log.Printf("cannot replicate segment %d from node %d - truncate failed: %v", segmentID, nodeID, err)
		return
	}

	stream, err := client.SegmentRead(ctx, &SegmentReadRequest{
		SegmentID: segmentID,
		Offset:    size,
		Wait:      wait,
	}, grpc.MaxCallRecvMsgSize(math.MaxUint32))
	if err != nil {
		log.Printf("cannot replicate segment %d from node %d - read failed: %v", segmentID, nodeID, err)
		return
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
			log.Printf("cannot replicate segment %d from node %d - receive failed: %v", segmentID, nodeID, err)
			return
		}

		if err := segment.Write(response.Data); err != nil {
			log.Printf("cannot replicate segment %d from node %d - write failed: %v", segmentID, nodeID, err)
			return
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
				return
			default:
				runtime.Gosched()
			}
			state = s.clusterState.Current()
			closedSegment = state.GetClosedSegment(segmentID)
		}
	}

	finalSha1Sum, finalSize, err := segment.Sum(sha1.New(), segmentfile.SumAll)
	if err != nil {
		log.Printf("cannot sum segment %d: %v", segmentID, err)
		return
	}

	_, err = s.SegmentReplicaClose(ctx, &SegmentCloseRequest{
		NodeID:    s.nodeID,
		SegmentID: segmentID,
		Size_:     finalSize,
		Sha1:      finalSha1Sum,
	})
	if err != nil {
		log.Printf("cannot close replica of segment %d: %v", segmentID, err)
		return
	}

	log.Printf("closed replica of segment %d", segmentID)
}
