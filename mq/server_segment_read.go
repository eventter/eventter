package mq

import (
	"context"
	"io"

	"eventter.io/mq/segments"
	"github.com/pkg/errors"
)

func (s *Server) SegmentRead(request *SegmentReadRequest, stream NodeRPC_SegmentReadServer) error {
	if !s.segmentDir.Exists(request.SegmentID) {
		return errors.Errorf("segment %d does not exist", request.SegmentID)
	}

	segment, err := s.segmentDir.Open(request.SegmentID)
	if err != nil {
		return errors.Wrap(err, "segment open failed")
	}
	defer s.segmentDir.Release(segment)

	var iterator *segments.Iterator
	if request.Offset > 0 {
		iterator, err = segment.ReadAt(request.Offset, request.Wait)
	} else {
		iterator, err = segment.Read(request.Wait)
	}
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	go func() {
		<-ctx.Done()
		iterator.Close()
	}()

	for {
		data, offset, err := iterator.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return errors.Wrap(err, "iterator next failed")
		}

		err = stream.Send(&SegmentReadResponse{
			SegmentID: request.SegmentID,
			Offset:    offset,
			Data:      data,
		})
		if err != nil {
			return errors.Wrap(err, "send failed")
		}
	}
}
