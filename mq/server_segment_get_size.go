package mq

import (
	"context"

	"github.com/pkg/errors"
)

func (s *Server) SegmentGetSize(ctx context.Context, request *SegmentGetSizeRequest) (*SegmentGetSizeResponse, error) {
	if !s.segmentDir.Exists(request.SegmentID) {
		return &SegmentGetSizeResponse{
			SegmentID: request.SegmentID,
			Size_:     -1,
		}, nil
	}

	segment, err := s.segmentDir.Open(request.SegmentID)
	if err != nil {
		return nil, errors.Wrap(err, "open segment failed")
	}
	defer s.segmentDir.Release(segment)

	return &SegmentGetSizeResponse{
		SegmentID: request.SegmentID,
		Size_:     segment.Size(),
	}, nil
}
