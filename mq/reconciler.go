package mq

import (
	"context"

	"github.com/hashicorp/memberlist"
)

type Reconciler struct {
	delegate ReconcilerDelegate
}

type ReconcilerDelegate interface {
	Apply(cmd interface{}) (uint64, error)
	Members() []*memberlist.Node
	AddVoter(id string, addr string) error
	GetSegmentSizeFromNode(ctx context.Context, segmentID uint64, nodeID uint64, nodeAddr string) (size int64, err error)
	NextSegmentID() uint64
}

func NewReconciler(delegate ReconcilerDelegate) *Reconciler {
	return &Reconciler{
		delegate: delegate,
	}
}
