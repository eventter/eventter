package mq

import (
	"github.com/hashicorp/memberlist"
)

type Reconciler struct {
	delegate ReconcilerDelegate
}

type ReconcilerDelegate interface {
	Apply(cmd interface{}) (uint64, error)
	Members() []*memberlist.Node
	AddVoter(id string, addr string) error
}

func NewReconciler(delegate ReconcilerDelegate) *Reconciler {
	return &Reconciler{
		delegate: delegate,
	}
}
