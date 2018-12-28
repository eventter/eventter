package mq

import (
	"log"

	"eventter.io/mq/emq"
)

func (r *Reconciler) ReconcileNamespaces(state *ClusterState) {
	defaultNamespace, _ := state.FindNamespace(emq.DefaultNamespace)
	if defaultNamespace != nil {
		return
	}

	_, err := r.delegate.Apply(&ClusterCommandNamespaceCreate{Namespace: emq.DefaultNamespace})
	if err != nil {
		log.Printf("could not create default namespace: %v", err)
		return
	}

	log.Printf("created default namespace (%s)", emq.DefaultNamespace)
}
