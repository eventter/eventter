package mq

import (
	"context"
	"testing"

	"eventter.io/mq/client"
	"github.com/stretchr/testify/assert"
)

func TestServer_CreateNamespace(t *testing.T) {
	assert := assert.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.CreateNamespace(ctx, &client.CreateNamespaceRequest{Namespace: "test-create-namespace"})
		assert.NoError(err)
		assert.True(response.OK)

		ns, _ := ts.ClusterStateStore.Current().FindNamespace("test-create-namespace")
		assert.NotNil(ns)
	}

	{
		response, err := ts.Server.CreateNamespace(ctx, &client.CreateNamespaceRequest{Namespace: "test-create-namespace"})
		assert.NoError(err)
		assert.True(response.OK)
	}
}
