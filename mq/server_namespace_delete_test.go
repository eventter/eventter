package mq

import (
	"context"
	"testing"

	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_DeleteNamespace(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.CreateNamespace(ctx, &emq.CreateNamespaceRequest{Namespace: "test-delete-namespace"})
		assert.NoError(err)
		assert.True(response.OK)

		ns, _ := ts.ClusterStateStore.Current().FindNamespace("test-delete-namespace")
		assert.NotNil(ns)
	}

	{
		response, err := ts.Server.DeleteNamespace(ctx, &emq.DeleteNamespaceRequest{Namespace: "test-delete-namespace"})
		assert.NoError(err)
		assert.True(response.OK)
	}
}

func TestServer_DeleteNamespace_ShouldFailIfNamespaceDoesntExit(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.DeleteNamespace(ctx, &emq.DeleteNamespaceRequest{Namespace: "does-not-exit"})
		assert.Error(err)
		assert.Nil(response)
	}
}

func TestServer_DeleteNamespace_ShouldFailIfTryingToDeleteDefaultNamespace(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.DeleteNamespace(ctx, &emq.DeleteNamespaceRequest{Namespace: emq.DefaultNamespace})
		assert.Error(err)
		assert.Nil(response)
	}
}
