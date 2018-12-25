package mq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServer_Debug(t *testing.T) {
	assert := assert.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.Debug(ctx, &DebugRequest{})
		assert.NoError(err)
		assert.NotEmpty(response.ClusterState)
		assert.Empty(response.Segments)
	}
}
