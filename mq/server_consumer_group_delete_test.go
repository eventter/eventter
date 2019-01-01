package mq

import (
	"context"
	"testing"

	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_DeleteConsumerGroup(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.CreateConsumerGroup(ctx, &emq.ConsumerGroupCreateRequest{
			ConsumerGroup: emq.ConsumerGroup{
				Namespace: "default",
				Name:      "test-delete-consumer-group",
			},
		})
		assert.NoError(err)
		assert.True(response.OK)

		ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
		assert.NotNil(ns)
		cg, _ := ns.FindConsumerGroup("test-delete-consumer-group")
		assert.NotNil(cg)
	}

	{
		response, err := ts.Server.DeleteConsumerGroup(ctx, &emq.ConsumerGroupDeleteRequest{
			Namespace: "default",
			Name:      "test-delete-consumer-group",
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)

		ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
		assert.NotNil(ns)
		cg, _ := ns.FindConsumerGroup("test-delete-consumer-group")
		assert.Nil(cg)
	}
}
