package mq

import (
	"context"
	"testing"

	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_CreateConsumerGroup(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.CreateConsumerGroup(ctx, &emq.ConsumerGroupCreateRequest{
			ConsumerGroup: emq.ConsumerGroup{
				Name: emq.NamespaceName{
					Namespace: "default",
					Name:      "test-create-consumer-group",
				},
			},
		})
		assert.NoError(err)
		assert.True(response.OK)

		ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
		assert.NotNil(ns)
		cg, _ := ns.FindConsumerGroup("test-create-consumer-group")
		assert.NotNil(cg)
		assert.Len(cg.Bindings, 0)
		assert.Equal(uint32(defaultConsumerGroupSize), cg.Size_)
	}
}
