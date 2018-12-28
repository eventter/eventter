package mq

import (
	"context"
	"testing"
	"time"

	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_ConsumerGroupWait(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.CreateConsumerGroup(ctx, &emq.CreateConsumerGroupRequest{
			ConsumerGroup: emq.ConsumerGroup{
				Name: emq.NamespaceName{
					Namespace: "default",
					Name:      "test-subscribe-consumer-group",
				},
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)
	}

	{
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		response, err := ts.Server.ConsumerGroupWait(ctx, &ConsumerGroupWaitRequest{
			ConsumerGroup: emq.NamespaceName{
				Namespace: "default",
				Name:      "test-subscribe-consumer-group",
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
	}

}
