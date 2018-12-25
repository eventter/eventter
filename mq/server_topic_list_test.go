package mq

import (
	"context"
	"fmt"
	"testing"

	"eventter.io/mq/client"
	"github.com/stretchr/testify/require"
)

func TestServer_ListTopics(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < 5; i++ {
		topicName := fmt.Sprintf("test-list-topic-%d", i)

		response, err := ts.Server.CreateTopic(ctx, &client.CreateTopicRequest{
			Topic: client.Topic{
				Name: client.NamespaceName{
					Namespace: "default",
					Name:      topicName,
				},
				Type:              client.ExchangeTypeFanout,
				Shards:            1,
				ReplicationFactor: 1,
				Retention:         1,
			},
		})
		assert.NoError(err)
		assert.True(response.OK)
	}

	{
		response, err := ts.Server.ListTopics(ctx, &client.ListTopicsRequest{
			Topic: client.NamespaceName{
				Namespace: "default",
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.Len(response.Topics, 5)
	}
}
