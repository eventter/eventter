package mq

import (
	"context"
	"testing"

	"eventter.io/mq/client"
	"github.com/stretchr/testify/assert"
)

func TestServer_Publish(t *testing.T) {
	assert := assert.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.CreateTopic(ctx, &client.CreateTopicRequest{
			Topic: client.Topic{
				Name: client.NamespaceName{
					Namespace: "default",
					Name:      "test-publish",
				},
				Type:              client.ExchangeTypeFanout,
				Shards:            1,
				ReplicationFactor: 1,
				Retention:         1,
			},
		})
		assert.NoError(err)
		assert.True(response.OK)

		segments := ts.ClusterStateStore.Current().FindOpenSegmentsFor(ClusterSegment_TOPIC, "default", "test-publish")
		assert.Len(segments, 0)
	}

	{
		response, err := ts.Server.Publish(ctx, &client.PublishRequest{
			Topic: client.NamespaceName{
				Namespace: "default",
				Name:      "test-publish",
			},
			Message: &client.Message{
				Data: []byte("hello, world"),
			},
		})
		assert.NoError(err)
		assert.True(response.OK)

		segments := ts.ClusterStateStore.Current().FindOpenSegmentsFor(ClusterSegment_TOPIC, "default", "test-publish")
		assert.Len(segments, 1)
	}
}
