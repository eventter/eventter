package mq

import (
	"context"
	"testing"

	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_Publish(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.CreateTopic(ctx, &emq.TopicCreateRequest{
			Topic: emq.Topic{
				Namespace:           "default",
				Name:                "test-publish",
				DefaultExchangeType: emq.ExchangeTypeFanout,
				Shards:              1,
				ReplicationFactor:   1,
				Retention:           1,
			},
		})
		assert.NoError(err)
		assert.True(response.OK)

		segments := ts.ClusterStateStore.Current().FindOpenSegmentsFor(ClusterSegment_TOPIC, "default", "test-publish")
		assert.Len(segments, 0)
	}

	{
		response, err := ts.Server.Publish(ctx, &emq.TopicPublishRequest{
			Namespace: "default",
			Name:      "test-publish",
			Message: &emq.Message{
				Data: []byte("hello, world"),
			},
		})
		assert.NoError(err)
		assert.True(response.OK)

		segments := ts.ClusterStateStore.Current().FindOpenSegmentsFor(ClusterSegment_TOPIC, "default", "test-publish")
		assert.Len(segments, 1)
	}
}
