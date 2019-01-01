package mq

import (
	"context"
	"testing"

	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_Subscribe(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.CreateTopic(ctx, &emq.TopicCreateRequest{
			Topic: emq.Topic{
				Name: emq.NamespaceName{
					Namespace: "default",
					Name:      "test-subscribe-topic",
				},
				DefaultExchangeType: emq.ExchangeTypeFanout,
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)
	}

	{
		response, err := ts.Server.CreateConsumerGroup(ctx, &emq.ConsumerGroupCreateRequest{
			ConsumerGroup: emq.ConsumerGroup{
				Name: emq.NamespaceName{
					Namespace: "default",
					Name:      "test-subscribe-consumer-group",
				},
				Bindings: []*emq.ConsumerGroup_Binding{
					{TopicName: "test-subscribe-topic", ExchangeType: emq.ExchangeTypeFanout},
				},
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)

		ts.WaitForConsumerGroup(t, ctx, "default", "test-subscribe-consumer-group")
	}

	{
		response, err := ts.Server.Publish(ctx, &emq.TopicPublishRequest{
			Topic: emq.NamespaceName{
				Namespace: "default",
				Name:      "test-subscribe-topic",
			},
			Message: &emq.Message{
				Data: []byte("hello, world"),
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)
	}

	{
		stream := newSubscribeConsumer(ctx, 0, "", nil)

		go func() {
			defer stream.Close()

			err := ts.Server.Subscribe(&emq.ConsumerGroupSubscribeRequest{
				ConsumerGroup: emq.NamespaceName{
					Namespace: "default",
					Name:      "test-subscribe-consumer-group",
				},
			}, stream)
			assert.NoError(err)
		}()

		delivery := <-stream.C
		response := delivery.Response
		assert.NotNil(response)
		assert.Equal("default", response.Topic.Namespace)
		assert.Equal("test-subscribe-topic", response.Topic.Name)
		assert.NotNil(response.Message)
		assert.Equal([]byte("hello, world"), response.Message.Data)
		assert.Equal(ts.Server.nodeID, response.NodeID)
		assert.Condition(func() (success bool) { return response.SubscriptionID > 0 })
		assert.Condition(func() (success bool) { return response.SeqNo > 0 })
	}
}
