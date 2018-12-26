package mq

import (
	"context"
	"testing"

	"eventter.io/mq/client"
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
		response, err := ts.Server.CreateTopic(ctx, &client.CreateTopicRequest{
			Topic: client.Topic{
				Name: client.NamespaceName{
					Namespace: "default",
					Name:      "test-subscribe-topic",
				},
				Type: client.ExchangeTypeFanout,
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)
	}

	{
		response, err := ts.Server.CreateConsumerGroup(ctx, &client.CreateConsumerGroupRequest{
			ConsumerGroup: client.ConsumerGroup{
				Name: client.NamespaceName{
					Namespace: "default",
					Name:      "test-subscribe-consumer-group",
				},
				Bindings: []*client.ConsumerGroup_Binding{
					{TopicName: "test-subscribe-topic"},
				},
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)

		ts.WaitForConsumerGroup("default", "test-subscribe-consumer-group")
	}

	{
		response, err := ts.Server.Publish(ctx, &client.PublishRequest{
			Topic: client.NamespaceName{
				Namespace: "default",
				Name:      "test-subscribe-topic",
			},
			Message: &client.Message{
				Data: []byte("hello, world"),
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)
	}

	{
		stream := NewSubscribeStreamLocal(ctx)
		defer stream.Close()

		go func() {
			err := ts.Server.Subscribe(&client.SubscribeRequest{
				ConsumerGroup: client.NamespaceName{
					Namespace: "default",
					Name:      "test-subscribe-consumer-group",
				},
			}, stream)
			assert.NoError(err)
		}()

		response := <-stream.C
		assert.Equal("default", response.Topic.Namespace)
		assert.Equal("test-subscribe-topic", response.Topic.Name)
		assert.NotNil(response.Message)
		assert.Equal([]byte("hello, world"), response.Message.Data)
		assert.Equal(ts.Server.nodeID, response.NodeID)
		assert.Condition(func() (success bool) { return response.SubscriptionID > 0 })
		assert.Condition(func() (success bool) { return response.SeqNo > 0 })
	}
}
