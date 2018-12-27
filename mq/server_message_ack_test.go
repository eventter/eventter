package mq

import (
	"context"
	"testing"

	"eventter.io/mq/client"
	"github.com/stretchr/testify/require"
)

func TestServer_Ack(t *testing.T) {
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
					Name:      "test-ack-topic",
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
					Name:      "test-ack-consumer-group",
				},
				Bindings: []*client.ConsumerGroup_Binding{
					{TopicName: "test-ack-topic"},
				},
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)

		ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
		assert.NotNil(ns)
		cg, _ := ns.FindConsumerGroup("test-ack-consumer-group")
		assert.NotNil(cg)

		ts.WaitForConsumerGroup(t, ctx, "default", "test-ack-consumer-group")
	}

	{
		response, err := ts.Server.Publish(ctx, &client.PublishRequest{
			Topic: client.NamespaceName{
				Namespace: "default",
				Name:      "test-ack-topic",
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
		ts.WaitForMessage(t, ctx, "default", "test-ack-consumer-group")

		stream := newSubscribeConsumer(ctx, 0, "", nil)

		go func() {
			defer stream.Close()

			err := ts.Server.Subscribe(&client.SubscribeRequest{
				ConsumerGroup: client.NamespaceName{
					Namespace: "default",
					Name:      "test-ack-consumer-group",
				},
				Size_:      1,
				DoNotBlock: true,
			}, stream)
			assert.NoError(err)
		}()

		delivery, ok := <-stream.C
		assert.True(ok)

		response, err := ts.Server.Ack(ctx, &client.AckRequest{
			NodeID:         delivery.Response.NodeID,
			SubscriptionID: delivery.Response.SubscriptionID,
			SeqNo:          delivery.Response.SeqNo,
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)

		_, ok = <-stream.C
		assert.False(ok)
	}
}
