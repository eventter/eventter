package mq

import (
	"context"
	"testing"

	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_Nack(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.CreateTopic(ctx, &emq.CreateTopicRequest{
			Topic: emq.Topic{
				Name: emq.NamespaceName{
					Namespace: "default",
					Name:      "test-nack-topic",
				},
				DefaultExchangeType: emq.ExchangeTypeFanout,
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)
	}

	{
		response, err := ts.Server.CreateConsumerGroup(ctx, &emq.CreateConsumerGroupRequest{
			ConsumerGroup: emq.ConsumerGroup{
				Name: emq.NamespaceName{
					Namespace: "default",
					Name:      "test-nack-consumer-group",
				},
				Bindings: []*emq.ConsumerGroup_Binding{
					{TopicName: "test-nack-topic", ExchangeType: emq.ExchangeTypeFanout},
				},
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)

		ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
		assert.NotNil(ns)
		cg, _ := ns.FindConsumerGroup("test-nack-consumer-group")
		assert.NotNil(cg)

		ts.WaitForConsumerGroup(t, ctx, "default", "test-nack-consumer-group")
	}

	{
		response, err := ts.Server.Publish(ctx, &emq.PublishRequest{
			Topic: emq.NamespaceName{
				Namespace: "default",
				Name:      "test-nack-topic",
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
		ts.WaitForMessage(t, ctx, "default", "test-nack-consumer-group")

		stream := newSubscribeConsumer(ctx, 0, "", nil)

		go func() {
			defer stream.Close()

			err := ts.Server.Subscribe(&emq.SubscribeRequest{
				ConsumerGroup: emq.NamespaceName{
					Namespace: "default",
					Name:      "test-nack-consumer-group",
				},
				Size_:      1,
				DoNotBlock: true,
			}, stream)
			assert.NoError(err)
		}()

		delivery, ok := <-stream.C
		assert.True(ok)
		assert.Equal("hello, world", string(delivery.Response.Message.Data))

		response, err := ts.Server.Nack(ctx, &emq.NackRequest{
			NodeID:         delivery.Response.NodeID,
			SubscriptionID: delivery.Response.SubscriptionID,
			SeqNo:          delivery.Response.SeqNo,
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)

		delivery, ok = <-stream.C
		assert.True(ok)
		assert.Equal("hello, world", string(delivery.Response.Message.Data))
	}
}
