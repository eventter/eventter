package mq

import (
	"context"
	"testing"

	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_DeleteTopic(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.DeleteTopic(ctx, &emq.DeleteTopicRequest{
			Topic: emq.NamespaceName{
				Namespace: "default",
				Name:      "test-delete-topic",
			},
		})
		assert.Error(err)
		assert.Nil(response)
	}

	{
		response, err := ts.Server.CreateTopic(ctx, &emq.CreateTopicRequest{
			Topic: emq.Topic{
				Name: emq.NamespaceName{
					Namespace: "default",
					Name:      "test-delete-topic",
				},
				DefaultExchangeType: emq.ExchangeTypeFanout,
				Shards:              1,
				ReplicationFactor:   1,
				Retention:           1,
			},
		})
		assert.NoError(err)
		assert.True(response.OK)
		ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
		assert.NotNil(ns)
		tp, _ := ns.FindTopic("test-delete-topic")
		assert.NotNil(tp)
	}

	{
		response, err := ts.Server.DeleteTopic(ctx, &emq.DeleteTopicRequest{
			Topic: emq.NamespaceName{
				Namespace: "default",
				Name:      "test-delete-topic",
			},
		})
		assert.NoError(err)
		assert.True(response.OK)
		ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
		assert.NotNil(ns)
		tp, _ := ns.FindTopic("test-delete-topic")
		assert.Nil(tp)
	}
}
