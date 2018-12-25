package mq

import (
	"context"
	"testing"

	"eventter.io/mq/client"
	"github.com/stretchr/testify/assert"
)

func TestServer_DeleteTopic(t *testing.T) {
	assert := assert.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		response, err := ts.Server.DeleteTopic(ctx, &client.DeleteTopicRequest{
			Topic: client.NamespaceName{
				Namespace: "default",
				Name:      "test-delete-topic",
			},
		})
		assert.Error(err)
		assert.Nil(response)
	}

	{
		response, err := ts.Server.CreateTopic(ctx, &client.CreateTopicRequest{
			Topic: client.Topic{
				Name: client.NamespaceName{
					Namespace: "default",
					Name:      "test-delete-topic",
				},
				Type:              client.ExchangeTypeFanout,
				Shards:            1,
				ReplicationFactor: 1,
				Retention:         1,
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
		response, err := ts.Server.DeleteTopic(ctx, &client.DeleteTopicRequest{
			Topic: client.NamespaceName{
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
