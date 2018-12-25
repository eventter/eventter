package mq

import (
	"context"
	"fmt"
	"testing"

	"eventter.io/mq/client"
	"github.com/stretchr/testify/require"
)

func TestServer_CreateTopic(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, exchangeType := range []string{client.ExchangeTypeDirect, client.ExchangeTypeFanout, client.ExchangeTypeTopic, client.ExchangeTypeHeaders} {
		topicName := fmt.Sprintf("test-create-topic-%s", exchangeType)
		response, err := ts.Server.CreateTopic(ctx, &client.CreateTopicRequest{
			Topic: client.Topic{
				Name: client.NamespaceName{
					Namespace: "default",
					Name:      topicName,
				},
				Type:              exchangeType,
				Shards:            1,
				ReplicationFactor: 1,
				Retention:         1,
			},
		})
		assert.NoError(err)
		assert.True(response.OK)

		ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
		assert.NotNil(ns)
		tp, _ := ns.FindTopic(topicName)
		assert.NotNil(tp)
	}
}
