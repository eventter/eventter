package mq

import (
	"context"
	"fmt"
	"testing"

	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_CreateTopic(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, exchangeType := range []string{emq.ExchangeTypeDirect, emq.ExchangeTypeFanout, emq.ExchangeTypeTopic, emq.ExchangeTypeHeaders} {
		topicName := fmt.Sprintf("test-create-topic-%s", exchangeType)
		response, err := ts.Server.CreateTopic(ctx, &emq.CreateTopicRequest{
			Topic: emq.Topic{
				Name: emq.NamespaceName{
					Namespace: "default",
					Name:      topicName,
				},
				DefaultExchangeType: exchangeType,
				Shards:              1,
				ReplicationFactor:   1,
				Retention:           1,
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
