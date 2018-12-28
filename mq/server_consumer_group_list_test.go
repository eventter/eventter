package mq

import (
	"context"
	"fmt"
	"testing"

	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_ListConsumerGroups(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < 5; i++ {
		cgName := fmt.Sprintf("test-list-consumer-group-%d", i)

		response, err := ts.Server.CreateConsumerGroup(ctx, &emq.CreateConsumerGroupRequest{
			ConsumerGroup: emq.ConsumerGroup{
				Name: emq.NamespaceName{
					Namespace: "default",
					Name:      cgName,
				},
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)
	}

	{
		response, err := ts.Server.ListConsumerGroups(ctx, &emq.ListConsumerGroupsRequest{
			ConsumerGroup: emq.NamespaceName{
				Namespace: "default",
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)
		assert.Len(response.ConsumerGroups, 5)
	}
}
