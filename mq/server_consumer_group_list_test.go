package mq

import (
	"context"
	"fmt"
	"testing"

	"eventter.io/mq/client"
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

		response, err := ts.Server.CreateConsumerGroup(ctx, &client.CreateConsumerGroupRequest{
			ConsumerGroup: client.ConsumerGroup{
				Name: client.NamespaceName{
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
		response, err := ts.Server.ListConsumerGroups(ctx, &client.ListConsumerGroupsRequest{
			ConsumerGroup: client.NamespaceName{
				Namespace: "default",
			},
		})
		assert.NoError(err)
		assert.NotNil(response)
		assert.True(response.OK)
		assert.Len(response.ConsumerGroups, 5)
	}
}
