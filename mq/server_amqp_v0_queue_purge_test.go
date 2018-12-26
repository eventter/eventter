package mq

import (
	"testing"

	"eventter.io/mq/amqp/v0"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_QueuePurge(t *testing.T) {
	assert := require.New(t)

	ts, client, cleanup, err := newClient(t)
	assert.NoError(err)
	defer cleanup()

	{
		var channel uint16 = 1
		{
			var response *v0.ChannelOpenOk
			err := client.Call(&v0.ChannelOpen{FrameMeta: v0.FrameMeta{Channel: channel}}, &response)
			assert.NoError(err)
			assert.NotNil(response)
		}

		{
			var response *v0.QueueDeclareOk
			err := client.Call(&v0.QueueDeclare{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			cg, _ := ns.FindConsumerGroup("q")
			assert.NotNil(cg)
			assert.Len(cg.Bindings, 0)
			assert.Equal(uint32(defaultConsumerGroupSize), cg.Size_)
			assert.Len(cg.OffsetCommits, 0)
		}

		{
			var response *v0.ConnectionClose
			err := client.Call(&v0.QueuePurge{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
		}
	}
}
