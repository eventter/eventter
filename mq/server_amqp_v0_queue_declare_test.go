package mq

import (
	"testing"

	"eventter.io/mq/amqp/v0"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_QueueDeclare(t *testing.T) {
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
				Queue:     "test-queue-declare",
				Durable:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			cg, _ := ns.FindConsumerGroup("test-queue-declare")
			assert.NotNil(cg)
			assert.Len(cg.Bindings, 1)
			assert.Equal(uint32(defaultConsumerGroupSize), cg.Size_)
			assert.Len(cg.OffsetCommits, 0)
		}
	}

	{
		var channel uint16 = 2
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
				Queue:     "test-queue-declare-size",
				Durable:   true,
				Arguments: &types.Struct{
					Fields: map[string]*types.Value{
						"size": {Kind: &types.Value_NumberValue{NumberValue: 50}},
					},
				},
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			cg, _ := ns.FindConsumerGroup("test-queue-declare-size")
			assert.NotNil(cg)
			assert.Len(cg.Bindings, 1)
			assert.Equal(uint32(50), cg.Size_)
			assert.Len(cg.OffsetCommits, 0)
		}
	}

	{
		var channel uint16 = 3
		{
			var response *v0.ChannelOpenOk
			err := client.Call(&v0.ChannelOpen{FrameMeta: v0.FrameMeta{Channel: channel}}, &response)
			assert.NoError(err)
			assert.NotNil(response)
		}

		{
			var response *v0.ChannelClose
			err := client.Call(&v0.QueueDeclare{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "not-exists",
				Durable:   true,
				Passive:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotFound), response.ReplyCode)
		}
	}

	{
		var channel uint16 = 4
		{
			var response *v0.ChannelOpenOk
			err := client.Call(&v0.ChannelOpen{FrameMeta: v0.FrameMeta{Channel: channel}}, &response)
			assert.NoError(err)
			assert.NotNil(response)
		}

		{
			err := client.Send(&v0.QueueDeclare{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "test-queue-declare-nowait",
				Durable:   true,
				NoWait:    true,
			})
			assert.NoError(err)
		}

		{
			var response *v0.ChannelCloseOk
			err := client.Call(&v0.ChannelClose{FrameMeta: v0.FrameMeta{Channel: channel}}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			cg, _ := ns.FindConsumerGroup("test-queue-declare-nowait")
			assert.NotNil(cg)
			assert.Len(cg.Bindings, 1)
			assert.Equal(uint32(defaultConsumerGroupSize), cg.Size_)
			assert.Len(cg.OffsetCommits, 0)
		}
	}

	{
		var channel uint16 = 5
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
				Queue:     "",
				Durable:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.NotEmpty(response.Queue)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			cg, _ := ns.FindConsumerGroup(response.Queue)
			assert.NotNil(cg)
			assert.Len(cg.Bindings, 1)
			assert.Equal(uint32(defaultConsumerGroupSize), cg.Size_)
			assert.Len(cg.OffsetCommits, 0)
		}
	}
}
