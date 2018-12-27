package mq

import (
	"fmt"
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

func TestServer_ServeAMQPv0_BasicRecover(t *testing.T) {
	tests := []struct {
		requeue bool
	}{
		{true},
		{false},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("requeue=%t", test.requeue), func(t *testing.T) {
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
					err := client.Call(&v0.BasicRecover{
						FrameMeta: v0.FrameMeta{Channel: channel},
						Requeue:   test.requeue,
					}, &response)
					assert.NoError(err)
					assert.NotNil(response)
					assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
				}
			}
		})
	}
}

func TestServer_ServeAMQPv0_BasicRecoverAsync(t *testing.T) {
	tests := []struct {
		requeue bool
	}{
		{true},
		{false},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("requeue=%t", test.requeue), func(t *testing.T) {
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
					err := client.Call(&v0.BasicRecoverAsync{
						FrameMeta: v0.FrameMeta{Channel: channel},
						Requeue:   test.requeue,
					}, &response)
					assert.NoError(err)
					assert.NotNil(response)
					assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
				}
			}
		})
	}
}

func TestServer_ServeAMQPv0_TxSelect(t *testing.T) {
	assert := require.New(t)

	_, client, cleanup, err := newClient(t)
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
			var response *v0.ConnectionClose
			err := client.Call(&v0.TxSelect{
				FrameMeta: v0.FrameMeta{Channel: channel},
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
		}
	}
}

func TestServer_ServeAMQPv0_TxCommit(t *testing.T) {
	assert := require.New(t)

	_, client, cleanup, err := newClient(t)
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
			var response *v0.ConnectionClose
			err := client.Call(&v0.TxCommit{
				FrameMeta: v0.FrameMeta{Channel: channel},
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
		}
	}
}

func TestServer_ServeAMQPv0_TxRollback(t *testing.T) {
	assert := require.New(t)

	_, client, cleanup, err := newClient(t)
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
			var response *v0.ConnectionClose
			err := client.Call(&v0.TxRollback{
				FrameMeta: v0.FrameMeta{Channel: channel},
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
		}
	}
}
