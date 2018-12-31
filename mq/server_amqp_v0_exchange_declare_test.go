package mq

import (
	"testing"
	"time"

	"eventter.io/mq/amqp/v0"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_ExchangeDeclare(t *testing.T) {
	assert := require.New(t)

	ts, client, cleanup, err := newClientAMQPv0(t)
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
			var response *v0.ExchangeDeclareOk
			err := client.Call(&v0.ExchangeDeclare{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Exchange:  "test-exchange-declare",
				Type:      "fanout",
				Durable:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			tp, _ := ns.FindTopic("test-exchange-declare")
			assert.NotNil(tp)
			assert.Equal("fanout", tp.DefaultExchangeType)
			assert.Equal(uint32(1), tp.Shards)
			assert.Equal(uint32(3), tp.ReplicationFactor)
			assert.Equal(time.Duration(1), tp.Retention)
		}

		{
			var response *v0.ExchangeDeclareOk
			err := client.Call(&v0.ExchangeDeclare{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Exchange:  "test-exchange-declare",
				Type:      "fanout",
				Durable:   true,
				Passive:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			tp, _ := ns.FindTopic("test-exchange-declare")
			assert.NotNil(tp)
			assert.Equal("fanout", tp.DefaultExchangeType)
			assert.Equal(uint32(1), tp.Shards)
			assert.Equal(uint32(3), tp.ReplicationFactor)
			assert.Equal(time.Duration(1), tp.Retention)
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
			err := client.Send(&v0.ExchangeDeclare{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Exchange:  "test-exchange-declare-nowait",
				Type:      "direct",
				Durable:   true,
				Arguments: &types.Struct{
					Fields: map[string]*types.Value{
						"shards":             {Kind: &types.Value_NumberValue{NumberValue: 5}},
						"replication-factor": {Kind: &types.Value_NumberValue{NumberValue: 1}},
						"retention":          {Kind: &types.Value_StringValue{StringValue: "30m"}},
					},
				},
				NoWait: true,
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
			tp, _ := ns.FindTopic("test-exchange-declare-nowait")
			assert.NotNil(tp)
			assert.Equal("direct", tp.DefaultExchangeType)
			assert.Equal(uint32(5), tp.Shards)
			assert.Equal(uint32(1), tp.ReplicationFactor)
			assert.Equal(30*time.Minute, tp.Retention)
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
			err := client.Call(&v0.ExchangeDeclare{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Exchange:  "not-exists",
				Type:      "fanout",
				Durable:   true,
				Passive:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotFound), response.ReplyCode)
		}
	}
}
