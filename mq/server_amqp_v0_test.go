package mq

import (
	"context"
	"net"
	"testing"
	"time"

	"eventter.io/mq/amqp"
	"eventter.io/mq/amqp/v0"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_ExchangeDeclare(t *testing.T) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	assert.NoError(err)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx = amqp.NewContextV0(ctx, nil, 0, "/")

	clientConn, serverConn := net.Pipe()

	go func() {
		defer serverConn.Close()
		err := ts.Server.ServeAMQPv0(ctx, v0.NewTransport(serverConn))
		assert.NoError(err)
	}()

	client := v0.NewTransport(clientConn)
	defer func() {
		response := &v0.ConnectionCloseOk{}
		err := client.Call(&v0.ConnectionClose{}, &response)
		assert.NoError(err)
		assert.NotNil(response)

		err = clientConn.Close()
		assert.NoError(err)
	}()

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
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			tp, _ := ns.FindTopic("test-exchange-declare")
			assert.NotNil(tp)
			assert.Equal("fanout", tp.Type)
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
				Passive:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			tp, _ := ns.FindTopic("test-exchange-declare")
			assert.NotNil(tp)
			assert.Equal("fanout", tp.Type)
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
			assert.Equal("direct", tp.Type)
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
				Exchange:  "does-not-exist",
				Type:      "fanout",
				Passive:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotFound), response.ReplyCode)
		}
	}
}
