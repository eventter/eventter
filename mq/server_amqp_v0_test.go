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

func newClient(t *testing.T) (x1 *testServer, x2 *v0.Transport, cleanup func(), err error) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		if err != nil {
			ts.Close()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	ctx = amqp.NewContextV0(ctx, nil, 0, "/")

	clientConn, serverConn := net.Pipe()

	go func() {
		defer serverConn.Close()
		err := ts.Server.ServeAMQPv0(ctx, v0.NewTransport(serverConn))
		assert.NoError(err)
	}()

	client := v0.NewTransport(clientConn)

	return ts, client, func() {
		cancel()

		response := &v0.ConnectionCloseOk{}
		err := client.Call(&v0.ConnectionClose{}, &response)
		assert.NoError(err)
		assert.NotNil(response)

		err = clientConn.Close()
		assert.NoError(err)
	}, nil
}

func TestServer_ServeAMQPv0_ExchangeDeclare(t *testing.T) {
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
				Exchange:  "not-exists",
				Type:      "fanout",
				Passive:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotFound), response.ReplyCode)
		}
	}
}

func TestServer_ServeAMQPv0_ExchangeDelete(t *testing.T) {
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
			var response *v0.ExchangeDeclareOk
			err := client.Call(&v0.ExchangeDeclare{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Exchange:  "test-exchange-delete",
				Type:      "fanout",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			tp, _ := ns.FindTopic("test-exchange-delete")
			assert.NotNil(tp)
		}

		{
			var response *v0.ExchangeDeleteOk
			err := client.Call(&v0.ExchangeDelete{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Exchange:  "test-exchange-delete",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			tp, _ := ns.FindTopic("test-exchange-delete")
			assert.Nil(tp)
		}
	}
}

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
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			cg, _ := ns.FindConsumerGroup("test-queue-declare")
			assert.NotNil(cg)
			assert.Len(cg.Bindings, 0)
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
			assert.Len(cg.Bindings, 0)
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
			assert.Len(cg.Bindings, 0)
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
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.NotEmpty(response.Queue)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			cg, _ := ns.FindConsumerGroup(response.Queue)
			assert.NotNil(cg)
			assert.Len(cg.Bindings, 0)
			assert.Equal(uint32(defaultConsumerGroupSize), cg.Size_)
			assert.Len(cg.OffsetCommits, 0)
		}
	}
}

func TestServer_ServeAMQPv0_QueueDelete(t *testing.T) {
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
				Queue:     "test-queue-delete",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			cg, _ := ns.FindConsumerGroup("test-queue-delete")
			assert.NotNil(cg)
		}

		{
			var response *v0.QueueDeleteOk
			err := client.Call(&v0.QueueDelete{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "test-queue-delete",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
			assert.NotNil(ns)
			cg, _ := ns.FindConsumerGroup("test-queue-delete")
			assert.Nil(cg)
		}
	}
}

func TestServer_ServeAMQPv0_QueueBind(t *testing.T) {
	tests := []struct {
		exchangeType string
		routingKey   string
		arguments    *types.Struct
	}{
		{"fanout", "", nil},
		{"direct", "foo", nil},
		{"topic", "foo.#", nil},
		{"headers", "all", &types.Struct{
			Fields: map[string]*types.Value{
				"x-match": {Kind: &types.Value_StringValue{StringValue: "all"}},
				"foo":     {Kind: &types.Value_StringValue{StringValue: "bar"}},
				"baz":     {Kind: &types.Value_StringValue{StringValue: "qux"}},
			},
		}},
		{"headers", "any", &types.Struct{
			Fields: map[string]*types.Value{
				"x-match": {Kind: &types.Value_StringValue{StringValue: "any"}},
				"foo":     {Kind: &types.Value_StringValue{StringValue: "bar"}},
				"baz":     {Kind: &types.Value_StringValue{StringValue: "qux"}},
			},
		}},
	}
	for _, test := range tests {
		t.Run(test.exchangeType+"/"+test.routingKey, func(t *testing.T) {
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
					var response *v0.ExchangeDeclareOk
					err := client.Call(&v0.ExchangeDeclare{
						FrameMeta: v0.FrameMeta{Channel: channel},
						Exchange:  "xchng",
						Type:      test.exchangeType,
					}, &response)
					assert.NoError(err)
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
				}

				{
					var response *v0.QueueBindOk
					err := client.Call(&v0.QueueBind{
						FrameMeta:  v0.FrameMeta{Channel: channel},
						Queue:      "q",
						Exchange:   "xchng",
						RoutingKey: test.routingKey,
						Arguments:  test.arguments,
					}, &response)
					assert.NoError(err)
					assert.NotNil(response)

					ns, _ := ts.ClusterStateStore.Current().FindNamespace("default")
					assert.NotNil(ns)
					cg, _ := ns.FindConsumerGroup("q")
					assert.NotNil(cg)
					assert.Len(cg.Bindings, 1)
					assert.Equal("xchng", cg.Bindings[0].TopicName)
				}
			}
		})
	}
}
