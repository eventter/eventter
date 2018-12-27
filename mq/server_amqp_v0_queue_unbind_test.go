package mq

import (
	"testing"

	"eventter.io/mq/amqp/v0"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_QueueUnbind(t *testing.T) {
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
					assert.Len(cg.Bindings, 1)
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
					assert.Len(cg.Bindings, 2)
					assert.Equal("xchng", cg.Bindings[1].TopicName)
				}

				{
					var response *v0.QueueUnbindOk
					err := client.Call(&v0.QueueUnbind{
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
				}
			}
		})
	}
}
