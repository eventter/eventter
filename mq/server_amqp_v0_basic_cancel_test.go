package mq

import (
	"testing"

	"eventter.io/mq/amqp/v0"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_BasicCancel(t *testing.T) {
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
				Exchange:  "xchng",
				Type:      "fanout",
				Durable:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
		}

		{
			var response *v0.QueueDeclareOk
			err := client.Call(&v0.QueueDeclare{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
				Durable:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Len(ts.Server.groups, 1)
		}

		{
			var response *v0.QueueBindOk
			err := client.Call(&v0.QueueBind{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
				Exchange:  "xchng",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
		}

		{
			var response *v0.BasicConsumeOk
			err := client.Call(&v0.BasicConsume{
				FrameMeta:   v0.FrameMeta{Channel: channel},
				Queue:       "q",
				ConsumerTag: "ctag",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
		}

		{
			var response *v0.BasicCancelOk
			err := client.Call(&v0.BasicCancel{
				FrameMeta:   v0.FrameMeta{Channel: channel},
				ConsumerTag: "ctag",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
		}
	}
}
