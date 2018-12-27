package mq

import (
	"testing"
	"time"

	"eventter.io/mq/amqp/v0"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_BasicConsume(t *testing.T) {
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
				Type:      "fanout",
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
			assert.Len(ts.Server.subscriptions, 0)
		}

		{
			var response *v0.BasicConsumeOk
			err := client.Call(&v0.BasicConsume{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.NotEmpty(response.ConsumerTag)

			// subscription is created in another goroutine => wait for 1 second for it to appear
			deadline := time.Now().Add(1 * time.Second)
			for time.Now().Before(deadline) {
				ts.Server.groupMutex.Lock()
				l := len(ts.Server.subscriptions)
				ts.Server.groupMutex.Unlock()
				if l != 0 {
					break
				}
				time.Sleep(1 * time.Millisecond)
			}
			assert.Len(ts.Server.subscriptions, 1)
		}
	}
}
