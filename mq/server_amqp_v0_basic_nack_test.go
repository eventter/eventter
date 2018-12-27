package mq

import (
	"fmt"
	"testing"

	"eventter.io/mq/amqp/v0"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_BasicNack(t *testing.T) {
	tests := []struct {
		multiple bool
		requeue  bool
		next     string
	}{
		{true, true, "foo"},
		{true, false, "baz"},
		{false, true, "bar"},
		{false, false, "baz"},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("requeue=%t,multiple=%t", test.requeue, test.multiple), func(t *testing.T) {
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
					assert.Len(ts.Server.subscriptions, 0)
				}

				{
					var response *v0.BasicQosOk
					err := client.Call(&v0.BasicQos{
						FrameMeta:     v0.FrameMeta{Channel: channel},
						PrefetchCount: 2,
					}, &response)
					assert.NoError(err)
					assert.NotNil(response)
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
				}

				for _, x := range []string{"foo", "bar", "baz"} {
					err := client.Send(&v0.BasicPublish{
						FrameMeta: v0.FrameMeta{Channel: channel},
						Exchange:  "xchng",
					})
					assert.NoError(err)

					data := []byte(x)

					err = client.Send(&v0.ContentHeaderFrame{
						FrameMeta: v0.FrameMeta{Channel: channel},
						ClassID:   v0.BasicClass,
						BodySize:  uint64(len(data)),
					})
					assert.NoError(err)

					err = client.SendBody(channel, data)
					assert.NoError(err)
				}

				{
					var deliver *v0.BasicDeliver
					err := client.Call(nil, &deliver)
					assert.NoError(err)

					var header *v0.ContentHeaderFrame
					err = client.Call(nil, &header)
					assert.NoError(err)

					var body *v0.ContentBodyFrame
					err = client.Call(nil, &body)
					assert.NoError(err)

					assert.Equal("foo", string(body.Data))
				}

				{
					var deliver *v0.BasicDeliver
					err := client.Call(nil, &deliver)
					assert.NoError(err)

					var header *v0.ContentHeaderFrame
					err = client.Call(nil, &header)
					assert.NoError(err)

					var body *v0.ContentBodyFrame
					err = client.Call(nil, &body)
					assert.NoError(err)

					assert.Equal("bar", string(body.Data))

					err = client.Send(&v0.BasicNack{
						FrameMeta:   v0.FrameMeta{Channel: channel},
						DeliveryTag: deliver.DeliveryTag,
						Multiple:    test.multiple,
						Requeue:     test.requeue,
					})
					assert.NoError(err)
				}

				{
					var deliver *v0.BasicDeliver
					err := client.Call(nil, &deliver)
					assert.NoError(err)

					var header *v0.ContentHeaderFrame
					err = client.Call(nil, &header)
					assert.NoError(err)

					var body *v0.ContentBodyFrame
					err = client.Call(nil, &body)
					assert.NoError(err)

					assert.Equal(test.next, string(body.Data))
				}

				if test.multiple && test.requeue {
					{
						var deliver *v0.BasicDeliver
						err := client.Call(nil, &deliver)
						assert.NoError(err)

						var header *v0.ContentHeaderFrame
						err = client.Call(nil, &header)
						assert.NoError(err)

						var body *v0.ContentBodyFrame
						err = client.Call(nil, &body)
						assert.NoError(err)

						assert.Equal("bar", string(body.Data))
					}
				}

				{
					var response *v0.ChannelCloseOk
					err := client.Call(&v0.ChannelClose{
						FrameMeta: v0.FrameMeta{Channel: channel},
					}, &response)
					assert.NoError(err)
					assert.NotNil(response)
				}
			}
		})
	}
}
