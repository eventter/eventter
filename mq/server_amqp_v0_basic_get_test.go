package mq

import (
	"testing"
	"time"

	"eventter.io/mq/amqp/v0"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_BasicGet(t *testing.T) {
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
			var response *v0.BasicGetEmpty
			err := client.Call(&v0.BasicGet{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
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

		time.Sleep(1 * time.Second)

		{
			var response *v0.BasicGetOk
			err := client.Call(&v0.BasicGet{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint64(1), response.DeliveryTag)
			assert.Equal("xchng", response.Exchange)
			assert.Equal("", response.RoutingKey)

			var header *v0.ContentHeaderFrame
			err = client.Call(nil, &header)
			assert.NoError(err)

			var body *v0.ContentBodyFrame
			err = client.Call(nil, &body)
			assert.NoError(err)
			assert.Equal("foo", string(body.Data))
		}

		{
			var response *v0.BasicGetOk
			err := client.Call(&v0.BasicGet{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
				NoAck:     true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint64(2), response.DeliveryTag)
			assert.Equal("xchng", response.Exchange)
			assert.Equal("", response.RoutingKey)

			var header *v0.ContentHeaderFrame
			err = client.Call(nil, &header)
			assert.NoError(err)

			var body *v0.ContentBodyFrame
			err = client.Call(nil, &body)
			assert.NoError(err)
			assert.Equal("bar", string(body.Data))
		}

		{
			err := client.Send(&v0.BasicReject{
				FrameMeta:   v0.FrameMeta{Channel: channel},
				DeliveryTag: 1,
				Requeue:     true,
			})
			assert.NoError(err)
		}

		{
			var response *v0.BasicGetOk
			err := client.Call(&v0.BasicGet{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint64(3), response.DeliveryTag)
			assert.Equal("xchng", response.Exchange)
			assert.Equal("", response.RoutingKey)

			var header *v0.ContentHeaderFrame
			err = client.Call(nil, &header)
			assert.NoError(err)

			var body *v0.ContentBodyFrame
			err = client.Call(nil, &body)
			assert.NoError(err)
			assert.Equal("foo", string(body.Data))
		}

		{
			var response *v0.BasicGetOk
			err := client.Call(&v0.BasicGet{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint64(4), response.DeliveryTag)
			assert.Equal("xchng", response.Exchange)
			assert.Equal("", response.RoutingKey)

			var header *v0.ContentHeaderFrame
			err = client.Call(nil, &header)
			assert.NoError(err)

			var body *v0.ContentBodyFrame
			err = client.Call(nil, &body)
			assert.NoError(err)
			assert.Equal("baz", string(body.Data))
		}

		{
			err := client.Send(&v0.BasicAck{
				FrameMeta:   v0.FrameMeta{Channel: channel},
				DeliveryTag: 4,
				Multiple:    true,
			})
			assert.NoError(err)
		}

		{
			var response *v0.BasicGetEmpty
			err := client.Call(&v0.BasicGet{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
		}
	}
}
