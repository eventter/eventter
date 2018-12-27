package mq

import (
	"crypto/sha1"
	"strconv"
	"testing"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/segments"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_BasicPublish(t *testing.T) {
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
				Exchange:  "test-basic-publish",
				Type:      "fanout",
				Durable:   true,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
		}

		for i := 1; i <= 10; i++ {
			err := client.Send(&v0.BasicPublish{
				FrameMeta:  v0.FrameMeta{Channel: channel},
				Exchange:   "test-basic-publish",
				RoutingKey: strconv.Itoa(i),
			})
			assert.NoError(err)

			data := []byte(strconv.Itoa(i))

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
			var response *v0.ChannelCloseOk
			err := client.Call(&v0.ChannelClose{FrameMeta: v0.FrameMeta{Channel: channel}}, &response)
			assert.NoError(err)
			assert.NotNil(response)

			segs := ts.ClusterStateStore.Current().FindOpenSegmentsFor(ClusterSegment_TOPIC, "default", "test-basic-publish")
			assert.Len(segs, 1)

			handle, err := ts.Dir.Open(segs[0].ID)
			assert.NoError(err)
			defer ts.Dir.Release(handle)

			_, size, err := handle.Sum(sha1.New(), segments.SumAll)
			assert.NoError(err)
			assert.Condition(func() (success bool) {
				return size > 0
			})
		}
	}
}
