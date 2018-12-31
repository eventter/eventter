package mq

import (
	"testing"

	"eventter.io/mq/amqp/v0"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_BasicQos(t *testing.T) {
	assert := require.New(t)

	_, client, cleanup, err := newClientAMQPv0(t)
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
			var response *v0.BasicQosOk
			err := client.Call(&v0.BasicQos{
				FrameMeta:     v0.FrameMeta{Channel: channel},
				PrefetchCount: 10,
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
		}
	}
}
