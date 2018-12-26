package mq

import (
	"testing"

	"eventter.io/mq/amqp/v0"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv0_TxSelect(t *testing.T) {
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
			var response *v0.ConnectionClose
			err := client.Call(&v0.TxSelect{
				FrameMeta: v0.FrameMeta{Channel: channel},
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
		}
	}
}

func TestServer_ServeAMQPv0_TxCommit(t *testing.T) {
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
			var response *v0.ConnectionClose
			err := client.Call(&v0.TxCommit{
				FrameMeta: v0.FrameMeta{Channel: channel},
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
		}
	}
}

func TestServer_ServeAMQPv0_TxRollback(t *testing.T) {
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
			var response *v0.ConnectionClose
			err := client.Call(&v0.TxRollback{
				FrameMeta: v0.FrameMeta{Channel: channel},
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
		}
	}
}
