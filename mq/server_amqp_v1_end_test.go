package mq

import (
	"testing"

	"eventter.io/mq/amqp/v1"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv1_End(t *testing.T) {
	assert := require.New(t)

	_, client, cleanup, err := newClientAMQPv1(t)
	assert.NoError(err)
	defer cleanup()

	{
		var response *v1.Begin
		err = client.Call(&v1.Begin{
			RemoteChannel:  v1.RemoteChannelNull,
			NextOutgoingID: v1.TransferNumber(0),
			IncomingWindow: 100,
			OutgoingWindow: 100,
		}, &response)
		assert.NoError(err)
		assert.Equal(uint16(0), response.RemoteChannel)
	}

	{
		var end *v1.End
		err = client.Call(&v1.End{}, &end)
		assert.NoError(err)
		assert.NotNil(end)
	}
}

func TestServer_ServeAMQPv1_End_NoSession(t *testing.T) {
	assert := require.New(t)

	_, client, cleanup, err := newClientAMQPv1(t)
	assert.NoError(err)
	defer cleanup()

	{
		var response *v1.Close
		err = client.Call(&v1.End{}, &response)
		assert.NoError(err)
		assert.NotNil(response)
		assert.NotNil(response.Error)
	}
}
