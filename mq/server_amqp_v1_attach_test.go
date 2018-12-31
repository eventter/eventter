package mq

import (
	"testing"

	"eventter.io/mq/amqp/v1"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv1_Attach_Topic(t *testing.T) {
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
		assert.Equal(uint16(0), response.RemoteChannel) // server might not, but for now uses that same channel
	}

	{
		request := &v1.Attach{
			Name:   "topic-link",
			Handle: v1.Handle(0),
			Role:   v1.SenderRole,
			Target: &v1.Target{
				Address: v1.AddressString("my-topic"),
			},
		}
		var response *v1.Attach
		err = client.Call(request, &response)
		assert.NoError(err)
		assert.Equal(request.Name, response.Name)
		assert.Equal(request.Handle, response.Handle)

		var flow *v1.Flow
		err = client.Expect(&flow)
		assert.NoError(err)
		assert.Equal(request.Handle, flow.Handle)
		assert.Condition(func() bool { return flow.LinkCredit > 0 })
	}
}
