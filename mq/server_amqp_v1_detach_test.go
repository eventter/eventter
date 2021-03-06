package mq

import (
	"context"
	"testing"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv1_Detach(t *testing.T) {
	assert := require.New(t)

	ts, client, cleanup, err := newClientAMQPv1(t)
	assert.NoError(err)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		_, err := ts.CreateTopic(ctx, &emq.TopicCreateRequest{
			Topic: emq.Topic{
				Namespace: "default",
				Name:      "my-topic",

				DefaultExchangeType: emq.ExchangeTypeFanout,
			},
		})
		assert.NoError(err)
	}

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

	{
		var response *v1.Detach
		err = client.Call(&v1.Detach{
			Handle: v1.Handle(0),
			Closed: true,
		}, &response)
		assert.NoError(err)
		assert.NotNil(response)
		assert.Equal(v1.Handle(0), response.Handle)
		assert.True(response.Closed)
		assert.Nil(response.Error)
	}
}
