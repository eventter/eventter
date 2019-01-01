package mq

import (
	"context"
	"testing"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv1_Attach_Topic(t *testing.T) {
	assert := require.New(t)

	ts, client, cleanup, err := newClientAMQPv1(t)
	assert.NoError(err)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		_, err := ts.CreateTopic(ctx, &emq.TopicCreateRequest{
			Topic: emq.Topic{
				Namespace:           "default",
				Name:                "my-topic",
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
}

func TestServer_ServeAMQPv1_Attach_TopicByNamespaceProperty(t *testing.T) {
	assert := require.New(t)

	ts, client, cleanup, err := newClientAMQPv1(t)
	assert.NoError(err)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		_, err := ts.CreateNamespace(ctx, &emq.NamespaceCreateRequest{
			Namespace: "my-namespace",
		})
		assert.NoError(err)
	}

	{
		_, err := ts.CreateTopic(ctx, &emq.TopicCreateRequest{
			Topic: emq.Topic{
				Namespace:           "my-namespace",
				Name:                "my-topic",
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
			Name:          "topic-link",
			Handle:        v1.Handle(0),
			Role:          v1.SenderRole,
			SndSettleMode: v1.SenderSettleModeNull,
			RcvSettleMode: v1.ReceiverSettleModeNull,
			Target: &v1.Target{
				Address: v1.AddressString("my-topic"),
			},
			Properties: &v1.Fields{Fields: map[string]*types.Value{
				"namespace": {Kind: &types.Value_StringValue{StringValue: "my-namespace"}},
			}},
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

func TestServer_ServeAMQPv1_Attach_TopicByAbsoluteAddress(t *testing.T) {
	assert := require.New(t)

	ts, client, cleanup, err := newClientAMQPv1(t)
	assert.NoError(err)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		_, err := ts.CreateNamespace(ctx, &emq.NamespaceCreateRequest{
			Namespace: "my-namespace",
		})
		assert.NoError(err)
	}

	{
		_, err := ts.CreateTopic(ctx, &emq.TopicCreateRequest{
			Topic: emq.Topic{
				Namespace:           "my-namespace",
				Name:                "my-topic",
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
			Name:          "topic-link",
			Handle:        v1.Handle(0),
			Role:          v1.SenderRole,
			SndSettleMode: v1.SenderSettleModeNull,
			RcvSettleMode: v1.ReceiverSettleModeNull,
			Target: &v1.Target{
				Address: v1.AddressString("/my-namespace/my-topic"),
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
