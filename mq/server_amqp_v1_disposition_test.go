package mq

import (
	"bytes"
	"context"
	"testing"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"github.com/stretchr/testify/require"
)

func TestServer_ServeAMQPv1_Disposition_Accepted(t *testing.T) {
	assert := require.New(t)

	ts, client, cleanup, err := newClientAMQPv1(t)
	assert.NoError(err)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		_, err = ts.CreateTopic(ctx, &emq.TopicCreateRequest{
			Topic: emq.Topic{
				Namespace:           "default",
				Name:                "my-topic",
				DefaultExchangeType: emq.ExchangeTypeFanout,
			},
		})
		assert.NoError(err)
	}

	{
		_, err = ts.CreateConsumerGroup(ctx, &emq.ConsumerGroupCreateRequest{
			ConsumerGroup: emq.ConsumerGroup{
				Namespace: "default",
				Name:      "my-cg",
				Bindings: []*emq.ConsumerGroup_Binding{
					{TopicName: "my-topic", ExchangeType: emq.ExchangeTypeFanout},
				},
			},
		})
		assert.NoError(err)
		ts.WaitForConsumerGroup(t, ctx, "default", "my-cg")
	}

	{
		_, err = ts.Publish(ctx, &emq.TopicPublishRequest{
			Namespace: "default",
			Name:      "my-topic",
			Message: &emq.Message{
				RoutingKey: "foo",
				Data:       []byte("hello, world"),
			},
		})
		assert.NoError(err)
		ts.WaitForMessage(t, ctx, "default", "my-cg")
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
			Name:   "consumer-group-link",
			Handle: v1.Handle(0),
			Role:   v1.ReceiverRole,
			Source: &v1.Source{
				Address: v1.AddressString("my-cg"),
			},
		}
		var response *v1.Attach
		err = client.Call(request, &response)
		assert.NoError(err)
		assert.Equal(request.Name, response.Name)
		assert.Equal(request.Handle, response.Handle)

		err = client.Send(&v1.Flow{
			NextIncomingID: 0,
			IncomingWindow: 100,
			NextOutgoingID: 0,
			OutgoingWindow: 100,
			Handle:         v1.Handle(0),
			DeliveryCount:  response.InitialDeliveryCount,
			LinkCredit:     10,
		})
		assert.NoError(err)
	}

	{
		var transfer *v1.Transfer
		err = client.Expect(&transfer)
		assert.NoError(err)
		assert.NotNil(transfer)
		assert.NotNil(transfer.FrameMeta.Payload)
		assert.False(transfer.Settled)

		buf := bytes.NewBuffer(transfer.FrameMeta.Payload)
		var section v1.Section

		assert.NoError(v1.UnmarshalSection(&section, buf))
		properties, ok := section.(*v1.Properties)
		assert.True(ok)
		assert.Equal("foo", properties.Subject)

		assert.NoError(v1.UnmarshalSection(&section, buf))
		data, ok := section.(v1.Data)
		assert.Equal(v1.Data("hello, world"), data)

		assert.Equal(0, buf.Len())

		err = client.Send(&v1.Disposition{
			Role:    v1.ReceiverRole,
			First:   transfer.DeliveryID,
			Settled: true,
			State:   &v1.Accepted{},
		})
		assert.NoError(err)
	}

	{
		var response *v1.End
		err = client.Call(&v1.End{}, &response)
		assert.NoError(err)
		assert.NotNil(response)
		assert.Nil(response.Error)
	}
}

func TestServer_ServeAMQPv1_Disposition_Released(t *testing.T) {
	assert := require.New(t)

	ts, client, cleanup, err := newClientAMQPv1(t)
	assert.NoError(err)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		_, err = ts.CreateTopic(ctx, &emq.TopicCreateRequest{
			Topic: emq.Topic{
				Namespace:           "default",
				Name:                "my-topic",
				DefaultExchangeType: emq.ExchangeTypeFanout,
			},
		})
		assert.NoError(err)
	}

	{
		_, err = ts.CreateConsumerGroup(ctx, &emq.ConsumerGroupCreateRequest{
			ConsumerGroup: emq.ConsumerGroup{
				Namespace: "default",
				Name:      "my-cg",
				Bindings: []*emq.ConsumerGroup_Binding{
					{TopicName: "my-topic", ExchangeType: emq.ExchangeTypeFanout},
				},
			},
		})
		assert.NoError(err)
		ts.WaitForConsumerGroup(t, ctx, "default", "my-cg")
	}

	{
		_, err = ts.Publish(ctx, &emq.TopicPublishRequest{
			Namespace: "default",
			Name:      "my-topic",
			Message: &emq.Message{
				RoutingKey: "foo",
				Data:       []byte("hello, world"),
			},
		})
		assert.NoError(err)
		ts.WaitForMessage(t, ctx, "default", "my-cg")
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
			Name:   "consumer-group-link",
			Handle: v1.Handle(0),
			Role:   v1.ReceiverRole,
			Source: &v1.Source{
				Address: v1.AddressString("my-cg"),
			},
		}
		var response *v1.Attach
		err = client.Call(request, &response)
		assert.NoError(err)
		assert.Equal(request.Name, response.Name)
		assert.Equal(request.Handle, response.Handle)

		err = client.Send(&v1.Flow{
			NextIncomingID: 0,
			IncomingWindow: 100,
			NextOutgoingID: 0,
			OutgoingWindow: 100,
			Handle:         v1.Handle(0),
			DeliveryCount:  response.InitialDeliveryCount,
			LinkCredit:     10,
		})
		assert.NoError(err)
	}

	var firstDeliveryID v1.DeliveryNumber
	{ // first delivery
		var transfer *v1.Transfer
		err = client.Expect(&transfer)
		assert.NoError(err)
		assert.NotNil(transfer)
		assert.NotNil(transfer.FrameMeta.Payload)
		assert.False(transfer.Settled)

		firstDeliveryID = transfer.DeliveryID

		buf := bytes.NewBuffer(transfer.FrameMeta.Payload)
		var section v1.Section

		assert.NoError(v1.UnmarshalSection(&section, buf))
		properties, ok := section.(*v1.Properties)
		assert.True(ok)
		assert.Equal("foo", properties.Subject)

		assert.NoError(v1.UnmarshalSection(&section, buf))
		data, ok := section.(v1.Data)
		assert.Equal(v1.Data("hello, world"), data)

		assert.Equal(0, buf.Len())

		err = client.Send(&v1.Disposition{
			Role:    v1.ReceiverRole,
			First:   transfer.DeliveryID,
			Settled: true,
			State:   &v1.Released{},
		})
		assert.NoError(err)
	}

	{ // second delivery
		var transfer *v1.Transfer
		err = client.Expect(&transfer)
		assert.NoError(err)
		assert.NotNil(transfer)
		assert.NotNil(transfer.FrameMeta.Payload)
		assert.False(transfer.Settled)
		assert.NotEqual(firstDeliveryID, transfer.DeliveryID)

		buf := bytes.NewBuffer(transfer.FrameMeta.Payload)
		var section v1.Section

		assert.NoError(v1.UnmarshalSection(&section, buf))
		properties, ok := section.(*v1.Properties)
		assert.True(ok)
		assert.Equal("foo", properties.Subject)

		assert.NoError(v1.UnmarshalSection(&section, buf))
		data, ok := section.(v1.Data)
		assert.Equal(v1.Data("hello, world"), data)

		assert.Equal(0, buf.Len())

		err = client.Send(&v1.Disposition{
			Role:    v1.ReceiverRole,
			First:   transfer.DeliveryID,
			Settled: true,
			State:   &v1.Accepted{},
		})
		assert.NoError(err)
	}

	{
		var response *v1.End
		err = client.Call(&v1.End{}, &response)
		assert.NoError(err)
		assert.NotNil(response)
		assert.Nil(response.Error)
	}
}
