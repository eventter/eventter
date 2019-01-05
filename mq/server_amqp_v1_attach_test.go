package mq

import (
	"bytes"
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

func TestServer_ServeAMQPv1_Attach_ConsumerGroup(t *testing.T) {
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
			LinkCredit:     1,
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
	}
}

func TestServer_ServeAMQPv1_Attach_ConsumerGroupByNamespaceProperty(t *testing.T) {
	assert := require.New(t)

	ts, client, cleanup, err := newClientAMQPv1(t)
	assert.NoError(err)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		_, err = ts.CreateNamespace(ctx, &emq.NamespaceCreateRequest{
			Namespace: "my-namespace",
		})
		assert.NoError(err)
	}

	{
		_, err = ts.CreateConsumerGroup(ctx, &emq.ConsumerGroupCreateRequest{
			ConsumerGroup: emq.ConsumerGroup{
				Namespace: "my-namespace",
				Name:      "my-cg",
			},
		})
		assert.NoError(err)
		ts.WaitForConsumerGroup(t, ctx, "my-namespace", "my-cg")
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
			Properties: &v1.Fields{Fields: map[string]*types.Value{
				"namespace": {Kind: &types.Value_StringValue{StringValue: "my-namespace"}},
			}},
		}
		var response *v1.Attach
		err = client.Call(request, &response)
		assert.NoError(err)
		assert.Equal(request.Name, response.Name)
		assert.Equal(request.Handle, response.Handle)
	}
}

func TestServer_ServeAMQPv1_Attach_ConsumerGroupByAbsoluteAddress(t *testing.T) {
	assert := require.New(t)

	ts, client, cleanup, err := newClientAMQPv1(t)
	assert.NoError(err)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	{
		_, err = ts.CreateNamespace(ctx, &emq.NamespaceCreateRequest{
			Namespace: "my-namespace",
		})
		assert.NoError(err)
	}

	{
		_, err = ts.CreateConsumerGroup(ctx, &emq.ConsumerGroupCreateRequest{
			ConsumerGroup: emq.ConsumerGroup{
				Namespace: "my-namespace",
				Name:      "my-cg",
			},
		})
		assert.NoError(err)
		ts.WaitForConsumerGroup(t, ctx, "my-namespace", "my-cg")
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
				Address: v1.AddressString("/my-namespace/my-cg"),
			},
		}
		var response *v1.Attach
		err = client.Call(request, &response)
		assert.NoError(err)
		assert.Equal(request.Name, response.Name)
		assert.Equal(request.Handle, response.Handle)
	}
}
