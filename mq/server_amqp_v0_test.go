package mq

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"eventter.io/mq/amqp/v0"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func newClient(t *testing.T) (x1 *testServer, x2 *v0.Transport, cleanup func(), err error) {
	assert := require.New(t)

	ts, err := newTestServer(0)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		if err != nil {
			ts.Close()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	clientConn, serverConn := net.Pipe()

	go func() {
		defer serverConn.Close()
		err := ts.Server.ServeAMQPv0(ctx, v0.NewTransport(serverConn))
		assert.NoError(err)
	}()

	client := v0.NewTransport(clientConn)

	err = client.Send(&v0.ConnectionOpen{VirtualHost: "/"})
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "send connection.open failed")
	}

	return ts, client, func() {
		cancel()

		response := &v0.ConnectionCloseOk{}
		err := client.Call(&v0.ConnectionClose{}, &response)
		if errors.Cause(err) != io.EOF && errors.Cause(err) != io.ErrClosedPipe {
			assert.NoError(err)
			assert.NotNil(response)
		}

		err = clientConn.Close()
		assert.NoError(err)
	}, nil
}

func TestServer_ServeAMQPv0_QueuePurge(t *testing.T) {
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
			var response *v0.ConnectionClose
			err := client.Call(&v0.QueuePurge{
				FrameMeta: v0.FrameMeta{Channel: channel},
				Queue:     "q",
			}, &response)
			assert.NoError(err)
			assert.NotNil(response)
			assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
		}
	}
}

func TestServer_ServeAMQPv0_BasicRecover(t *testing.T) {
	tests := []struct {
		requeue bool
	}{
		{true},
		{false},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("requeue=%t", test.requeue), func(t *testing.T) {
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
					var response *v0.ConnectionClose
					err := client.Call(&v0.BasicRecover{
						FrameMeta: v0.FrameMeta{Channel: channel},
						Requeue:   test.requeue,
					}, &response)
					assert.NoError(err)
					assert.NotNil(response)
					assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
				}
			}
		})
	}
}

func TestServer_ServeAMQPv0_BasicRecoverAsync(t *testing.T) {
	tests := []struct {
		requeue bool
	}{
		{true},
		{false},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("requeue=%t", test.requeue), func(t *testing.T) {
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
					var response *v0.ConnectionClose
					err := client.Call(&v0.BasicRecoverAsync{
						FrameMeta: v0.FrameMeta{Channel: channel},
						Requeue:   test.requeue,
					}, &response)
					assert.NoError(err)
					assert.NotNil(response)
					assert.Equal(uint16(v0.NotImplemented), response.ReplyCode)
				}
			}
		})
	}
}

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
