package mq

import (
	"context"
	"io"
	"net"
	"testing"

	"eventter.io/mq/amqp"
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

	ctx = amqp.NewContextV0(ctx, nil, 0, "/")

	clientConn, serverConn := net.Pipe()

	go func() {
		defer serverConn.Close()
		err := ts.Server.ServeAMQPv0(ctx, v0.NewTransport(serverConn))
		assert.NoError(err)
	}()

	client := v0.NewTransport(clientConn)

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
