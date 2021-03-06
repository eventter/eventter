package mq

import (
	"context"
	"io"
	"net"
	"testing"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func newClientAMQPv1(t *testing.T) (x1 *testServer, x2 *v1.Transport, cleanup func(), err error) {
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
		err := ts.Server.ServeAMQPv1(ctx, v1.NewTransport(serverConn))
		assert.NoError(err)
	}()

	client := v1.NewTransport(clientConn)

	var open *v1.Open
	err = client.Call(&v1.Open{
		ContainerID:  "test-client",
		MaxFrameSize: v1.MinMaxFrameSize,
		IdleTimeOut:  60000,
	}, &open)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "send open failed")
	}

	return ts, client, func() {
		cancel()

		response := &v1.Close{}
		err := client.Call(&v1.Close{}, &response)
		if errors.Cause(err) != io.EOF && errors.Cause(err) != io.ErrClosedPipe {
			assert.NoError(err)
			assert.NotNil(response)
		}

		err = clientConn.Close()
		assert.NoError(err)
	}, nil
}
