package amqp

import (
	"context"
	"net"
	"sync"
	"testing"

	"eventter.io/mq/amqp/sasl"
	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/amqp/v1"
	"github.com/stretchr/testify/require"
)

type nilHandlerV0 struct{}

func (*nilHandlerV0) ServeAMQPv0(ctx context.Context, transport *v0.Transport) error {
	return nil
}

type nilHandlerV1 struct{}

func (*nilHandlerV1) ServeAMQPv1(ctx context.Context, transport *v1.Transport) error {
	return nil
}

func TestServer_Init_NoHandler(t *testing.T) {
	assert := require.New(t)

	s := &Server{}
	err := s.init()
	assert.Error(err)
	assert.Equal("no handler", err.Error())
}

func TestServer_Init_NoSASLButRequired(t *testing.T) {
	assert := require.New(t)

	s := &Server{
		SASLRequired: true,
		HandlerV1:    &nilHandlerV1{},
	}
	err := s.init()
	assert.Error(err)
	assert.Equal("no SASL provider", err.Error())
}

func TestServer_Init_NoSASLButHandlerV0(t *testing.T) {
	assert := require.New(t)

	s := &Server{
		HandlerV0: &nilHandlerV0{},
	}
	err := s.init()
	assert.Error(err)
	assert.Equal("no SASL provider", err.Error())
}

func TestServer_Init_MultipleSASLProviders(t *testing.T) {
	assert := require.New(t)

	s := &Server{
		SASLRequired: true,
		SASLProviders: []sasl.Provider{
			sasl.NewPLAIN(nil),
			sasl.NewPLAIN(nil),
		},
		HandlerV0: &nilHandlerV0{},
	}
	err := s.init()
	assert.Error(err)
	assert.Equal("multiple SASL providers for mechanism PLAIN", err.Error())
}

func TestServer_Handle_Negotiation(t *testing.T) {
	tests := []struct {
		name     string
		server   *Server
		request  string
		response string
	}{
		{
			name: "respond with AMQP 0.9.1 for bad protocol if only V0 handler",
			server: &Server{
				SASLProviders: []sasl.Provider{sasl.NewANONYMOUS()},
				HandlerV0:     &nilHandlerV0{},
			},
			request:  "HTTP1234",
			response: "AMQP\x00\x00\x09\x01",
		},
		{
			name: "respond with AMQP 1.0.0 for bad protocol if only V1 handler",
			server: &Server{
				SASLProviders: []sasl.Provider{sasl.NewANONYMOUS()},
				HandlerV1:     &nilHandlerV1{},
			},
			request:  "HTTP1234",
			response: "AMQP\x00\x01\x00\x00",
		},
		{
			name: "respond with SASL AMQP 1.0.0 for bad protocol if only V1 handler and SASL required",
			server: &Server{
				SASLProviders: []sasl.Provider{sasl.NewANONYMOUS()},
				SASLRequired:  true,
				HandlerV1:     &nilHandlerV1{},
			},
			request:  "HTTP1234",
			response: "AMQP\x03\x01\x00\x00",
		},
		{
			name: "respond with AMQP 1.0.0 for bad protocol if both V0 and V1 handlers",
			server: &Server{
				SASLProviders: []sasl.Provider{sasl.NewANONYMOUS()},
				HandlerV0:     &nilHandlerV0{},
				HandlerV1:     &nilHandlerV1{},
			},
			request:  "HTTP1234",
			response: "AMQP\x00\x01\x00\x00",
		},
		{
			name: "upgrade to AMQP 0.9.1 for older versions if only V0 handler",
			server: &Server{
				SASLProviders: []sasl.Provider{sasl.NewANONYMOUS()},
				HandlerV0:     &nilHandlerV0{},
			},
			request:  "AMQP\x00\x00\x08\x00",
			response: "AMQP\x00\x00\x09\x01",
		},
		{
			name: "upgrade to AMQP 1.0 for older versions if only V1 handler",
			server: &Server{
				SASLProviders: []sasl.Provider{sasl.NewANONYMOUS()},
				HandlerV1:     &nilHandlerV1{},
			},
			request:  "AMQP\x00\x00\x08\x00",
			response: "AMQP\x00\x01\x00\x00",
		},
		{
			name: "upgrade to AMQP 1.0 for older versions if both V0 and V1 handlers",
			server: &Server{
				SASLProviders: []sasl.Provider{sasl.NewANONYMOUS()},
				HandlerV0:     &nilHandlerV0{},
				HandlerV1:     &nilHandlerV1{},
			},
			request:  "AMQP\x00\x00\x08\x00",
			response: "AMQP\x00\x01\x00\x00",
		},
		{
			name: "downgrade to AMQP 0.9.1 if only V0 handler",
			server: &Server{
				SASLProviders: []sasl.Provider{sasl.NewANONYMOUS()},
				HandlerV0:     &nilHandlerV0{},
			},
			request:  "AMQP\x00\x01\x00\x00",
			response: "AMQP\x00\x00\x09\x01",
		},
		{
			name: "upgrade to AMQP 1.0 if only V1 handler",
			server: &Server{
				SASLProviders: []sasl.Provider{sasl.NewANONYMOUS()},
				HandlerV1:     &nilHandlerV1{},
			},
			request:  "AMQP\x00\x00\x09\x01",
			response: "AMQP\x00\x01\x00\x00",
		},
		{
			name: "upgrade to SASL AMQP 1.0 if SASL required",
			server: &Server{
				SASLProviders: []sasl.Provider{sasl.NewANONYMOUS()},
				SASLRequired:  true,
				HandlerV1:     &nilHandlerV1{},
			},
			request:  "AMQP\x00\x01\x00\x00",
			response: "AMQP\x03\x01\x00\x00",
		},
		{
			name: "downgrade from SASL AMQP 1.0 to plain AMQP 1.0 if no SASL provider",
			server: &Server{
				HandlerV1: &nilHandlerV1{},
			},
			request:  "AMQP\x03\x01\x00\x00",
			response: "AMQP\x00\x01\x00\x00",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := require.New(t)
			assert.Len(test.request, 8)
			assert.Len(test.response, 8)

			err := test.server.init()
			assert.NoError(err)

			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()

			var handleErr error
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer serverConn.Close()
				handleErr = test.server.handle(serverConn)
			}()

			_, err = clientConn.Write([]byte(test.request))
			assert.NoError(err)

			var x [8]byte

			n, err := clientConn.Read(x[:])
			assert.NoError(err)
			assert.Equal(8, n)
			assert.Equal([]byte(test.response), x[:])

			wg.Wait()

			assert.NoError(handleErr)
		})
	}
}
