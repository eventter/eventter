package v1

import (
	"net"
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestTransport_Send_HeartbeatFrame(t *testing.T) {
	assert := require.New(t)

	a, b := net.Pipe()
	ta := NewTransport(a)
	tb := NewTransport(b)

	go func() {
		err := ta.Send(nil)
		assert.NoError(err)
	}()

	{
		frame, err := tb.Receive()
		assert.NoError(err)
		assert.Nil(frame)
	}
}

func TestTransport_Send_AMQPFrame(t *testing.T) {
	assert := require.New(t)

	a, b := net.Pipe()
	ta := NewTransport(a)
	tb := NewTransport(b)

	go func() {
		err := ta.Send(&Open{
			ContainerID:         "container-id",
			Hostname:            "hostname",
			MaxFrameSize:        128 * 1024,
			ChannelMax:          1024,
			IdleTimeOut:         10000,
			OutgoingLocales:     []IETFLanguageTag{IETFLanguageTag("en_US"), IETFLanguageTag("cs_CZ")},
			IncomingLocales:     []IETFLanguageTag{IETFLanguageTag("cs_CZ"), IETFLanguageTag("en_GB")},
			OfferedCapabilities: []string{"foo", "bar", "baz"},
			DesiredCapabilities: []string{"qux"},
			Properties: &Fields{
				Fields: map[string]*types.Value{
					"answer": {Kind: &types.Value_NumberValue{NumberValue: 42}},
				},
			},
		})
		assert.NoError(err)
	}()

	{
		frame, err := tb.Receive()
		assert.NoError(err)
		open, ok := frame.(*Open)
		assert.True(ok)
		assert.Equal("container-id", open.ContainerID)
		assert.Equal("hostname", open.Hostname)
		assert.Equal(uint32(128*1024), open.MaxFrameSize)
		assert.Equal(uint16(1024), open.ChannelMax)
		assert.Equal(Milliseconds(10000), open.IdleTimeOut)
		assert.Equal([]IETFLanguageTag{IETFLanguageTag("en_US"), IETFLanguageTag("cs_CZ")}, open.OutgoingLocales)
		assert.Equal([]IETFLanguageTag{IETFLanguageTag("cs_CZ"), IETFLanguageTag("en_GB")}, open.IncomingLocales)
		assert.Equal([]string{"foo", "bar", "baz"}, open.OfferedCapabilities)
		assert.Equal([]string{"qux"}, open.DesiredCapabilities)
		assert.NotNil(open.Properties)
		assert.NotNil(open.Properties.Fields)
		assert.Equal(map[string]*types.Value{
			"answer": {Kind: &types.Value_NumberValue{NumberValue: 42}},
		}, open.Properties.Fields)
	}
}

func TestTransport_Send_SASLFrame(t *testing.T) {
	assert := require.New(t)

	a, b := net.Pipe()
	ta := NewTransport(a)
	tb := NewTransport(b)

	go func() {
		err := ta.Send(&SASLMechanisms{
			SASLServerMechanisms: []string{"ANONYMOUS"},
		})
		assert.NoError(err)
	}()

	{
		frame, err := tb.Receive()
		assert.NoError(err)
		mechanisms, ok := frame.(*SASLMechanisms)
		assert.True(ok)
		assert.Equal([]string{"ANONYMOUS"}, mechanisms.SASLServerMechanisms)
	}
}
