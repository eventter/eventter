package v0

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransport_Send_HeartbeatFrame(t *testing.T) {
	assert := require.New(t)

	a, b := net.Pipe()
	ta := NewTransport(a)
	tb := NewTransport(b)

	go func() {
		err := ta.Send(&HeartbeatFrame{})
		assert.NoError(err)
	}()

	{
		frame, err := tb.Receive()
		assert.NoError(err)
		heartbeat, ok := frame.(*HeartbeatFrame)
		assert.True(ok)
		assert.Equal(uint16(0), heartbeat.FrameMeta.Channel)
	}
}

func TestTransport_Send_MethodFrame(t *testing.T) {
	assert := require.New(t)

	a, b := net.Pipe()
	ta := NewTransport(a)
	tb := NewTransport(b)

	go func() {
		err := ta.Send(&ConnectionStart{
			VersionMajor: Major,
			VersionMinor: Minor,
		})
		assert.NoError(err)
	}()

	{
		frame, err := tb.Receive()
		assert.NoError(err)
		start, ok := frame.(*ConnectionStart)
		assert.True(ok)
		assert.Equal(uint8(Major), start.VersionMajor)
		assert.Equal(uint8(Minor), start.VersionMinor)
	}
}

func TestTransport_Send_ContentHeaderFrame(t *testing.T) {
	assert := require.New(t)

	a, b := net.Pipe()
	ta := NewTransport(a)
	tb := NewTransport(b)

	go func() {
		err := ta.Send(&ContentHeaderFrame{
			FrameMeta:     FrameMeta{Channel: 42},
			ClassID:       BasicClass,
			CorrelationID: "foo",
		})
		assert.NoError(err)
	}()

	{
		frame, err := tb.Receive()
		assert.NoError(err)
		contentHeader, ok := frame.(*ContentHeaderFrame)
		assert.True(ok)
		assert.Equal(uint16(42), contentHeader.FrameMeta.Channel)
		assert.Equal(BasicClass, contentHeader.ClassID)
		assert.Empty(contentHeader.ContentType)
		assert.Equal("foo", contentHeader.CorrelationID)
	}
}

func TestTransport_Send_ContentBodyFrame(t *testing.T) {
	assert := require.New(t)

	a, b := net.Pipe()
	ta := NewTransport(a)
	tb := NewTransport(b)

	go func() {
		err := ta.Send(&ContentBodyFrame{
			FrameMeta: FrameMeta{Channel: 19},
			Data:      []byte("hello, world"),
		})
		assert.NoError(err)
	}()

	{
		frame, err := tb.Receive()
		assert.NoError(err)
		contentBody, ok := frame.(*ContentBodyFrame)
		assert.True(ok)
		assert.Equal(uint16(19), contentBody.FrameMeta.Channel)
		assert.Equal("hello, world", string(contentBody.Data))
	}
}
