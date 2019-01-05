package v1

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"reflect"
	"time"

	"eventter.io/mq/util"
	"github.com/pkg/errors"
)

const (
	AMQPFrameType = 0x00
	SASLFrameType = 0x01
)

var (
	endian            = binary.BigEndian
	ErrMalformedFrame = errors.New("malformed frame")
	ErrFrameTooBig    = errors.New("frame size over limit")
)

type Transport struct {
	conn           net.Conn
	r              io.Reader
	w              *bufio.Writer
	data           []byte
	buf            bytes.Buffer
	scratch        [8]byte // 8 bytes for frame header
	frameMax       uint32
	receiveTimeout time.Duration
	sendTimeout    time.Duration
}

func NewTransport(conn net.Conn) *Transport {
	return &Transport{
		conn:     conn,
		r:        conn,
		w:        bufio.NewWriter(conn),
		data:     make([]byte, util.NextPowerOfTwo32(MinMaxFrameSize)),
		frameMax: MinMaxFrameSize,
	}
}

func (t *Transport) GetFrameMax() uint32 {
	return t.frameMax
}

func (t *Transport) SetFrameMax(frameMax uint32) {
	t.frameMax = frameMax
}

func (t *Transport) SetBuffered(buffered bool) error {
	if buffered {
		if _, ok := t.r.(*bufio.Reader); !ok {
			t.r = bufio.NewReader(t.conn)
		}
		return nil

	} else {
		if bufr, ok := t.r.(*bufio.Reader); ok {
			if bufr.Buffered() > 0 {
				return errors.New("read buffer not empty")
			}
			t.r = t.conn
		}
		return nil
	}
}

func (t *Transport) SetReceiveTimeout(receiveTimeout time.Duration) error {
	t.receiveTimeout = receiveTimeout
	if t.receiveTimeout == 0 {
		err := t.conn.SetReadDeadline(time.Time{})
		if err != nil {
			return errors.Wrap(err, "set read deadline failed")
		}
	}
	return nil
}

func (t *Transport) SetSendTimeout(sendTimeout time.Duration) error {
	t.sendTimeout = sendTimeout
	if t.sendTimeout == 0 {
		err := t.conn.SetWriteDeadline(time.Time{})
		if err != nil {
			return errors.Wrap(err, "set write deadline failed")
		}
	}
	return nil
}

func (t *Transport) Send(frame Frame) (err error) {
	if t.sendTimeout != 0 {
		err = t.conn.SetWriteDeadline(time.Now().Add(t.sendTimeout))
		if err != nil {
			return errors.Wrap(err, "set write deadline failed")
		}
	}

	var data []byte
	var meta *FrameMeta
	if frame != nil {
		meta = frame.GetFrameMeta()
		t.buf.Reset()
		err = frame.MarshalBuffer(&t.buf)
		if err != nil {
			return errors.Wrap(err, "marshal failed")
		}
		data = t.buf.Bytes()
	}

	size := uint32(8) + uint32(len(data))
	if meta != nil {
		size += uint32(len(meta.Payload))
	}
	if size > t.frameMax {
		return ErrFrameTooBig
	}

	endian.PutUint32(t.scratch[:4], size)
	t.scratch[4] = 8 / 4 // data offset

	if frame == nil {
		t.scratch[5] = AMQPFrameType
		t.scratch[6] = 0
		t.scratch[7] = 0
	} else {
		switch frame.(type) {
		case AMQPFrame:
			t.scratch[5] = AMQPFrameType
		case SASLFrame:
			t.scratch[5] = SASLFrameType
		default:
			return errors.Errorf("unhandled frame type %T", frame)
		}
		endian.PutUint16(t.scratch[6:8], meta.Channel)
	}

	_, err = t.w.Write(t.scratch[:])
	if err != nil {
		return errors.Wrap(err, "write frame header failed")
	}
	if len(data) > 0 {
		_, err = t.w.Write(data)
		if err != nil {
			return errors.Wrap(err, "write frame data failed")
		}
	}
	if meta != nil && len(meta.Payload) > 0 {
		_, err = t.w.Write(meta.Payload)
		if err != nil {
			return errors.Wrap(err, "write frame payload failed")
		}
	}
	err = t.w.Flush()
	if err != nil {
		return errors.Wrap(err, "flush failed")
	}

	return nil
}

func (t *Transport) Receive() (Frame, error) {
	if t.receiveTimeout != 0 {
		err := t.conn.SetReadDeadline(time.Now().Add(t.receiveTimeout))
		if err != nil {
			return nil, errors.Wrap(err, "set read deadline failed")
		}
	}

	_, err := io.ReadFull(t.r, t.data[:8])
	if err != nil {
		return nil, errors.Wrap(err, "read frame header failed")
	}

	meta := FrameMeta{}
	meta.Size = endian.Uint32(t.data[:4])
	meta.DataOffset = uint8(t.data[4])
	meta.Type = uint8(t.data[5])
	meta.Channel = endian.Uint16(t.data[6:8])

	if meta.Size > t.frameMax {
		return nil, ErrFrameTooBig
	}

	dataSize := meta.Size - 8

	if uint32(len(t.data)) < dataSize {
		t.data = make([]byte, util.NextPowerOfTwo32(dataSize))
	}

	if dataSize == 0 {
		switch meta.Type {
		case AMQPFrameType: // AMQP heartbeat frame
			return nil, nil
		default:
			return nil, ErrMalformedFrame
		}
	}

	_, err = io.ReadFull(t.r, t.data[:dataSize])
	if err != nil {
		return nil, errors.Wrap(err, "read frame content failed")
	}

	switch meta.Type {
	case AMQPFrameType: // AMQP frame
		fallthrough
	case SASLFrameType: // SASL frame
		buf := bytes.NewBuffer(t.data[:dataSize-(4*uint32(meta.DataOffset)-8)])
		b, err := buf.ReadByte()
		if err != nil {
			return nil, errors.Wrap(err, "read descriptor failed")
		}
		if b != DescriptorEncoding {
			return nil, ErrMalformedFrame
		}
		b, err = buf.ReadByte()
		if err != nil {
			return nil, errors.Wrap(err, "read descriptor failed")
		}
		var descriptor uint64
		err = unmarshalUlong(&descriptor, b, buf)
		if err != nil {
			return nil, errors.Wrap(err, "read descriptor failed")
		}

		// reset buffer
		buf.Reset()
		buf.Write(t.data[:dataSize-(4*uint32(meta.DataOffset)-8)])

		var frame Frame
		switch descriptor {
		case OpenDescriptor:
			frame = &Open{FrameMeta: meta}
		case BeginDescriptor:
			frame = &Begin{FrameMeta: meta}
		case AttachDescriptor:
			frame = &Attach{FrameMeta: meta}
		case FlowDescriptor:
			frame = &Flow{FrameMeta: meta}
		case TransferDescriptor:
			frame = &Transfer{FrameMeta: meta}
		case DispositionDescriptor:
			frame = &Disposition{FrameMeta: meta}
		case DetachDescriptor:
			frame = &Detach{FrameMeta: meta}
		case EndDescriptor:
			frame = &End{FrameMeta: meta}
		case CloseDescriptor:
			frame = &Close{FrameMeta: meta}
		case SASLMechanismsDescriptor:
			frame = &SASLMechanisms{FrameMeta: meta}
		case SASLInitDescriptor:
			frame = &SASLInit{FrameMeta: meta}
		case SASLChallengeDescriptor:
			frame = &SASLChallenge{FrameMeta: meta}
		case SASLResponseDescriptor:
			frame = &SASLResponse{FrameMeta: meta}
		case SASLOutcomeDescriptor:
			frame = &SASLOutcome{FrameMeta: meta}
		default:
			return nil, ErrMalformedFrame
		}

		err = frame.UnmarshalBuffer(buf)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal failed")
		}

		if buf.Len() > 0 {
			frame.GetFrameMeta().Payload = make([]byte, buf.Len())
			copy(frame.GetFrameMeta().Payload, buf.Bytes())
		}

		return frame, nil
	default:
		return nil, ErrMalformedFrame
	}
}

func (t *Transport) Call(request Frame, response interface{}) error {
	err := t.Send(request)
	if err != nil {
		return errors.Wrap(err, "send failed")
	}
	return t.Expect(response)
}

func (t *Transport) Expect(response interface{}) error {
	responseValue := reflect.ValueOf(response).Elem()
	if !responseValue.CanSet() {
		return errors.New("response is not pointer")
	}

	frame, err := t.Receive()
	if err != nil {
		return errors.Wrap(err, "receive failed")
	}

	frameValue := reflect.ValueOf(frame)

	if !frameValue.Type().AssignableTo(responseValue.Type()) {
		return errors.Errorf("expected frame of type %s, got %#v", responseValue.Type(), frame)
	}

	responseValue.Set(frameValue)

	return nil
}
