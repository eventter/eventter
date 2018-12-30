package v1

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"time"

	"eventter.io/mq/util"
	"github.com/pkg/errors"
)

var (
	endian            = binary.BigEndian
	ErrMalformedFrame = errors.New("malformed frame")
	ErrFrameTooBig    = errors.New("frame size over limit")
)

type Transport struct {
	conn           net.Conn
	rw             io.ReadWriter
	readData       []byte
	writeData      []byte
	frameMax       uint32
	receiveTimeout time.Duration
	sendTimeout    time.Duration
}

func NewTransport(conn net.Conn) *Transport {
	return &Transport{
		conn:      conn,
		rw:        conn,
		readData:  make([]byte, util.NextPowerOfTwo32(MinMaxFrameSize)),
		writeData: make([]byte, util.NextPowerOfTwo32(MinMaxFrameSize)),
		frameMax:  MinMaxFrameSize,
	}
}

func (t *Transport) SetFrameMax(frameMax uint32) {
	t.frameMax = frameMax
}

func (t *Transport) SetBuffered(buffered bool) error {
	if buffered {
		if _, ok := t.rw.(*bufio.ReadWriter); !ok {
			t.rw = bufio.NewReadWriter(bufio.NewReader(t.conn), bufio.NewWriter(t.conn))
		}
		return nil

	} else {
		if bufrw, ok := t.rw.(*bufio.ReadWriter); ok {
			if bufrw.Reader.Buffered() > 0 {
				return errors.New("read buffer not empty")
			}
			if bufrw.Writer.Buffered() > 0 {
				err := bufrw.Flush()
				if err != nil {
					return errors.Wrap(err, "flush write buffer failed")
				}
			}
			t.rw = t.conn
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

	var x [18]byte // 8 bytes frame header + 1 byte descriptor constructor + (at most) 9 bytes ulong

	meta := frame.GetFrameMeta()

	frameBuf := bytes.NewBuffer(t.writeData[:0])
	err = frame.MarshalBuffer(frameBuf)
	if err != nil {
		return errors.Wrap(err, "marshal failed")
	}
	data := frameBuf.Bytes()
	t.writeData = data

	size := uint32(8) + uint32(len(data)) + uint32(len(meta.Payload))
	if size > t.frameMax {
		return ErrFrameTooBig
	}

	endian.PutUint32(x[:4], size)
	x[4] = 8 / 4 // data offset
	switch frame.(type) {
	case AMQPFrame:
		x[5] = 0x00
	case SASLFrame:
		x[5] = 0x01
	default:
		return errors.Errorf("unhandled frame type %T", frame)
	}
	endian.PutUint16(x[6:8], meta.Channel)

	x[8] = 0x00
	buf := bytes.NewBuffer(x[9:9])
	err = marshalUlong(frame.Descriptor(), buf)
	if err != nil {
		return errors.Wrap(err, "marshal descriptor failed")
	}

	_, err = t.rw.Write(x[:9+buf.Len()])
	if err != nil {
		return errors.Wrap(err, "write frame header failed")
	}
	_, err = t.rw.Write(data)
	if err != nil {
		return errors.Wrap(err, "write frame data failed")
	}
	if len(meta.Payload) > 0 {
		_, err = t.rw.Write(meta.Payload)
		if err != nil {
			return errors.Wrap(err, "write frame payload failed")
		}
	}

	if bufrw, ok := t.rw.(*bufio.ReadWriter); ok {
		err = bufrw.Flush()
		if err != nil {
			return errors.Wrap(err, "flush failed")
		}
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

	_, err := io.ReadFull(t.rw, t.readData[:8])
	if err != nil {
		return nil, errors.Wrap(err, "read frame header failed")
	}

	meta := FrameMeta{}
	meta.Size = endian.Uint32(t.readData[:4])
	meta.DataOffset = uint8(t.readData[4])
	meta.Type = uint8(t.readData[5])
	meta.Channel = endian.Uint16(t.readData[6:8])

	if meta.Size > t.frameMax {
		return nil, ErrFrameTooBig
	}

	dataSize := meta.Size - 8

	if uint32(len(t.readData)) < dataSize {
		t.readData = make([]byte, util.NextPowerOfTwo32(dataSize))
	}

	_, err = io.ReadFull(t.rw, t.readData[:dataSize])
	if err != nil {
		return nil, errors.Wrap(err, "read frame content failed")
	}

	switch meta.Type {
	case 0x00: // AMQP frame
		fallthrough
	case 0x01: // SASL frame
		buf := bytes.NewBuffer(t.readData[:dataSize-(4*uint32(meta.DataOffset)-8)])
		b, err := buf.ReadByte()
		if err != nil {
			return nil, errors.Wrap(err, "read descriptor failed")
		}
		if b != 0x00 {
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
