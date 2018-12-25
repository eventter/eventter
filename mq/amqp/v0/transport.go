package v0

import (
	"bufio"
	"io"
	"net"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

var ErrFrameTooBig = errors.New("frame size over limit")

type Transport struct {
	conn           net.Conn
	rw             *bufio.ReadWriter
	buf            []byte
	frameMax       uint32
	receiveTimeout time.Duration
	sendTimeout    time.Duration
}

func NewTransport(conn net.Conn) *Transport {
	return NewTransportWithReader(conn, bufio.NewReader(conn))
}

func NewTransportWithReader(conn net.Conn, r *bufio.Reader) *Transport {
	return &Transport{
		conn:     conn,
		rw:       bufio.NewReadWriter(r, bufio.NewWriter(conn)),
		buf:      make([]byte, FrameMinSize),
		frameMax: FrameMinSize,
	}
}

func (t *Transport) SetFrameMax(frameMax uint32) {
	t.frameMax = frameMax
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

func (t *Transport) SendBody(channel uint16, data []byte) error {
	var (
		start  uint32 = 0
		end           = t.frameMax
		length        = uint32(len(data))
	)
	for start < length {
		if end > length {
			end = length
		}
		err := t.Send(&ContentBodyFrame{
			FrameMeta: FrameMeta{
				Channel: channel,
			},
			Data: data[start:end],
		})
		if err != nil {
			return err
		}
		start, end = end, end+t.frameMax
	}
	return nil
}

func (t *Transport) Send(frame Frame) (err error) {
	if t.sendTimeout > 0 {
		err = t.conn.SetWriteDeadline(time.Now().Add(t.sendTimeout))
		if err != nil {
			return errors.Wrap(err, "set write deadline failed")
		}
	}

	var (
		frameType FrameType
		payload   []byte
		x         [12]byte
		end       = 7
	)

	switch frame := frame.(type) {
	case MethodFrame:
		frameType = FrameMethod
		frame.FixMethodMeta()
		mm := frame.GetMethodMeta()
		endian.PutUint16(x[7:9], uint16(mm.ClassID))
		endian.PutUint16(x[9:11], uint16(mm.MethodID))
		end = 11
		payload, err = frame.Marshal()
		if err != nil {
			return errors.Wrap(err, "method frame marshal failed")
		}
	case *ContentHeaderFrame:
		frameType = FrameHeader
		payload, err = frame.Marshal()
		if err != nil {
			return errors.Wrap(err, "content header frame marshal failed")
		}
	case *ContentBodyFrame:
		frameType = FrameBody
		payload = frame.Data
	case *HeartbeatFrame:
		frameType = FrameHeartbeat
		payload = nil
	default:
		return errors.Errorf("unhandled frame type %T", frame)
	}

	if uint32(len(payload)+end+1) > t.frameMax {
		return ErrFrameTooBig
	}

	x[0] = byte(frameType)
	endian.PutUint16(x[1:3], frame.GetFrameMeta().Channel)
	endian.PutUint32(x[3:7], uint32(len(payload)+(end-7)))
	x[end] = FrameEnd

	if len(payload) == 0 {
		_, err = t.rw.Write(x[:end+1])
		if err != nil {
			return errors.Wrap(err, "write frame failed")
		}
	} else {
		_, err = t.rw.Write(x[:end])
		if err != nil {
			return errors.Wrap(err, "write frame header failed")
		}
		_, err = t.rw.Write(payload)
		if err != nil {
			return errors.Wrap(err, "write frame payload failed")
		}
		_, err = t.rw.Write(x[end : end+1])
		if err != nil {
			return errors.Wrap(err, "write frame end failed")
		}
	}

	err = t.rw.Flush()
	if err != nil {
		return errors.Wrap(err, "flush failed")
	}
	return nil
}

func (t *Transport) Receive() (Frame, error) {
	if t.receiveTimeout > 0 {
		err := t.conn.SetReadDeadline(time.Now().Add(t.receiveTimeout))
		if err != nil {
			return nil, errors.Wrap(err, "set read deadline failed")
		}
	}
	if _, err := io.ReadFull(t.rw, t.buf[:7]); err != nil {
		return nil, errors.Wrap(err, "read frame header failed")
	}

	frameType := FrameType(t.buf[0])
	channel := endian.Uint16(t.buf[1:3])
	size := endian.Uint32(t.buf[3:7])

	if size+8 > t.frameMax {
		return nil, ErrFrameTooBig
	}

	if size+1 > uint32(len(t.buf)) {
		t.buf = make([]byte, size+1)
	}

	if _, err := io.ReadFull(t.rw, t.buf[:size+1]); err != nil {
		return nil, errors.Wrap(err, "read frame payload failed")
	}

	if t.buf[size] != FrameEnd {
		return nil, ErrMalformedFrame
	}

	meta := FrameMeta{
		Type:    frameType,
		Channel: channel,
		Size:    size,
	}

	payload := t.buf[:size]
	switch frameType {
	case FrameMethod:
		return decodeMethodFrame(meta, payload)
	case FrameHeader:
		frame := &ContentHeaderFrame{FrameMeta: meta}
		if err := frame.Unmarshal(payload); err != nil {
			return nil, err
		}
		return frame, nil
	case FrameBody:
		return &ContentBodyFrame{FrameMeta: meta, Data: payload}, nil
	case FrameHeartbeat:
		if len(payload) > 0 {
			return nil, ErrMalformedFrame
		}
		return &HeartbeatFrame{FrameMeta: meta}, nil
	default:
		return nil, ErrMalformedFrame
	}
}

func (t *Transport) Call(request MethodFrame, response interface{}) error {
	responseValue := reflect.ValueOf(response).Elem()
	if !responseValue.CanSet() {
		return errors.New("response is not pointer")
	}

	err := t.Send(request)
	if err != nil {
		return errors.Wrap(err, "send failed")
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
