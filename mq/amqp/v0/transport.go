package v0

import (
	"bufio"
	"io"

	"github.com/pkg/errors"
)

type Transport struct {
	rw  *bufio.ReadWriter
	buf []byte
}

func NewTransport(rw *bufio.ReadWriter) *Transport {
	return &Transport{
		rw:  rw,
		buf: make([]byte, FrameMinSize),
	}
}

func (t *Transport) Encode(frame Frame) (err error) {
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

	x[0] = byte(frameType)
	endian.PutUint16(x[1:3], frame.GetFrameMeta().Channel)
	endian.PutUint32(x[3:7], uint32(len(payload)))
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

func (t *Transport) Decode() (Frame, error) {
	if _, err := io.ReadFull(t.rw, t.buf[:7]); err != nil {
		return nil, errors.Wrap(err, "read frame header failed")
	}

	frameType := FrameType(t.buf[0])
	channel := endian.Uint16(t.buf[1:3])
	size := endian.Uint32(t.buf[3:7])

	if size+1 > uint32(len(t.buf)) {
		t.buf = make([]byte, size+1)
	}

	if _, err := io.ReadFull(t.rw, t.buf[:size+1]); err != nil {
		return nil, errors.Wrap(err, "read frame payload failed")
	}

	if t.buf[size] != FrameEnd {
		return nil, errors.Errorf("frame-end: expected %x, got %x", FrameEnd, t.buf[size])
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
			return nil, errors.New("heartbeat frame with content")
		}
		return &HeartbeatFrame{FrameMeta: meta}, nil
	default:
		return nil, errors.Errorf("unhandled frame type: %d", frameType)
	}
}
