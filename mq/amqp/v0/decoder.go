package v0

import (
	"io"

	"github.com/pkg/errors"
)

type Decoder struct {
	reader io.Reader
	buf    []byte
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		reader: r,
		buf:    make([]byte, FrameMinSize),
	}
}

func (d *Decoder) Decode() (Frame, error) {
	if _, err := io.ReadFull(d.reader, d.buf[:7]); err != nil {
		return nil, errors.Wrap(err, "read frame header failed")
	}

	frameType := FrameType(d.buf[0])
	channel := endian.Uint16(d.buf[1:3])
	size := endian.Uint32(d.buf[3:7])

	if size+1 > uint32(len(d.buf)) {
		d.buf = make([]byte, size+1)
	}

	if _, err := io.ReadFull(d.reader, d.buf[:size+1]); err != nil {
		return nil, errors.Wrap(err, "read frame data failed")
	}

	if d.buf[size] != FrameEnd {
		return nil, errors.Errorf("frame-end: expected %x, got %x", FrameEnd, d.buf[size])
	}

	meta := FrameMeta{
		Type:    frameType,
		Channel: channel,
		Size:    size,
	}

	data := d.buf[:size]
	switch frameType {
	case FrameMethod:
		return decodeMethodFrame(meta, data)
	case FrameHeader:
		contentHeader := &ContentHeaderFrame{FrameMeta: meta}
		return contentHeader, contentHeader.Unmarshal(data)
	case FrameBody:
		return &ContentFrame{FrameMeta: meta, Data: data}, nil
	case FrameHeartbeat:
		if len(data) > 0 {
			return nil, errors.New("heartbeat frame with content")
		}
		return &HeartbeatFrame{FrameMeta: meta}, nil
	default:
		return nil, errors.Errorf("unhandled frame type: %d", frameType)
	}
}
