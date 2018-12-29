package v1

import (
	"bufio"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
)

type Transport struct {
	conn           net.Conn
	rw             io.ReadWriter
	receiveTimeout time.Duration
	sendTimeout    time.Duration
}

func NewTransport(conn net.Conn) *Transport {
	return &Transport{
		conn: conn,
		rw:   conn,
	}
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

func (t *Transport) Receive() error {
	panic("implement me")
}
