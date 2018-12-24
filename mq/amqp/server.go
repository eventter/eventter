package amqp

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"eventter.io/mq/amqp/v0"
	"github.com/pkg/errors"
)

const (
	defaultConnectionTimeout = 10 * time.Second
)

type Server struct {
	ConnectTimeout time.Duration // Time to establish connection.
}

func (s *Server) Serve(l net.Listener) error {
	if s.ConnectTimeout == 0 {
		s.ConnectTimeout = defaultConnectionTimeout
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			return errors.Wrap(err, "accept failed")
		}
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	var x [8]byte

	r := bufio.NewReader(conn)

	if err := conn.SetDeadline(time.Now().Add(s.ConnectTimeout)); err != nil {
		log.Printf("deadline failed: %v", err)
		goto CLOSE
	}

	if _, err := io.ReadFull(r, x[:]); err != nil {
		log.Printf("read failed: %v", err)
		goto CLOSE
	}
	if x[0] != 'A' || x[1] != 'M' || x[2] != 'Q' || x[3] != 'P' || x[4] != 0 {
		log.Printf("invalid protocol %q", fmt.Sprintf("%c%c%c%c", x[0], x[1], x[2], x[3]))
		goto CLOSE
	}

	if x[5] == v0.Major && x[6] == v0.Minor && x[7] == v0.Revision {
		s.handleV0(conn, r)
	} else {
		goto CLOSE
	}

	return

CLOSE:
	x[0] = 'A'
	x[1] = 'M'
	x[2] = 'Q'
	x[3] = 'P'
	x[4] = 0
	x[5] = v0.Major
	x[6] = v0.Minor
	x[7] = v0.Revision

	if _, err := conn.Write(x[:]); err != nil {
		log.Printf("write close failed: %v", err)
	}
	if err := conn.Close(); err != nil {
		log.Printf("close failed")
	}
}

func (s *Server) handleV0(conn net.Conn, r *bufio.Reader) {
	rw := bufio.NewReadWriter(r, bufio.NewWriter(conn))
	transport := v0.NewTransport(rw)

	// TODO

	_ = transport
}
