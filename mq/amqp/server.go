package amqp

import (
	"bufio"
	"io"
	"log"
	"math"
	"net"
	"time"

	"eventter.io/mq/amqp/authentication"
	"eventter.io/mq/amqp/v0"
	"github.com/pkg/errors"
)

const (
	defaultConnectTimeout = 10 * time.Second
	defaultHeartbeat      = 10 * time.Second
)

type Server struct {
	// Reported as `product` field in AMQPv0 `connection.start` `server-properties`.
	Name string
	// Reported as `version` field in AMQPv0 `connection.start` `server-properties`.
	Version string
	// Max time to establish connection.
	ConnectTimeout time.Duration
	// Server authentication providers.
	AuthenticationProviders []authentication.Provider
	// Heartbeat the server tries to negotiate with clients.
	Heartbeat time.Duration
	// Will be transformed to map with strings as keys & `true` as values and sent to client as `capabilities`
	// field in `connection.start` `server-properties`.
	CapabilitiesV0 []string
	// Handle AMQPv0 connection.
	HandlerV0 HandlerV0
}

func (s *Server) Serve(l net.Listener) error {
	if s.ConnectTimeout == 0 {
		s.ConnectTimeout = defaultConnectTimeout
	}
	if len(s.AuthenticationProviders) == 0 {
		return errors.New("no authentication provider")
	} else {
		m := make(map[string]bool)
		for _, provider := range s.AuthenticationProviders {
			mechanism := provider.Mechanism()
			if m[mechanism] {
				return errors.Errorf("duplicate authentication providers for mechanism %s", mechanism)
			}
			m[mechanism] = true
		}
	}
	if s.Heartbeat == 0 {
		s.Heartbeat = defaultHeartbeat
	} else if seconds := s.Heartbeat / time.Second; seconds > math.MaxUint16 {
		return errors.Errorf("heartbeat %d out of bounds (%d)", seconds, math.MaxUint16)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			return errors.Wrap(err, "accept failed")
		}
		go func(conn net.Conn) {
			defer conn.Close()
			err := s.Handle(conn)
			if err != nil {
				log.Printf("[AMQP %s] %s", conn.RemoteAddr(), err.Error())
			}
		}(conn)
	}
}

func (s *Server) Handle(conn net.Conn) error {
	err := conn.SetDeadline(time.Now().Add(s.ConnectTimeout))
	if err != nil {
		return errors.Wrap(err, "set deadline failed")
	}

	var x [8]byte
	r := bufio.NewReader(conn)
	_, err = io.ReadFull(r, x[:])
	if err != nil {
		return errors.Wrap(err, "read failed")
	}

	if x[0] == 'A' && x[1] == 'M' && x[2] == 'Q' && x[3] == 'P' && x[4] == 0 &&
		x[5] == v0.Major && x[6] == v0.Minor && x[7] == v0.Revision && s.HandlerV0 != nil {

		transport := v0.NewTransportWithReader(conn, r)
		ctx, err := s.initV0(transport)
		if err != nil {
			return errors.Wrapf(err, "init v%d.%d.%d connection failed", x[5], x[6], x[7])
		}

		return s.HandlerV0.ServeAMQPv0(ctx, transport)

	} else {
		x[0] = 'A'
		x[1] = 'M'
		x[2] = 'Q'
		x[3] = 'P'
		x[4] = 0
		x[5] = v0.Major
		x[6] = v0.Minor
		x[7] = v0.Revision

		_, err := conn.Write(x[:])
		if err != nil {
			return errors.Wrap(err, "write version failed")
		}

		return nil
	}
}
