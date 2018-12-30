package amqp

import (
	"context"
	"io"
	"log"
	"math"
	"net"
	"time"

	"eventter.io/mq/amqp/sasl"
	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/amqp/v1"
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
	SASLProviders []sasl.Provider
	// Require SASL authentication.
	SASLRequired bool
	// Heartbeat the server tries to negotiate with clients.
	Heartbeat time.Duration
	// Will be transformed to map with strings as keys & `true` as values and sent to client as `capabilities`
	// field in `connection.start` `server-properties`.
	CapabilitiesV0 []string
	// Handle AMQPv0 connection.
	HandlerV0 HandlerV0
	// Handle AMQPv1 connection.
	HandlerV1 HandlerV1

	listener net.Listener
	ctx      context.Context
	cancel   func()
}

func (s *Server) init() error {
	if s.HandlerV0 == nil && s.HandlerV1 == nil {
		return errors.New("no handler")
	}

	if s.ConnectTimeout == 0 {
		s.ConnectTimeout = defaultConnectTimeout
	}

	if len(s.SASLProviders) == 0 && (s.SASLRequired || s.HandlerV0 != nil) {
		// AMQP 0.9.1 requires authentication
		return errors.New("no SASL provider")
	}

	m := make(map[string]bool)
	for _, provider := range s.SASLProviders {
		mechanism := provider.Mechanism()
		if m[mechanism] {
			return errors.Errorf("multiple SASL providers for mechanism %s", mechanism)
		}
		m[mechanism] = true
	}

	if s.Heartbeat == 0 {
		s.Heartbeat = defaultHeartbeat
	} else if seconds := s.Heartbeat / time.Second; seconds > math.MaxUint16 {
		return errors.Errorf("heartbeat %d out of bounds (%d)", seconds, math.MaxUint16)
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	return nil
}

func (s *Server) Serve(l net.Listener) error {
	if s.listener != nil {
		return errors.New("server already started")
	}

	err := s.init()
	if err != nil {
		return errors.Wrap(err, "init failed")
	}

	s.listener = l

	for {
		conn, err := l.Accept()
		if err != nil {
			return errors.Wrap(err, "accept failed")
		}
		go s.accept(conn)
	}
}

func (s *Server) accept(conn net.Conn) {
	defer conn.Close()
	err := s.handle(conn)
	if err != nil {
		log.Printf("[AMQP %s] %s", conn.RemoteAddr(), err.Error())
	}
}

func (s *Server) handle(conn net.Conn) error {
	err := conn.SetDeadline(time.Now().Add(s.ConnectTimeout))
	if err != nil {
		return errors.Wrap(err, "set deadline failed")
	}

	var x [8]byte

	_, err = io.ReadFull(conn, x[:])
	if err != nil {
		return errors.Wrap(err, "read protocol header failed")
	}

	if x[0] == 'A' && x[1] == 'M' && x[2] == 'Q' && x[3] == 'P' && x[4] == 0 &&
		x[5] == v0.Major && x[6] == v0.Minor && x[7] == v0.Revision && s.HandlerV0 != nil {

		// AMQP 0.9.1

		transport := v0.NewTransport(conn)
		ctx, err := s.initV0(transport)
		if err != nil {
			return errors.Wrapf(err, "init v%d.%d.%d connection failed", x[5], x[6], x[7])
		}

		return s.HandlerV0.ServeAMQPv0(ctx, transport)

	} else if x[0] == 'A' && x[1] == 'M' && x[2] == 'Q' && x[3] == 'P' &&
		x[5] == v1.Major && x[6] == v1.Major && x[7] == v1.Major && s.HandlerV1 != nil {

		// AMQP 1.0

		var token sasl.Token
		transport := v1.NewTransport(conn) // un-buffered
		id := x[4]

		switch id {
		case 0: // plain
			if token == nil && s.SASLRequired {
				x[4] = 3
				_, err = conn.Write(x[:])
				return errors.Wrap(err, "write protocol header failed")
			}

			_, err = conn.Write(x[:])
			if err != nil {
				return errors.Wrap(err, "echo protocol header failed")
			}

			err = transport.SetBuffered(true)
			if err != nil {
				return errors.Wrap(err, "transport buffering failed")
			}

			return s.HandlerV1.ServeAMQPv1(NewContextV1(s.ctx, token), transport)

		case 3: // SASL
			if len(s.SASLProviders) == 0 {
				// no SASL providers => force client to use plain connection
				x[4] = 0
				_, err = conn.Write(x[:])
				return errors.Wrap(err, "write protocol header failed")
			}

			_, err = conn.Write(x[:])
			if err != nil {
				return errors.Wrap(err, "echo protocol header failed")
			}

			panic("implement me")

		default:
			// unknown id-protocol ID
			if s.SASLRequired {
				x[4] = 3
			} else {
				x[4] = 0
			}
			_, err = conn.Write(x[:])
			return errors.Wrap(err, "write protocol header failed")
		}

	} else {
		x[0] = 'A'
		x[1] = 'M'
		x[2] = 'Q'
		x[3] = 'P'

		if s.HandlerV1 != nil {
			if s.SASLRequired {
				x[4] = 3
			} else {
				x[4] = 0
			}
			x[5] = v1.Major
			x[6] = v1.Minor
			x[7] = v1.Revision
		} else if s.HandlerV0 != nil {
			x[4] = 0
			x[5] = v0.Major
			x[6] = v0.Minor
			x[7] = v0.Revision
		} else {
			x[4] = 0
			x[5] = 0
			x[6] = 0
			x[7] = 0
		}

		_, err := conn.Write(x[:])
		return errors.Wrap(err, "write protocol header failed")
	}
}

func (s *Server) Close() error {
	s.cancel()
	return s.listener.Close()
}
