package amqp

import (
	"context"
	"io"
	"log"
	"math"
	"net"
	"time"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/sasl"
	"github.com/pkg/errors"
)

const (
	defaultConnectTimeout = 10 * time.Second
	defaultHeartbeat      = 60 * time.Second
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
	HeartbeatV0 time.Duration
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

	if s.HeartbeatV0 == 0 {
		s.HeartbeatV0 = defaultHeartbeat
	} else if seconds := s.HeartbeatV0 / time.Second; seconds > math.MaxUint16 {
		return errors.Errorf("heartbeat %d out of bounds (max %d seconds)", seconds, math.MaxUint16)
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
	deadline := time.Now().Add(s.ConnectTimeout)
	err := conn.SetDeadline(deadline)
	if err != nil {
		return errors.Wrap(err, "set deadline failed")
	}

	var proto [8]byte

	_, err = io.ReadFull(conn, proto[:])
	if err != nil {
		return errors.Wrap(err, "read protocol header failed")
	}

	if proto[0] == 'A' && proto[1] == 'M' && proto[2] == 'Q' && proto[3] == 'P' && proto[4] == 0 &&
		proto[5] == v0.Major && proto[6] == v0.Minor && proto[7] == v0.Revision && s.HandlerV0 != nil {

		transport := v0.NewTransport(conn)
		ctx, err := s.initV0(transport, deadline)
		if err != nil {
			return errors.Wrapf(err, "init v%d.%d.%d connection failed", proto[5], proto[6], proto[7])
		}

		return s.HandlerV0.ServeAMQPv0(ctx, transport)

	} else if proto[0] == 'A' && proto[1] == 'M' && proto[2] == 'Q' && proto[3] == 'P' &&
		proto[5] == v1.Major && proto[6] == v1.Minor && proto[7] == v1.Revision && s.HandlerV1 != nil {

		transport := v1.NewTransport(conn) // intentionally un-buffered
		ctx, err := s.initV1(transport, deadline, conn, proto[4])
		if err != nil {
			return errors.Wrapf(err, "init v%d.%d.%d connection failed", proto[5], proto[6], proto[7])
		}

		return s.HandlerV1.ServeAMQPv1(ctx, transport)

	} else {
		proto = [8]byte{'A', 'M', 'Q', 'P', 0, 0, 0, 0}

		if s.HandlerV1 != nil {
			if s.SASLRequired {
				proto[4] = protoSASL
			}
			proto[5] = v1.Major
			proto[6] = v1.Minor
			proto[7] = v1.Revision
		} else if s.HandlerV0 != nil {
			proto[5] = v0.Major
			proto[6] = v0.Minor
			proto[7] = v0.Revision
		}

		_, err := conn.Write(proto[:])
		return errors.Wrap(err, "write protocol header failed")
	}
}

func (s *Server) Close() error {
	s.cancel()
	return s.listener.Close()
}
