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
	// Max time to establish connection.
	ConnectTimeout time.Duration
	// Server authentication providers.
	AuthenticationProviders []authentication.Provider
	// Heartbeat the server tries to negotiate with clients.
	Heartbeat time.Duration
	// Handle AMQP 0.9.1 connection.
	HandlerV0 HandlerV0
}

func (s *Server) Serve(l net.Listener) error {
	if s.ConnectTimeout == 0 {
		s.ConnectTimeout = defaultConnectTimeout
	}
	if len(s.AuthenticationProviders) == 0 {
		return errors.Errorf("no authentication provider")
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
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(s.ConnectTimeout)); err != nil {
		log.Printf("[AMQP %s] set deadline failed: %s", conn.RemoteAddr(), err)
	} else {
		var x [8]byte
		r := bufio.NewReader(conn)
		if _, err := io.ReadFull(r, x[:]); err != nil {
			log.Printf("[AMQP %s] read failed: %s", conn.RemoteAddr(), err)
		} else {
			if x[0] == 'A' && x[1] == 'M' && x[2] == 'Q' && x[3] == 'P' && x[4] == 0 &&
				x[5] == v0.Major && x[6] == v0.Minor && x[7] == v0.Revision && s.HandlerV0 != nil {

				transport := v0.NewTransport(conn, bufio.NewReadWriter(r, bufio.NewWriter(conn)))
				token, heartbeat, virtualHost, err := s.initV0(transport, r)

				if err != nil {
					log.Printf("[AMQP %s] init v%d.%d.%d connection failed: %s", conn.RemoteAddr(), x[5], x[6], x[7], err)
				} else {
					err = s.HandlerV0.ServeAMQPv0(transport, token, heartbeat, virtualHost)
					if err != nil {
						log.Printf("[AMQP %s] serving v%d.%d.%d connection failed: %s", conn.RemoteAddr(), x[5], x[6], x[7], err)
					}
				}

			} else {
				x[0] = 'A'
				x[1] = 'M'
				x[2] = 'Q'
				x[3] = 'P'
				x[4] = 0
				x[5] = v0.Major
				x[6] = v0.Minor
				x[7] = v0.Revision

				if _, err := conn.Write(x[:]); err != nil {
					log.Printf("[AMQP %s] write close failed: %s", conn.RemoteAddr(), err)
				}
			}
		}
	}
}
