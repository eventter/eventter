package amqp

import (
	"bufio"
	"io"
	"log"
	"math"
	"net"
	"strings"
	"time"

	"eventter.io/mq/about"
	"eventter.io/mq/amqp/authentication"
	"eventter.io/mq/amqp/v0"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

const (
	defaultConnectTimeout = 10 * time.Second
	defaultHeartbeat      = 10 * time.Second
)

type HandlerV0 func(conn net.Conn, sc *v0.ServerConn) error

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

				sc, err := s.initV0(conn, r)
				if err != nil {
					log.Printf("[AMQP %s] init v%d.%d.%d connection failed: %s", conn.RemoteAddr(), x[5], x[6], x[7], err)
				} else {
					err = s.HandlerV0(conn, sc)
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

func (s *Server) initV0(conn net.Conn, r *bufio.Reader) (*v0.ServerConn, error) {
	rw := bufio.NewReadWriter(r, bufio.NewWriter(conn))
	transport := v0.NewTransport(rw)

	var mechanisms []string
	for _, provider := range s.AuthenticationProviders {
		mechanisms = append(mechanisms, provider.Mechanism())
	}

	err := transport.Send(&v0.ConnectionStart{
		VersionMajor: v0.Major,
		VersionMinor: v0.Minor,
		ServerProperties: &types.Struct{
			Fields: map[string]*types.Value{
				"product": {Kind: &types.Value_StringValue{StringValue: about.Name}},
				"version": {Kind: &types.Value_StringValue{StringValue: about.Version}},
				"capabilities": {Kind: &types.Value_StructValue{StructValue: &types.Struct{
					Fields: map[string]*types.Value{
						"basic.nack": {Kind: &types.Value_BoolValue{BoolValue: true}},
					},
				}}},
			},
		},
		Mechanisms: strings.Join(mechanisms, " "),
		Locales:    "en_US",
	})
	if err != nil {
		return nil, errors.Wrap(err, "send connection.start failed")
	}

	frame, err := transport.Receive()
	if err != nil {
		return nil, errors.Wrap(err, "receive connection.start-ok failed")
	}
	startOk, ok := frame.(*v0.ConnectionStartOk)
	if !ok {
		return nil, errors.Errorf("did not receive connection.start-ok, got %T instead", frame)
	}

	var (
		challenge string
		token     authentication.Token
	)
	for _, provider := range s.AuthenticationProviders {
		if startOk.Mechanism != provider.Mechanism() {
			continue
		}

		token, challenge, err = provider.Authenticate(challenge, startOk.Response)
		if err != nil {
			return nil, errors.Wrapf(err, "authentication using %s failed", startOk.Mechanism)
		}

		for token == nil {
			err = transport.Send(&v0.ConnectionSecure{Challenge: challenge})
			if err != nil {
				return nil, errors.Wrap(err, "send connection.secure failed")
			}
			frame, err = transport.Receive()
			if err != nil {
				return nil, errors.Wrap(err, "receive connection.secure-ok failed")
			}
			secureOk, ok := frame.(*v0.ConnectionSecureOk)
			if !ok {
				return nil, errors.Errorf("did not receive connection.secure-ok, got %T instead", frame)
			}

			token, challenge, err = provider.Authenticate(challenge, secureOk.Response)
			if err != nil {
				return nil, errors.Wrapf(err, "authentication using %s failed", startOk.Mechanism)
			}
		}

		break
	}
	if token == nil {
		return nil, errors.Errorf("client selected unsupported authentication mechanism %s", startOk.Mechanism)
	}

	if !token.IsAuthenticated() {
		return nil, errors.Errorf("user %q not authenticated", token.Subject())
	}

	err = transport.Send(&v0.ConnectionTune{
		ChannelMax: math.MaxUint16,
		FrameMax:   math.MaxUint32,
		Heartbeat:  uint16(s.Heartbeat / time.Second),
	})
	if err != nil {
		return nil, errors.Wrap(err, "send connection.tune failed")
	}

	frame, err = transport.Receive()
	if err != nil {
		return nil, errors.Wrap(err, "receive connection.tune-ok failed")
	}
	tuneOk, ok := frame.(*v0.ConnectionTuneOk)
	if !ok {
		return nil, errors.Errorf("did not receive connection.tune-ok, got %T instead", frame)
	}

	frame, err = transport.Receive()
	if err != nil {
		return nil, errors.Wrap(err, "receive connection.open failed")
	}
	open, ok := frame.(*v0.ConnectionOpen)
	if !ok {
		return nil, errors.Errorf("did not receive connection.open got %T instead", frame)
	}

	err = transport.Send(&v0.ConnectionOpenOk{})
	if err != nil {
		return nil, errors.Errorf("send connection.open-ok failed")
	}

	err = conn.SetDeadline(time.Time{})
	if err != nil {
		return nil, errors.Wrap(err, "reset deadline failed")
	}

	return &v0.ServerConn{
		Transport:   transport,
		Token:       token,
		ChannelMax:  tuneOk.ChannelMax,
		FrameMax:    tuneOk.FrameMax,
		Heartbeat:   time.Duration(tuneOk.Heartbeat) * time.Second,
		VirtualHost: open.VirtualHost,
	}, nil
}
