package amqp

import (
	"context"
	"io"
	"net"
	"time"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/sasl"
	"github.com/pkg/errors"
)

const (
	protoPlain = 0
	protoSASL  = 3
)

type HandlerV1 interface {
	ServeAMQPv1(ctx context.Context, transport *v1.Transport) error // TODO: should serve accept connection / session / link?
}

func NewContextV1(parent context.Context, token sasl.Token) context.Context {
	return context.WithValue(parent, contextKey, &contextValueV0{
		token: token,
	})
}

type contextValueV1 struct {
	token sasl.Token
}

func (s *Server) initV1(transport *v1.Transport, deadline time.Time, conn net.Conn, protoID byte) (ctx context.Context, err error) {
	ctx, cancel := context.WithDeadline(s.ctx, deadline)
	defer cancel()

	var token sasl.Token
	var proto = [8]byte{'A', 'M', 'Q', 'P', protoID, v1.Major, v1.Minor, v1.Revision}

START:
	switch proto[4] {
	case protoPlain:
		if token == nil && s.SASLRequired {
			// not authenticated => force client to do SASL
			proto[4] = protoSASL
			_, err = conn.Write(proto[:])
			return nil, errors.Wrap(err, "write protocol header failed")
		}

		_, err = conn.Write(proto[:])
		if err != nil {
			return nil, errors.Wrap(err, "echo protocol header failed")
		}

		err = transport.SetBuffered(true)
		if err != nil {
			return nil, errors.Wrap(err, "start of transport buffering failed")
		}

		return NewContextV1(s.ctx, token), nil

	case protoSASL:
		if len(s.SASLProviders) == 0 {
			// no SASL providers => force client to use plain connection
			proto[4] = protoPlain
			_, err = conn.Write(proto[:])
			return nil, errors.Wrap(err, "write protocol header failed")
		}

		_, err = conn.Write(proto[:])
		if err != nil {
			return nil, errors.Wrap(err, "echo protocol header failed")
		}

		mechanisms := make([]string, 0, len(s.SASLProviders))
		for _, provider := range s.SASLProviders {
			mechanisms = append(mechanisms, provider.Mechanism())
		}

		var saslInit *v1.SASLInit
		err = transport.Call(&v1.SASLMechanisms{SASLServerMechanisms: mechanisms}, &saslInit)
		if err != nil {
			return nil, errors.Wrap(err, "SASL failed: send mechanisms")
		}

		var provider sasl.Provider
		for _, p := range s.SASLProviders {
			if p.Mechanism() == saslInit.Mechanism {
				provider = p
				break
			}
		}

		if provider == nil {
			return nil, errors.Errorf("client selected unknown SASL mechanism %s", saslInit.Mechanism)
		}

		var challenge []byte = nil
		response := saslInit.InitialResponse
		for {
			token, challenge, err = provider.Authenticate(ctx, challenge, response)
			if err != nil {
				outcomeErr := transport.Send(&v1.SASLOutcome{Code: v1.SysSASLCode})
				if outcomeErr != nil {
					return nil, errors.Wrapf(outcomeErr, "SASL provider failed with (%s) & send SASL outcome failed", err)
				}
				return nil, errors.Wrap(err, "SASL provider failed")
			} else if token == nil && challenge == nil {
				err = transport.Send(&v1.SASLOutcome{Code: v1.AuthSASLCode})
				if err != nil {
					return nil, errors.Wrap(err, "send SASL outcome failed")
				}
				return nil, errors.New("not authenticated")
			} else if token != nil {
				err = transport.Send(&v1.SASLOutcome{Code: v1.OkSASLCode})
				if err != nil {
					return nil, errors.Wrap(err, "send SASL outcome failed")
				}
				break
			}

			var saslResponse *v1.SASLResponse
			err = transport.Call(&v1.SASLChallenge{Challenge: challenge}, &saslResponse)
			if err != nil {
				return nil, errors.Wrap(err, "send SASL challenge failed")
			}
		}

		_, err = io.ReadFull(conn, proto[:8])
		if err != nil {
			return nil, errors.Wrap(err, "read protocol header after SASL failed")
		}

		if proto[0] == 'A' && proto[1] == 'M' && proto[2] == 'Q' && proto[3] == 'P' &&
			proto[5] == v1.Major && proto[6] == v1.Minor && proto[7] == v1.Revision {
			goto START
		} else {
			proto = [8]byte{'A', 'M', 'Q', 'P', protoPlain, v1.Major, v1.Minor, v1.Revision}
			if s.SASLRequired {
				proto[4] = protoSASL
			}
			_, err = conn.Write(proto[:])
			return nil, errors.Wrap(err, "write protocol header failed")
		}

	default:
		if s.SASLRequired {
			proto[4] = protoSASL
		} else {
			proto[4] = protoPlain
		}
		_, err = conn.Write(proto[:])
		return nil, errors.Wrap(err, "write protocol header failed")
	}
}
