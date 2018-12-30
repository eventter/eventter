package amqp

import (
	"context"
	"math"
	"strings"
	"time"

	"eventter.io/mq/amqp/sasl"
	"eventter.io/mq/amqp/v0"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

type HandlerV0 interface {
	ServeAMQPv0(ctx context.Context, transport *v0.Transport) error
}

func NewContextV0(parent context.Context, token sasl.Token, heartbeat time.Duration, virtualHost string) context.Context {
	return context.WithValue(parent, contextKey, &contextValueV0{
		token:       token,
		heartbeat:   heartbeat,
		virtualHost: virtualHost,
	})
}

type contextValueV0 struct {
	token       sasl.Token
	heartbeat   time.Duration
	virtualHost string
}

func (s *Server) initV0(transport *v0.Transport) (ctx context.Context, err error) {
	var mechanisms []string
	for _, provider := range s.SASLProviders {
		mechanisms = append(mechanisms, provider.Mechanism())
	}

	serverProperties := make(map[string]*types.Value)
	if s.Name != "" {
		serverProperties["product"] = &types.Value{Kind: &types.Value_StringValue{StringValue: s.Name}}
	}
	if s.Version != "" {
		serverProperties["version"] = &types.Value{Kind: &types.Value_StringValue{StringValue: s.Version}}
	}
	if len(s.CapabilitiesV0) > 0 {
		fields := make(map[string]*types.Value)
		for _, capability := range s.CapabilitiesV0 {
			fields[capability] = &types.Value{Kind: &types.Value_BoolValue{BoolValue: true}}
		}
		serverProperties["capabilities"] = &types.Value{Kind: &types.Value_StructValue{StructValue: &types.Struct{
			Fields: fields,
		}}}
	}

	err = transport.Send(&v0.ConnectionStart{
		VersionMajor:     v0.Major,
		VersionMinor:     v0.Minor,
		ServerProperties: &types.Struct{Fields: serverProperties},
		Mechanisms:       strings.Join(mechanisms, " "),
		Locales:          "en_US",
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
		token     sasl.Token
		challenge string
	)
	for _, provider := range s.SASLProviders {
		if startOk.Mechanism != provider.Mechanism() {
			continue
		}

		token, challenge, err = provider.Authenticate(challenge, startOk.Response)
		if err != nil {
			return nil, errors.Wrapf(err, "sasl using %s failed", startOk.Mechanism)
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
				return nil, errors.Wrapf(err, "sasl using %s failed", startOk.Mechanism)
			}
		}

		break
	}
	if token == nil {
		return nil, errors.Errorf("client selected unsupported sasl mechanism %s", startOk.Mechanism)
	}

	tune := &v0.ConnectionTune{
		ChannelMax: 2047, // see https://github.com/rabbitmq/rabbitmq-server/issues/1593
		FrameMax:   math.MaxUint32,
		Heartbeat:  uint16(s.Heartbeat / time.Second),
	}
	err = transport.Send(tune)
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
	if tuneOk.ChannelMax == 0 {
		tuneOk.ChannelMax = tune.ChannelMax
	} else if tuneOk.ChannelMax > tune.ChannelMax {
		return nil, errors.Errorf("client tried to raise channel max (server=%d, client=%d)", tune.ChannelMax, tuneOk.ChannelMax)
	}
	if tuneOk.FrameMax == 0 {
		tuneOk.FrameMax = tune.FrameMax
	} else if tuneOk.FrameMax < v0.FrameMinSize {
		return nil, errors.Errorf("client tried to negotiate frame max size %d, less than mandatory minimum", tuneOk.FrameMax)
	} else if tuneOk.FrameMax > tune.FrameMax {
		return nil, errors.Errorf("client tried to raise frame max (server=%d, client=%d)", tune.FrameMax, tuneOk.FrameMax)
	}

	heartbeat := time.Duration(tuneOk.Heartbeat) * time.Second

	transport.SetFrameMax(tuneOk.FrameMax)
	if err := transport.SetReceiveTimeout(heartbeat * 2); err != nil {
		return nil, errors.Wrap(err, "set receive timeout failed")
	}
	if err := transport.SetSendTimeout(heartbeat / 2); err != nil {
		return nil, errors.Wrap(err, "set send timeout failed")
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

	return NewContextV0(s.ctx, token, heartbeat, open.VirtualHost), nil
}
