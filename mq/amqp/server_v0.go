package amqp

import (
	"context"
	"math"
	"strings"
	"time"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/sasl"
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

func (s *Server) initV0(transport *v0.Transport, deadline time.Time) (ctx context.Context, err error) {
	ctx, cancel := context.WithDeadline(s.ctx, deadline)
	defer cancel()

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

	var startOk *v0.ConnectionStartOk
	err = transport.Call(&v0.ConnectionStart{
		VersionMajor:     v0.Major,
		VersionMinor:     v0.Minor,
		ServerProperties: &types.Struct{Fields: serverProperties},
		Mechanisms:       strings.Join(mechanisms, " "),
		Locales:          "en_US",
	}, &startOk)
	if err != nil {
		return nil, errors.Wrap(err, "call connection.start failed")
	}

	var provider sasl.Provider
	for _, p := range s.SASLProviders {
		if p.Mechanism() == startOk.Mechanism {
			provider = p
			break
		}
	}

	if provider == nil {
		return nil, errors.Errorf("client selected unknown SASL mechanism %s", startOk.Mechanism)
	}

	var token sasl.Token
	var challenge []byte = nil
	response := []byte(startOk.Response)
	for {
		token, challenge, err = provider.Authenticate(ctx, challenge, response)
		if err != nil {
			return nil, errors.Wrap(err, "SASL provider failed")
		} else if token == nil && challenge == nil {
			return nil, errors.New("not authenticated")
		} else if token != nil {
			break
		}

		var secureOk *v0.ConnectionSecureOk
		err = transport.Call(&v0.ConnectionSecure{Challenge: string(challenge)}, &secureOk)
		if err != nil {
			return nil, errors.Wrap(err, "call connection.secure failed")
		}
	}

	var tuneOk *v0.ConnectionTuneOk
	tune := &v0.ConnectionTune{
		ChannelMax: 2047, // see https://github.com/rabbitmq/rabbitmq-server/issues/1593
		FrameMax:   math.MaxUint32,
		Heartbeat:  uint16(s.Heartbeat / time.Second),
	}
	err = transport.Call(tune, &tuneOk)
	if err != nil {
		return nil, errors.Wrap(err, "call connection.tune failed")
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

	var open *v0.ConnectionOpen
	err = transport.Call(nil, &open)
	if err != nil {
		return nil, errors.Wrap(err, "receive connection.open failed")
	}

	err = transport.Send(&v0.ConnectionOpenOk{})
	if err != nil {
		return nil, errors.Errorf("send connection.open-ok failed")
	}

	return NewContextV0(s.ctx, token, heartbeat, open.VirtualHost), nil
}
