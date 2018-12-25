package amqp

import (
	"bufio"
	"math"
	"strings"
	"time"

	"eventter.io/mq/about"
	"eventter.io/mq/amqp/authentication"
	"eventter.io/mq/amqp/v0"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

type HandlerV0 interface {
	ServeAMQPv0(transport *v0.Transport, token authentication.Token, heartbeat time.Duration, virtualHost string) error
}

func (s *Server) initV0(transport *v0.Transport, r *bufio.Reader) (token authentication.Token, heartbeat time.Duration, virtualHost string, err error) {
	var mechanisms []string
	for _, provider := range s.AuthenticationProviders {
		mechanisms = append(mechanisms, provider.Mechanism())
	}

	err = transport.Send(&v0.ConnectionStart{
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
		return nil, 0, "", errors.Wrap(err, "send connection.start failed")
	}

	frame, err := transport.Receive()
	if err != nil {
		return nil, 0, "", errors.Wrap(err, "receive connection.start-ok failed")
	}
	startOk, ok := frame.(*v0.ConnectionStartOk)
	if !ok {
		return nil, 0, "", errors.Errorf("did not receive connection.start-ok, got %T instead", frame)
	}

	var challenge string
	for _, provider := range s.AuthenticationProviders {
		if startOk.Mechanism != provider.Mechanism() {
			continue
		}

		token, challenge, err = provider.Authenticate(challenge, startOk.Response)
		if err != nil {
			return nil, 0, "", errors.Wrapf(err, "authentication using %s failed", startOk.Mechanism)
		}

		for token == nil {
			err = transport.Send(&v0.ConnectionSecure{Challenge: challenge})
			if err != nil {
				return nil, 0, "", errors.Wrap(err, "send connection.secure failed")
			}
			frame, err = transport.Receive()
			if err != nil {
				return nil, 0, "", errors.Wrap(err, "receive connection.secure-ok failed")
			}
			secureOk, ok := frame.(*v0.ConnectionSecureOk)
			if !ok {
				return nil, 0, "", errors.Errorf("did not receive connection.secure-ok, got %T instead", frame)
			}

			token, challenge, err = provider.Authenticate(challenge, secureOk.Response)
			if err != nil {
				return nil, 0, "", errors.Wrapf(err, "authentication using %s failed", startOk.Mechanism)
			}
		}

		break
	}
	if token == nil {
		return nil, 0, "", errors.Errorf("client selected unsupported authentication mechanism %s", startOk.Mechanism)
	}

	if !token.IsAuthenticated() {
		return nil, 0, "", errors.Errorf("user %q not authenticated", token.Subject())
	}

	err = transport.Send(&v0.ConnectionTune{
		ChannelMax: math.MaxUint16,
		FrameMax:   math.MaxUint32,
		Heartbeat:  uint16(s.Heartbeat / time.Second),
	})
	if err != nil {
		return nil, 0, "", errors.Wrap(err, "send connection.tune failed")
	}

	frame, err = transport.Receive()
	if err != nil {
		return nil, 0, "", errors.Wrap(err, "receive connection.tune-ok failed")
	}
	tuneOk, ok := frame.(*v0.ConnectionTuneOk)
	if !ok {
		return nil, 0, "", errors.Errorf("did not receive connection.tune-ok, got %T instead", frame)
	}
	if tuneOk.ChannelMax == 0 {
		tuneOk.ChannelMax = math.MaxUint16
	}
	if tuneOk.FrameMax == 0 {
		tuneOk.FrameMax = math.MaxUint32
	} else if tuneOk.FrameMax < v0.FrameMinSize {
		return nil, 0, "", errors.Errorf("client tried to negotiate frame max size %d, less than mandatory minimum", tuneOk.FrameMax)
	}

	heartbeat = time.Duration(tuneOk.Heartbeat) * time.Second

	transport.SetFrameMax(tuneOk.FrameMax)
	if err := transport.SetReceiveTimeout(heartbeat * 2); err != nil {
		return nil, 0, "", errors.Wrap(err, "set receive timeout failed")
	}
	if err := transport.SetSendTimeout(heartbeat); err != nil {
		return nil, 0, "", errors.Wrap(err, "set send timeout failed")
	}

	frame, err = transport.Receive()
	if err != nil {
		return nil, 0, "", errors.Wrap(err, "receive connection.open failed")
	}
	open, ok := frame.(*v0.ConnectionOpen)
	if !ok {
		return nil, 0, "", errors.Errorf("did not receive connection.open got %T instead", frame)
	}

	err = transport.Send(&v0.ConnectionOpenOk{})
	if err != nil {
		return nil, 0, "", errors.Errorf("send connection.open-ok failed")
	}

	return token, heartbeat, open.VirtualHost, nil
}
