package authentication

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type amqplainProvider struct {
	verify UsernamePasswordVerifier
}

func NewAMQPLAIN(verify UsernamePasswordVerifier) Provider {
	return &amqplainProvider{verify: verify}
}

func (p *amqplainProvider) Mechanism() string {
	return "AMQPLAIN"
}

func (p *amqplainProvider) Authenticate(challenge string, response string) (token Token, nextChallenge string, err error) {
	buf := bytes.NewBuffer([]byte(response))
	var username, password string
	for {
		lb, err := buf.ReadByte()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, "", errors.Wrap(err, "read field name failed")
		}

		l := int(lb)
		name := string(buf.Next(l))
		if len(name) < l {
			return nil, "", errors.New("read field name failed")
		}

		tb, err := buf.ReadByte()
		if err != nil {
			return nil, "", errors.Wrap(err, "read field type failed")
		}

		var value string
		switch tb {
		case 's':
			lb, err = buf.ReadByte()
			if err != nil {
				return nil, "", errors.Wrap(err, "read shortstr failed")
			}
			l = int(lb)
			value = string(buf.Next(l))
			if len(value) < l {
				return nil, "", errors.New("read shortstr failed")
			}
		case 'S':
			var x [4]byte
			if n, err := buf.Read(x[:4]); err != nil {
				return nil, "", errors.Wrap(err, "read longstr failed")
			} else if n < 4 {
				return nil, "", errors.New("read longstr failed")
			}
			l := int(binary.BigEndian.Uint32(x[:4]))
			value = string(buf.Next(l))
			if len(value) < l {
				return nil, "", errors.New("read longstr failed")
			}
		default:
			return nil, "", errors.Errorf("expected shortstr/longstr field, got %c", tb)
		}

		switch name {
		case "LOGIN":
			username = value
		case "PASSWORD":
			password = value
		default:
			return nil, "", errors.Errorf("unhandled field %s", name)
		}
	}

	ok, err := p.verify(username, password)
	if err != nil {
		return nil, "", errors.Wrap(err, "authentication failed")
	}
	if !ok {
		return &NotAuthenticatedToken{Username: username}, "", nil
	}
	return &UsernamePasswordToken{
		Username: username,
		Password: password,
	}, "", nil
}
