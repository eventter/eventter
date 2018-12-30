package sasl

import (
	"context"
	"testing"

	"eventter.io/mq/amqp/v0"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestAmqplainProvider_Authenticate_Success(t *testing.T) {
	assert := require.New(t)

	provider := NewAMQPLAIN(func(ctx context.Context, username, password string) (ok bool, err error) {
		return true, nil
	})

	buf, err := v0.MarshalTable(&types.Struct{
		Fields: map[string]*types.Value{
			"LOGIN":    {Kind: &types.Value_StringValue{StringValue: "user"}},
			"PASSWORD": {Kind: &types.Value_StringValue{StringValue: "pass"}},
		},
	})
	assert.NoError(err)

	token, challenge, err := provider.Authenticate(context.Background(), "", string(buf))
	assert.NoError(err)
	assert.Empty(challenge)
	assert.NotNil(token)

	assert.Equal(&UsernamePasswordToken{
		Username: "user",
		Password: "pass",
	}, token)
}

func TestAmqplainProvider_Authenticate_BadResponse(t *testing.T) {
	assert := require.New(t)

	provider := NewAMQPLAIN(func(ctx context.Context, username, password string) (ok bool, err error) {
		return true, nil
	})

	token, challenge, err := provider.Authenticate(context.Background(), "", "")
	assert.Error(err)
	assert.Empty(challenge)
	assert.Nil(token)
}

func TestAmqplainProvider_Authenticate_NotVerified(t *testing.T) {
	assert := require.New(t)

	provider := NewAMQPLAIN(func(ctx context.Context, username, password string) (ok bool, err error) {
		return false, nil
	})

	buf, err := v0.MarshalTable(&types.Struct{
		Fields: map[string]*types.Value{
			"LOGIN":    {Kind: &types.Value_StringValue{StringValue: "user"}},
			"PASSWORD": {Kind: &types.Value_StringValue{StringValue: "pass"}},
		},
	})
	assert.NoError(err)

	token, challenge, err := provider.Authenticate(context.Background(), "", string(buf))
	assert.NoError(err)
	assert.Empty(challenge)
	assert.Nil(token)
}
