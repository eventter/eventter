package v1

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalDeliveryStateUnion(t *testing.T) {
	tests := []DeliveryState{
		&Accepted{},
		&Modified{},
		&Received{},
		&Rejected{},
		&Released{},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%T", test), func(t *testing.T) {
			assert := require.New(t)

			buf := &bytes.Buffer{}
			err := marshalDeliveryStateUnion(test, buf)
			assert.NoError(err)

			var out DeliveryState
			err = unmarshalDeliveryStateUnion(&out, buf)
			assert.NoError(err)
			assert.Equal(test, out)
		})
	}
}

func TestUnmarshalMessageIDUnion(t *testing.T) {
	tests := []MessageID{
		MessageIDString("message-id-string"),
		MessageIDBinary("message-id-binary"),
		MessageIDUlong(10),
		MessageIDUUID{},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%T", test), func(t *testing.T) {
			assert := require.New(t)

			buf := &bytes.Buffer{}
			err := marshalMessageIDUnion(test, buf)
			assert.NoError(err)

			var out MessageID
			err = unmarshalMessageIDUnion(&out, buf)
			assert.NoError(err)
			assert.Equal(test, out)
		})
	}
}

func TestUnmarshalMessageSection(t *testing.T) {
	tests := []Section{
		&Header{Durable: true},
		&DeliveryAnnotations{Fields: map[string]*types.Value{
			"delivery-annotation": {Kind: &types.Value_StringValue{StringValue: "value"}},
		}},
		&MessageAnnotations{Fields: map[string]*types.Value{
			"message-annotation": {Kind: &types.Value_NumberValue{NumberValue: 42}},
		}},
		&Properties{MessageID: MessageIDUlong(1)},
		&Properties{ReplyToGroupID: "group"},
		&ApplicationProperties{Fields: map[string]*types.Value{
			"application-property": {Kind: &types.Value_BoolValue{BoolValue: true}},
		}},
		Data([]byte("hello, world")),
		&Footer{Fields: map[string]*types.Value{
			"footer": {Kind: &types.Value_ListValue{ListValue: &types.ListValue{Values: []*types.Value{
				{Kind: &types.Value_NullValue{}},
				{Kind: &types.Value_StringValue{StringValue: "bar"}},
			}}}},
		}},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%T", test), func(t *testing.T) {
			assert := require.New(t)

			buf := &bytes.Buffer{}
			err := test.MarshalBuffer(buf)
			assert.NoError(err)

			var out Section
			err = UnmarshalSection(&out, buf)
			assert.NoError(err)
			assert.Equal(test, out)

			assert.Equal(0, buf.Len())
		})
	}
}
