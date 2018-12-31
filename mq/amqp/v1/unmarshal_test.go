package v1

import (
	"bytes"
	"fmt"
	"testing"

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
