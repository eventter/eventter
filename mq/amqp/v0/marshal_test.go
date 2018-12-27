package v0

import (
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

var tests = []struct {
	name  string
	table *types.Struct
	data  []byte
}{
	{"nil", nil, nil},
	{"nil fields", &types.Struct{}, nil},
	{"empty fields", &types.Struct{Fields: make(map[string]*types.Value)}, nil},
	{"null value", &types.Struct{Fields: map[string]*types.Value{
		"null": {Kind: &types.Value_NullValue{}},
	}}, []byte("\x04nullV")},
	{"int8 value", &types.Struct{Fields: map[string]*types.Value{
		"int8": {Kind: &types.Value_NumberValue{NumberValue: 0x01}},
	}}, []byte("\x04int8b\x01")},
	{"int16 value", &types.Struct{Fields: map[string]*types.Value{
		"int16": {Kind: &types.Value_NumberValue{NumberValue: 0x0123}},
	}}, []byte("\x05int16s\x01\x23")},
	{"int32 value", &types.Struct{Fields: map[string]*types.Value{
		"int32": {Kind: &types.Value_NumberValue{NumberValue: 0x012345}},
	}}, []byte("\x05int32I\x00\x01\x23\x45")},
	{"int64 value", &types.Struct{Fields: map[string]*types.Value{
		"int64": {Kind: &types.Value_NumberValue{NumberValue: 0x0123456789}},
	}}, []byte("\x05int64l\x00\x00\x00\x01\x23\x45\x67\x89")},
	{"float64 value", &types.Struct{Fields: map[string]*types.Value{
		"float64": {Kind: &types.Value_NumberValue{NumberValue: 3.14}},
	}}, []byte("\afloat64d@\t\x1e\xb8Q\xeb\x85\x1f")},
	{"string value", &types.Struct{Fields: map[string]*types.Value{
		"string": {Kind: &types.Value_StringValue{StringValue: "hello, world"}},
	}}, []byte("\x06stringS\x00\x00\x00\fhello, world")},
	{"false value", &types.Struct{Fields: map[string]*types.Value{
		"false": {Kind: &types.Value_BoolValue{BoolValue: false}},
	}}, []byte("\x05falset\x00")},
	{"true value", &types.Struct{Fields: map[string]*types.Value{
		"true": {Kind: &types.Value_BoolValue{BoolValue: true}},
	}}, []byte("\x04truet\x01")},
	{"table value", &types.Struct{Fields: map[string]*types.Value{
		"table": {Kind: &types.Value_StructValue{StructValue: &types.Struct{Fields: map[string]*types.Value{
			"n": {Kind: &types.Value_NumberValue{NumberValue: 42}},
		}}}},
	}}, []byte("\x05tableF\x00\x00\x00\x04\x01nb\x2a")},
	{"list value", &types.Struct{Fields: map[string]*types.Value{
		"list": {Kind: &types.Value_ListValue{ListValue: &types.ListValue{Values: []*types.Value{
			{Kind: &types.Value_NumberValue{NumberValue: 1}},
			{Kind: &types.Value_NumberValue{NumberValue: 2}},
			{Kind: &types.Value_NumberValue{NumberValue: 3}},
		}}}},
	}}, []byte("\x04listA\x00\x00\x00\x06b\x01b\x02b\x03")},
}

func TestMarshalTable(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := require.New(t)

			data, err := MarshalTable(test.table)
			assert.NoError(err)
			assert.Equal(test.data, data)
		})
	}
}

func TestUnmarshalTable(t *testing.T) {
	for _, test := range tests {
		if test.name == "nil fields" || test.name == "empty fields" {
			// non-canonical
			continue
		}

		t.Run(test.name, func(t *testing.T) {
			assert := require.New(t)

			table, err := UnmarshalTable(test.data)
			assert.NoError(err)
			assert.Equal(test.table, table)
		})
	}
}
