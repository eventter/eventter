package v0

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

var endian = binary.BigEndian

func marshalTable(table *types.Struct) ([]byte, error) {
	if table == nil || table.Fields == nil {
		return nil, nil
	}

	buf := bytes.Buffer{}
	for name, value := range table.Fields {
		if l := len(name); l > math.MaxUint8 {
			return nil, errors.Errorf("field name can be at most %d bytes long, got %d bytes", math.MaxUint8, l)
		} else {
			buf.WriteByte(byte(l))
			buf.WriteString(name)
		}

		if err := marshalValue(value, &buf); err != nil {
			return nil, errors.Wrapf(err, "field %s", name)
		}
	}

	return buf.Bytes(), nil
}

func marshalArray(values []*types.Value) ([]byte, error) {
	buf := bytes.Buffer{}
	for index, value := range values {
		if err := marshalValue(value, &buf); err != nil {
			return nil, errors.Wrapf(err, "index %d", index)
		}
	}
	return buf.Bytes(), nil
}

func marshalValue(value *types.Value, buf *bytes.Buffer) error {
	var x [8]byte
	switch value := value.Kind.(type) {
	case *types.Value_NullValue:
		buf.WriteByte('V')
	case *types.Value_NumberValue:
		if value.NumberValue == math.Round(value.NumberValue) {
			intValue := int64(value.NumberValue)
			if intValue >= math.MinInt8 && intValue <= math.MaxInt8 {
				buf.WriteByte('b')
				buf.WriteByte(byte(intValue))
			} else if intValue >= math.MinInt16 && intValue <= math.MaxInt16 {
				buf.WriteByte('s')
				endian.PutUint16(x[:2], uint16(intValue))
				buf.Write(x[:2])
			} else if intValue >= math.MinInt32 && intValue <= math.MaxInt32 {
				buf.WriteByte('I')
				endian.PutUint32(x[:4], uint32(intValue))
				buf.Write(x[:4])
			} else {
				buf.WriteByte('l')
				endian.PutUint64(x[:8], uint64(intValue))
				buf.Write(x[:8])
			}
		} else {
			buf.WriteByte('d')
			floatValue := value.NumberValue
			endian.PutUint64(x[:8], math.Float64bits(floatValue))
			buf.Write(x[:8])
		}
	case *types.Value_StringValue:
		l := len(value.StringValue)
		if l <= math.MaxUint32 {
			buf.WriteByte('S')
			endian.PutUint32(x[:4], uint32(l))
			buf.Write(x[:4])
			buf.WriteString(value.StringValue)
		} else {
			return errors.Errorf("string value can be at most %d bytes long, got %d bytes", math.MaxUint32, l)
		}
	case *types.Value_BoolValue:
		buf.WriteByte('t')
		if value.BoolValue {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	case *types.Value_StructValue:
		if tableBuf, err := marshalTable(value.StructValue); err != nil {
			return errors.Wrap(err, "table marshal failed")
		} else if l := len(tableBuf); l > math.MaxUint32 {
			return errors.Errorf("table value can be at most %d bytes long, got %d bytes", math.MaxUint32, l)
		} else {
			buf.WriteByte('F')
			endian.PutUint32(x[:4], uint32(l))
			buf.Write(x[:4])
			buf.Write(tableBuf)
		}
	case *types.Value_ListValue:
		if arrayBuf, err := marshalArray(value.ListValue.Values); err != nil {
			return errors.Wrap(err, "array marshal failed")
		} else if l := len(arrayBuf); l > math.MaxUint32 {
			return errors.Errorf("array value can be at most %d bytes long, got %d bytes", math.MaxUint32, l)
		} else {
			buf.WriteByte('A')
			endian.PutUint32(x[:4], uint32(l))
			buf.Write(x[:4])
			buf.Write(arrayBuf)
		}
	default:
		return errors.Errorf("unhandled kind %T", value)
	}
	return nil
}

func unmarshalTable(data []byte) (*types.Struct, error) {
	if len(data) == 0 {
		return nil, nil
	}
	buf := bytes.NewBuffer(data)
	fields := make(map[string]*types.Value)
	for {
		lb, err := buf.ReadByte()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "read field name failed")
		}

		l := int(lb)
		nameBytes := buf.Next(l)
		if len(nameBytes) < l {
			return nil, errors.New("read field name failed")
		}
		name := string(nameBytes)

		if value, err := unmarshalValue(buf); err != nil {
			return nil, errors.Wrapf(err, "field %s", name)
		} else {
			fields[name] = value
		}
	}

	return &types.Struct{
		Fields: fields,
	}, nil
}

func unmarshalArray(data []byte) (*types.ListValue, error) {
	if len(data) == 0 {
		return &types.ListValue{}, nil
	}

	buf := bytes.NewBuffer(data)
	var values []*types.Value
	for {
		if _, err := buf.ReadByte(); err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrapf(err, "index %d", len(values))
		} else {
			if err := buf.UnreadByte(); err != nil {
				return nil, errors.Wrapf(err, "index %d", len(values))
			}
		}
		if value, err := unmarshalValue(buf); err != nil {
			return nil, errors.Wrapf(err, "index %d", len(values))
		} else {
			values = append(values, value)
		}
	}

	return &types.ListValue{Values: values}, nil
}

func unmarshalValue(buf *bytes.Buffer) (*types.Value, error) {
	var x [8]byte

	tb, err := buf.ReadByte()
	if err != nil {
		return nil, errors.Wrap(err, "read field type failed")
	}

	value := &types.Value{}
	switch tb {
	case 't':
		bb, err := buf.ReadByte()
		if err != nil {
			return nil, errors.Wrap(err, "read bool failed")
		}
		value.Kind = &types.Value_BoolValue{BoolValue: bb != 0}
	case 'b':
		bb, err := buf.ReadByte()
		if err != nil {
			return nil, errors.Wrap(err, "read int8 failed")
		}
		value.Kind = &types.Value_NumberValue{NumberValue: float64(int8(bb))}
	case 'B':
		bb, err := buf.ReadByte()
		if err != nil {
			return nil, errors.Wrap(err, "read uint8 failed")
		}
		value.Kind = &types.Value_NumberValue{NumberValue: float64(uint8(bb))}
	case 'U':
		fallthrough
	case 's':
		if n, err := buf.Read(x[:2]); err != nil {
			return nil, errors.Wrap(err, "read int16 failed")
		} else if n < 2 {
			return nil, errors.New("read int16 failed")
		}
		value.Kind = &types.Value_NumberValue{NumberValue: float64(int16(endian.Uint16(x[:2])))}
	case 'u':
		if n, err := buf.Read(x[:2]); err != nil {
			return nil, errors.Wrap(err, "read uint16 failed")
		} else if n < 2 {
			return nil, errors.New("read uint16 failed")
		}
		value.Kind = &types.Value_NumberValue{NumberValue: float64(uint16(endian.Uint16(x[:2])))}
	case 'I':
		if n, err := buf.Read(x[:4]); err != nil {
			return nil, errors.Wrap(err, "read int32 failed")
		} else if n < 4 {
			return nil, errors.New("read int32 failed")
		}
		value.Kind = &types.Value_NumberValue{NumberValue: float64(int32(endian.Uint32(x[:4])))}
	case 'i':
		if n, err := buf.Read(x[:4]); err != nil {
			return nil, errors.Wrap(err, "read uint32 failed")
		} else if n < 4 {
			return nil, errors.New("read uint32 failed")
		}
		value.Kind = &types.Value_NumberValue{NumberValue: float64(uint32(endian.Uint32(x[:4])))}
	case 'l':
		if n, err := buf.Read(x[:8]); err != nil {
			return nil, errors.Wrap(err, "read int64 failed")
		} else if n < 8 {
			return nil, errors.New("read int64 failed")
		}
		value.Kind = &types.Value_NumberValue{NumberValue: float64(int64(endian.Uint64(x[:8])))}
	case 'L':
		if n, err := buf.Read(x[:8]); err != nil {
			return nil, errors.Wrap(err, "read uint64 failed")
		} else if n < 8 {
			return nil, errors.New("read uint64 failed")
		}
		value.Kind = &types.Value_NumberValue{NumberValue: float64(uint64(endian.Uint64(x[:8])))}
	case 'f':
		if n, err := buf.Read(x[:4]); err != nil {
			return nil, errors.Wrap(err, "read float32 failed")
		} else if n < 4 {
			return nil, errors.New("read float32 failed")
		}
		value.Kind = &types.Value_NumberValue{NumberValue: float64(math.Float32frombits(endian.Uint32(x[:4])))}
	case 'd':
		if n, err := buf.Read(x[:8]); err != nil {
			return nil, errors.Wrap(err, "read float64 failed")
		} else if n < 8 {
			return nil, errors.New("read float64 failed")
		}
		value.Kind = &types.Value_NumberValue{NumberValue: float64(math.Float64frombits(endian.Uint64(x[:8])))}
	case 'D':
		return nil, errors.New("decimal not implemented")
	case 'x': // bytes
		fallthrough
	case 'S':
		if n, err := buf.Read(x[:4]); err != nil {
			return nil, errors.Wrap(err, "read longstr failed")
		} else if n < 4 {
			return nil, errors.New("read longstr failed")
		}
		l := int(endian.Uint32(x[:4]))
		s := buf.Next(l)
		if len(s) < l {
			return nil, errors.New("read longstr failed")
		}
		value.Kind = &types.Value_StringValue{StringValue: string(s)}
	case 'A':
		if n, err := buf.Read(x[:4]); err != nil {
			return nil, errors.Wrap(err, "read array failed")
		} else if n < 4 {
			return nil, errors.New("read array failed")
		}
		l := int(endian.Uint32(x[:4]))
		data := buf.Next(l)
		if len(data) < l {
			return nil, errors.New("read array failed")
		}

		if arrayValue, err := unmarshalArray(data); err != nil {
			return nil, errors.Wrap(err, "read array failed")
		} else {
			value.Kind = &types.Value_ListValue{ListValue: arrayValue}
		}
	case 'T':
		return nil, errors.New("timestamp not implemented")
	case 'F':
		if n, err := buf.Read(x[:4]); err != nil {
			return nil, errors.Wrap(err, "read table failed")
		} else if n < 4 {
			return nil, errors.New("read table failed")
		}
		l := int(endian.Uint32(x[:4]))
		data := buf.Next(l)
		if len(data) < l {
			return nil, errors.New("read table failed")
		}

		if tableValue, err := unmarshalTable(data); err != nil {
			return nil, errors.Wrap(err, "read table failed")
		} else {
			value.Kind = &types.Value_StructValue{StructValue: tableValue}
		}
	case 'V':
		value.Kind = &types.Value_NullValue{}
	default:
		return nil, errors.Errorf("unhandled field type %c", tb)
	}

	return value, nil
}
