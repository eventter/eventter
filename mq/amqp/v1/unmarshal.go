package v1

import (
	"bytes"
	"encoding/base64"
	"math"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

func unmarshalBoolean(dst *bool, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = false
	case BooleanEncoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal boolean failed")
		}
		*dst = v != 0
	case BooleanTrueEncoding:
		*dst = true
	case BooleanFalseEncoding:
		*dst = false
	default:
		return errors.Errorf("unmarshal boolean failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalUbyte(dst *uint8, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = 0
	case UbyteEncoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal ubyte failed")
		}
		*dst = uint8(v)
	default:
		return errors.Errorf("unmarshal ubyte failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalByte(dst *int8, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = 0
	case ByteEncoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal byte failed")
		}
		*dst = int8(v)
	default:
		return errors.Errorf("unmarshal byte failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalUshort(dst *uint16, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = 0
	case UshortEncoding:
		if buf.Len() < 2 {
			return errors.New("unmarshal ushort failed: buffer underflow")
		}
		*dst = endian.Uint16(buf.Next(2))
	default:
		return errors.Errorf("unmarshal ushort failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalShort(dst *int16, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = 0
	case ShortEncoding:
		if buf.Len() < 2 {
			return errors.New("unmarshal short failed: buffer underflow")
		}
		*dst = int16(endian.Uint16(buf.Next(2)))
	default:
		return errors.Errorf("unmarshal short failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalUint(dst *uint32, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = 0
	case Uint0Encoding:
		*dst = 0
	case UintSmalluintEncoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal uint failed")
		}
		*dst = uint32(uint8(v))
	case UintEncoding:
		if buf.Len() < 4 {
			return errors.New("unmarshal uint failed: buffer underflow")
		}
		*dst = endian.Uint32(buf.Next(4))
	default:
		return errors.Errorf("unmarshal uint failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalInt(dst *int32, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = 0
	case IntSmallintEncoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal int failed")
		}
		*dst = int32(int8(v))
	case IntEncoding:
		if buf.Len() < 4 {
			return errors.New("unmarshal int failed: buffer underflow")
		}
		*dst = int32(endian.Uint32(buf.Next(4)))
	default:
		return errors.Errorf("unmarshal int failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalUlong(dst *uint64, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = 0
	case Ulong0Encoding:
		*dst = 0
	case UlongSmallulongEncoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal ulong failed")
		}
		*dst = uint64(uint8(v))
	case UlongEncoding:
		if buf.Len() < 8 {
			return errors.New("unmarshal ulong failed: buffer underflow")
		}
		*dst = endian.Uint64(buf.Next(8))
	default:
		return errors.Errorf("unmarshal ulong failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalLong(dst *int64, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = 0
	case LongSmalllongEncoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal long failed")
		}
		*dst = int64(int8(v))
	case LongEncoding:
		if buf.Len() < 8 {
			return errors.New("unmarshal long failed: buffer underflow")
		}
		*dst = int64(endian.Uint64(buf.Next(8)))
	default:
		return errors.Errorf("unmarshal long failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalFloat(dst *float32, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = 0
	case FloatIeee754Encoding:
		if buf.Len() < 4 {
			return errors.New("unmarshal float failed: buffer underflow")
		}
		*dst = math.Float32frombits(endian.Uint32(buf.Next(4)))
	default:
		return errors.Errorf("unmarshal float failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalDouble(dst *float64, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = 0
	case DoubleIeee754Encoding:
		if buf.Len() < 8 {
			return errors.New("unmarshal double failed: buffer underflow")
		}
		*dst = math.Float64frombits(endian.Uint64(buf.Next(8)))
	default:
		return errors.Errorf("unmarshal double failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalChar(dst *rune, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = 0
	case CharUtf32Encoding:
		if buf.Len() < 4 {
			return errors.New("unmarshal char failed: buffer underflow")
		}
		*dst = rune(endian.Uint32(buf.Next(4)))
	default:
		return errors.Errorf("unmarshal char failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalTimestamp(dst *time.Time, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = time.Time{}
	case TimestampMs64Encoding:
		if buf.Len() < 8 {
			return errors.New("unmarshal timestamp failed: buffer underflow")
		}
		millis := int64(endian.Uint64(buf.Next(8)))
		*dst = time.Unix(millis/1000, (millis%1000)*1000*1000)
	default:
		return errors.Errorf("unmarshal timestamp failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalUUID(dst *UUID, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = UUID{}
	case UUIDEncoding:
		if buf.Len() < 16 {
			return errors.New("unmarshal uuid failed: buffer underflow")
		}
		_, err := buf.Read(dst[:16])
		if err != nil {
			return errors.Wrap(err, "unmarshal uuid failed")
		}
	default:
		return errors.Errorf("unmarshal uuid failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalBinary(dst *[]byte, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = nil
	case BinaryVbin8Encoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal binary failed")
		}
		l := int(v)
		if buf.Len() < l {
			return errors.New("unmarshal binary failed: buffer underflow")
		}
		*dst = make([]byte, l)
		copy(*dst, buf.Next(l))
	case BinaryVbin32Encoding:
		if buf.Len() < 4 {
			return errors.New("unmarshal binary failed: buffer underflow")
		}
		l := int(endian.Uint32(buf.Next(4)))
		if buf.Len() < l {
			return errors.New("unmarshal binary failed: buffer underflow")
		}
		*dst = make([]byte, l)
		copy(*dst, buf.Next(l))
	default:
		return errors.Errorf("unmarshal binary failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalString(dst *string, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = ""
	case StringStr8Utf8Encoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal string failed")
		}
		l := int(v)
		if buf.Len() < l {
			return errors.New("unmarshal string failed: buffer underflow")
		}
		*dst = string(buf.Next(l))
	case StringStr32Utf8Encoding:
		if buf.Len() < 4 {
			return errors.New("unmarshal string failed: buffer underflow")
		}
		l := int(endian.Uint32(buf.Next(4)))
		if buf.Len() < l {
			return errors.New("unmarshal string failed: buffer underflow")
		}
		*dst = string(buf.Next(l))
	default:
		return errors.Errorf("unmarshal string failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalSymbol(dst *string, constructor byte, buf *bytes.Buffer) error {
	switch constructor {
	case NullEncoding:
		*dst = ""
	case SymbolSym8Encoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal symbol failed")
		}
		l := int(v)
		if buf.Len() < l {
			return errors.New("unmarshal symbol failed: buffer underflow")
		}
		*dst = string(buf.Next(l))
	case SymbolSym32Encoding:
		if buf.Len() < 4 {
			return errors.New("unmarshal symbol failed: buffer underflow")
		}
		l := int(endian.Uint32(buf.Next(4)))
		if buf.Len() < l {
			return errors.New("unmarshal symbol failed: buffer underflow")
		}
		*dst = string(buf.Next(l))
	default:
		return errors.Errorf("unmarshal symbol failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalMap(dst **types.Struct, constructor byte, buf *bytes.Buffer) error {
	var size int
	switch constructor {
	case NullEncoding:
		size = 0
	case Map8Encoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal map failed")
		}
		size = int(v)
	case Map32Encoding:
		if buf.Len() < 4 {
			return errors.New("unmarshal map failed: buffer underflow")
		}
		size = int(endian.Uint32(buf.Next(4)))
	default:
		return errors.Errorf("unmarshal map failed: unexpected constructor 0x%02x", constructor)
	}

	if size == 0 {
		*dst = &types.Struct{Fields: make(map[string]*types.Value)}
		return nil
	}

	if buf.Len() < size {
		return errors.New("unmarshal map failed: buffer underflow")
	}

	itemBuf := bytes.NewBuffer(buf.Next(size))

	var count int
	switch constructor {
	case Map8Encoding:
		v, err := itemBuf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal map failed")
		}
		count = int(v)
	case Map32Encoding:
		if itemBuf.Len() < 4 {
			return errors.New("unmarshal map failed: buffer underflow")
		}
		count = int(endian.Uint32(itemBuf.Next(4)))
	default:
		return errors.Errorf("unmarshal map failed: unexpected constructor 0x%02x", constructor)
	}

	if count%2 != 0 {
		return errors.Errorf("unmarshal map failed: must have even number of elements, got %d", count)
	}

	*dst = &types.Struct{Fields: make(map[string]*types.Value)}
	for i := 0; i < count/2; i++ {
		var key types.Value
		err := unmarshalValue(&key, itemBuf)
		if err != nil {
			return errors.Wrapf(err, "unmarshal map item %d failed", i)
		}

		keyString, ok := key.Kind.(*types.Value_StringValue)
		if !ok {
			return errors.Errorf("unmarshal map item %d failed: non-string key %T", i, key.Kind)
		}

		var value types.Value
		err = unmarshalValue(&value, itemBuf)
		if err != nil {
			return errors.Wrapf(err, "unmarshal map item %d (key %s) failed", i, keyString.StringValue)
		}

		(*dst).Fields[keyString.StringValue] = &value
	}

	return nil
}

func unmarshalList(dst **types.ListValue, constructor byte, buf *bytes.Buffer) error {
	var size int
	switch constructor {
	case NullEncoding:
		size = 0
	case List0Encoding:
		size = 0
	case List8Encoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal list failed")
		}
		size = int(v)
	case List32Encoding:
		if buf.Len() < 4 {
			return errors.New("unmarshal list failed: buffer underflow")
		}
		size = int(endian.Uint32(buf.Next(4)))
	default:
		return errors.Errorf("unmarshal list failed: unexpected constructor 0x%02x", constructor)
	}

	if size == 0 {
		*dst = &types.ListValue{Values: make([]*types.Value, 0)}
		return nil
	}

	itemBuf := bytes.NewBuffer(buf.Next(size))

	var count int
	switch constructor {
	case NullEncoding:
		count = 0
	case List0Encoding:
		count = 0
	case List8Encoding:
		v, err := itemBuf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal list failed")
		}
		count = int(v)
	case List32Encoding:
		if itemBuf.Len() < 4 {
			return errors.New("unmarshal list failed: buffer underflow")
		}
		count = int(endian.Uint32(itemBuf.Next(4)))
	default:
		return errors.Errorf("unmarshal list failed: unexpected constructor 0x%02x", constructor)
	}

	*dst = &types.ListValue{}
	for i := 0; i < count; i++ {
		var value types.Value
		err := unmarshalValue(&value, itemBuf)
		if err != nil {
			return errors.Wrapf(err, "unmarshal list item %d failed", i)
		}
		(*dst).Values = append((*dst).Values, &value)
	}

	return nil
}

func unmarshalArray(dst **types.ListValue, constructor byte, buf *bytes.Buffer) error {
	var size, count int
	var itemBuf *bytes.Buffer
	switch constructor {
	case NullEncoding:
		*dst = nil
		return nil
	case Array8Encoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal array failed")
		}
		size = int(v)
		v, err = buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal array failed")
		}
		count = int(v)
		itemBuf = bytes.NewBuffer(buf.Next(size - 1))
	case Array32Encoding:
		if buf.Len() < 8 {
			return errors.New("unmarshal array failed: buffer underflow")
		}
		size = int(endian.Uint32(buf.Next(4)))
		count = int(endian.Uint32(buf.Next(4)))
		itemBuf = bytes.NewBuffer(buf.Next(size - 4))
	default:
		return errors.Errorf("unmarshal array failed: unexpected constructor 0x%02x", constructor)
	}

	if count == 0 {
		*dst = &types.ListValue{Values: make([]*types.Value, 0)}
		return nil
	}

	valueConstructor, err := itemBuf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "unmarshal array failed")
	}

	*dst = &types.ListValue{}
	for i := 0; i < count; i++ {
		var value types.Value
		err := unmarshalValueWithConstructor(&value, valueConstructor, itemBuf)
		if err != nil {
			return errors.Wrapf(err, "unmarshal array item %d failed", i)
		}
		(*dst).Values = append((*dst).Values, &value)
	}

	return nil
}

func unmarshalValue(dst *types.Value, buf *bytes.Buffer) error {
	constructor, err := buf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "read constructor failed")
	}
	return unmarshalValueWithConstructor(dst, constructor, buf)
}

func unmarshalValueWithConstructor(dst *types.Value, constructor byte, buf *bytes.Buffer) error {
	*dst = types.Value{}

	switch constructor {
	case NullEncoding:
		dst.Kind = &types.Value_NullValue{}
	case BooleanEncoding:
		fallthrough
	case BooleanFalseEncoding:
		fallthrough
	case BooleanTrueEncoding:
		var v bool
		err := unmarshalBoolean(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_BoolValue{BoolValue: v}
	case UbyteEncoding:
		var v uint8
		err := unmarshalUbyte(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_NumberValue{NumberValue: float64(v)}
	case ByteEncoding:
		var v int8
		err := unmarshalByte(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_NumberValue{NumberValue: float64(v)}
	case UshortEncoding:
		var v uint16
		err := unmarshalUshort(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_NumberValue{NumberValue: float64(v)}
	case ShortEncoding:
		var v int16
		err := unmarshalShort(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_NumberValue{NumberValue: float64(v)}
	case Uint0Encoding:
		fallthrough
	case UintSmalluintEncoding:
		fallthrough
	case UintEncoding:
		var v uint32
		err := unmarshalUint(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_NumberValue{NumberValue: float64(v)}
	case IntSmallintEncoding:
		fallthrough
	case IntEncoding:
		var v int32
		err := unmarshalInt(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_NumberValue{NumberValue: float64(v)}
	case Ulong0Encoding:
		fallthrough
	case UlongSmallulongEncoding:
		fallthrough
	case UlongEncoding:
		var v uint64
		err := unmarshalUlong(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_NumberValue{NumberValue: float64(v)}
	case LongSmalllongEncoding:
		fallthrough
	case LongEncoding:
		var v int64
		err := unmarshalLong(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_NumberValue{NumberValue: float64(v)}
	case FloatIeee754Encoding:
		var v float32
		err := unmarshalFloat(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_NumberValue{NumberValue: float64(v)}
	case DoubleIeee754Encoding:
		var v float64
		err := unmarshalDouble(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_NumberValue{NumberValue: float64(v)}
	case Decimal32Ieee754Encoding:
		fallthrough
	case Decimal64Ieee754Encoding:
		fallthrough
	case Decimal128Ieee754Encoding:
		return errors.New("unmarshal value failed: decimals not implemented")
	case CharUtf32Encoding:
		var v rune
		err := unmarshalChar(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_NumberValue{NumberValue: float64(v)}
	case TimestampMs64Encoding:
		var v time.Time
		err := unmarshalTimestamp(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		// serialized as ISO 8601 string to milliseconds
		dst.Kind = &types.Value_StringValue{StringValue: v.Format("2006-01-02T15:04:05.999Z07:00")}
	case UUIDEncoding:
		var v UUID
		err := unmarshalUUID(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_StringValue{StringValue: v.String()}
	case BinaryVbin8Encoding:
		fallthrough
	case BinaryVbin32Encoding:
		var v []byte
		err := unmarshalBinary(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString(v)}
	case StringStr8Utf8Encoding:
		fallthrough
	case StringStr32Utf8Encoding:
		var v string
		err := unmarshalString(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_StringValue{StringValue: v}
	case SymbolSym8Encoding:
		fallthrough
	case SymbolSym32Encoding:
		var v string
		err := unmarshalSymbol(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_StringValue{StringValue: v}
	case List0Encoding:
		fallthrough
	case List8Encoding:
	case List32Encoding:
		var v *types.ListValue
		err := unmarshalList(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_ListValue{ListValue: v}
	case Map8Encoding:
		fallthrough
	case Map32Encoding:
		var v *types.Struct
		err := unmarshalMap(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_StructValue{StructValue: v}
	case Array8Encoding:
		fallthrough
	case Array32Encoding:
		var v *types.ListValue
		err := unmarshalArray(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal value failed")
		}
		dst.Kind = &types.Value_ListValue{ListValue: v}
	default:
		return errors.Errorf("unmarshal value failed: unexpected constructor 0x%02x", constructor)
	}

	return nil
}

func unmarshalSymbolArray(dst *[]string, constructor byte, buf *bytes.Buffer) error {
	var size, count int
	var itemBuf *bytes.Buffer
	switch constructor {
	case NullEncoding:
		*dst = nil
		return nil
	case Array8Encoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal symbol array failed")
		}
		size = int(v)
		v, err = buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal symbol array failed")
		}
		count = int(v)
		itemBuf = bytes.NewBuffer(buf.Next(size - 1))
	case Array32Encoding:
		if buf.Len() < 8 {
			return errors.New("unmarshal symbol array failed: buffer underflow")
		}
		size = int(endian.Uint32(buf.Next(4)))
		count = int(endian.Uint32(buf.Next(4)))
		itemBuf = bytes.NewBuffer(buf.Next(size - 4))
	default:
		return errors.Errorf("unmarshal symbol array failed: unexpected constructor 0x%02x", constructor)
	}

	if count == 0 {
		*dst = make([]string, 0)
		return nil
	}

	valueConstructor, err := itemBuf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "unmarshal symbol array failed")
	}

	*dst = make([]string, 0, count)
	for i := 0; i < count; i++ {
		var s string
		err := unmarshalSymbol(&s, valueConstructor, itemBuf)
		if err != nil {
			return errors.Wrapf(err, "unmarshal symbol array item %d failed", i)
		}
		(*dst) = append((*dst), s)
	}

	return nil
}

func unmarshalIETFLanguageTagArray(dst *[]IETFLanguageTag, constructor byte, buf *bytes.Buffer) error {
	var size, count int
	var itemBuf *bytes.Buffer
	switch constructor {
	case NullEncoding:
		*dst = nil
		return nil
	case Array8Encoding:
		v, err := buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal symbol array failed")
		}
		size = int(v)
		v, err = buf.ReadByte()
		if err != nil {
			return errors.Wrap(err, "unmarshal symbol array failed")
		}
		count = int(v)
		itemBuf = bytes.NewBuffer(buf.Next(size - 1))
	case Array32Encoding:
		if buf.Len() < 8 {
			return errors.New("unmarshal symbol array failed: buffer underflow")
		}
		size = int(endian.Uint32(buf.Next(4)))
		count = int(endian.Uint32(buf.Next(4)))
		itemBuf = bytes.NewBuffer(buf.Next(size - 4))
	default:
		return errors.Errorf("unmarshal symbol array failed: unexpected constructor 0x%02x", constructor)
	}

	if count == 0 {
		*dst = make([]IETFLanguageTag, 0)
		return nil
	}

	valueConstructor, err := itemBuf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "unmarshal symbol array failed")
	}

	*dst = make([]IETFLanguageTag, 0, count)
	for i := 0; i < count; i++ {
		var s string
		err := unmarshalSymbol(&s, valueConstructor, itemBuf)
		if err != nil {
			return errors.Wrapf(err, "unmarshal symbol array item %d failed", i)
		}
		(*dst) = append((*dst), IETFLanguageTag(s))
	}

	return nil
}

func unmarshalDeliveryStateUnion(dst *DeliveryState, buf *bytes.Buffer) error {
	descriptorBuf := bytes.NewBuffer(buf.Bytes())

	constructor, err := descriptorBuf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "unmarshal delivery-state failed")
	}

	if constructor != DescriptorEncoding {
		return errors.Errorf("unmarshal delivery-state failed: unexpected constructor 0x%02x", constructor)
	}
	constructor, err = descriptorBuf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "unmarshal delivery-state failed")
	}
	var descriptor uint64
	err = unmarshalUlong(&descriptor, constructor, descriptorBuf)
	if err != nil {
		return errors.Wrap(err, "unmarshal delivery-state failed")
	}

	switch descriptor {
	case AcceptedDescriptor:
		*dst = &Accepted{}
	case ModifiedDescriptor:
		*dst = &Modified{}
	case ReceivedDescriptor:
		*dst = &Received{}
	case RejectedDescriptor:
		*dst = &Rejected{}
	case ReleasedDescriptor:
		*dst = &Released{}
	default:
		return errors.Errorf("unmarshal delivery-state failed: unhandled descriptor 0x%08x:0x%08x", descriptor>>32, descriptor&0xffffffff)
	}

	unmarshaler, ok := (*dst).(BufferUnmarshaler)
	if !ok {
		return errors.Errorf("unmarshal delivery-state failed: %T is not unmarshaler", *dst)
	}

	return errors.Wrap(unmarshaler.UnmarshalBuffer(buf), "unmarshal delivery-state failed")
}

func unmarshalMessageIDUnion(dst *MessageID, buf *bytes.Buffer) error {
	constructor, err := buf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "unmarshal message-id failed")
	}

	switch constructor {
	case BinaryVbin8Encoding:
		fallthrough
	case BinaryVbin32Encoding:
		var v []byte
		err := unmarshalBinary(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal message-id failed")
		}
		*dst = MessageIDBinary(v)
	case StringStr8Utf8Encoding:
		fallthrough
	case StringStr32Utf8Encoding:
		var v string
		err := unmarshalString(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal message-id failed")
		}
		*dst = MessageIDString(v)
	case UUIDEncoding:
		var v UUID
		err := unmarshalUUID(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal message-id failed")
		}
		*dst = MessageIDUUID(v)
	case Ulong0Encoding:
		fallthrough
	case UlongSmallulongEncoding:
		fallthrough
	case UlongEncoding:
		var v uint64
		err := unmarshalUlong(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal message-id failed")
		}
		*dst = MessageIDUlong(v)
	default:
		return errors.Errorf("unmarshal message-id failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalAddressUnion(dst *Address, buf *bytes.Buffer) error {
	constructor, err := buf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "unmarshal address failed")
	}

	switch constructor {
	case StringStr8Utf8Encoding:
		fallthrough
	case StringStr32Utf8Encoding:
		var v string
		err := unmarshalString(&v, constructor, buf)
		if err != nil {
			return errors.Wrap(err, "unmarshal address failed")
		}
		*dst = AddressString(v)
	default:
		return errors.Errorf("unmarshal address failed: unexpected constructor 0x%02x", constructor)
	}
	return nil
}

func unmarshalOutcomeUnion(dst *Outcome, buf *bytes.Buffer) error {
	descriptorBuf := bytes.NewBuffer(buf.Bytes())

	constructor, err := descriptorBuf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "unmarshal outcome failed")
	}

	if constructor != DescriptorEncoding {
		return errors.Errorf("unmarshal outcome failed: unexpected constructor 0x%02x", constructor)
	}
	constructor, err = descriptorBuf.ReadByte()
	if err != nil {
		return errors.Wrap(err, "unmarshal outcome failed")
	}
	var descriptor uint64
	err = unmarshalUlong(&descriptor, constructor, descriptorBuf)
	if err != nil {
		return errors.Wrap(err, "unmarshal outcome failed")
	}

	switch descriptor {
	case AcceptedDescriptor:
		*dst = &Accepted{}
	case ModifiedDescriptor:
		*dst = &Modified{}
	case RejectedDescriptor:
		*dst = &Rejected{}
	case ReleasedDescriptor:
		*dst = &Released{}
	default:
		return errors.Errorf("unmarshal outcome failed: unhandled descriptor 0x%08x:0x%08x", descriptor>>32, descriptor&0xffffffff)
	}

	unmarshaler, ok := (*dst).(BufferUnmarshaler)
	if !ok {
		return errors.Errorf("unmarshal outcome failed: %T is not unmarshaler", *dst)
	}

	return errors.Wrap(unmarshaler.UnmarshalBuffer(buf), "unmarshal outcome failed")
}
