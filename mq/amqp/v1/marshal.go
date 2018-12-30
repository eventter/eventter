package v1

import (
	"bytes"
	"math"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

func marshalBoolean(src bool, buf *bytes.Buffer) error {
	if src {
		buf.WriteByte(BooleanTrueEncoding)
	} else {
		buf.WriteByte(BooleanFalseEncoding)
	}
	return nil
}

func marshalUbyte(src uint8, buf *bytes.Buffer) error {
	buf.WriteByte(UbyteEncoding)
	buf.WriteByte(src)
	return nil
}

func marshalByte(src int8, buf *bytes.Buffer) error {
	buf.WriteByte(ByteEncoding)
	buf.WriteByte(byte(src))
	return nil
}

func marshalUshort(src uint16, buf *bytes.Buffer) error {
	var x [2]byte
	buf.WriteByte(UshortEncoding)
	endian.PutUint16(x[:], src)
	buf.Write(x[:])
	return nil
}

func marshalShort(src int16, buf *bytes.Buffer) error {
	var x [2]byte
	buf.WriteByte(ShortEncoding)
	endian.PutUint16(x[:], uint16(src))
	buf.Write(x[:])
	return nil
}

func marshalUint(src uint32, buf *bytes.Buffer) error {
	var x [4]byte
	if src == 0 {
		buf.WriteByte(Uint0Encoding)
	} else if src <= math.MaxUint8 {
		buf.WriteByte(UintSmalluintEncoding)
		buf.WriteByte(byte(src))
	} else {
		buf.WriteByte(UintEncoding)
		endian.PutUint32(x[:], src)
		buf.Write(x[:])
	}
	return nil
}

func marshalInt(src int32, buf *bytes.Buffer) error {
	var x [4]byte
	if src >= math.MinInt8 && src <= math.MaxInt8 {
		buf.WriteByte(IntSmallintEncoding)
		buf.WriteByte(byte(int8(src)))
	} else {
		buf.WriteByte(IntEncoding)
		endian.PutUint32(x[:], uint32(src))
		buf.Write(x[:])
	}
	return nil
}

func marshalUlong(src uint64, buf *bytes.Buffer) error {
	var x [8]byte
	if src == 0 {
		buf.WriteByte(Ulong0Encoding)
	} else if src <= math.MaxUint8 {
		buf.WriteByte(UlongSmallulongEncoding)
		buf.WriteByte(byte(src))
	} else {
		buf.WriteByte(UlongEncoding)
		endian.PutUint64(x[:], src)
		buf.Write(x[:])
	}
	return nil
}

func marshalLong(src int64, buf *bytes.Buffer) error {
	var x [8]byte
	if src >= math.MinInt8 && src <= math.MaxInt8 {
		buf.WriteByte(LongSmalllongEncoding)
		buf.WriteByte(byte(int8(src)))
	} else {
		buf.WriteByte(LongEncoding)
		endian.PutUint64(x[:], uint64(src))
		buf.Write(x[:])
	}
	return nil
}

func marshalFloat(src float32, buf *bytes.Buffer) error {
	var x [4]byte
	buf.WriteByte(FloatIeee754Encoding)
	endian.PutUint32(x[:], math.Float32bits(src))
	buf.Write(x[:])
	return nil
}

func marshalDouble(src float64, buf *bytes.Buffer) error {
	var x [8]byte
	buf.WriteByte(DoubleIeee754Encoding)
	endian.PutUint64(x[:], math.Float64bits(src))
	buf.Write(x[:])
	return nil
}

func marshalChar(src rune, buf *bytes.Buffer) error {
	var x [4]byte
	buf.WriteByte(CharUtf32Encoding)
	endian.PutUint32(x[:], uint32(src))
	buf.Write(x[:])
	return nil
}

func marshalTimestamp(src time.Time, buf *bytes.Buffer) error {
	var x [8]byte
	buf.WriteByte(TimestampMs64Encoding)
	millis := src.UnixNano() / 1000000
	if millis > 0 {
		endian.PutUint64(x[:], uint64(millis))
	}
	buf.Write(x[:])
	return nil
}

func marshalUUID(src UUID, buf *bytes.Buffer) error {
	buf.WriteByte(UUIDEncoding)
	buf.Write(src[:])
	return nil
}

func marshalBinary(src []byte, buf *bytes.Buffer) error {
	var x [4]byte
	if len(src) <= math.MaxUint8 {
		buf.WriteByte(BinaryVbin8Encoding)
		buf.WriteByte(uint8(len(src)))
		buf.Write(src)
	} else {
		buf.WriteByte(BinaryVbin32Encoding)
		endian.PutUint32(x[:], uint32(len(src)))
		buf.Write(x[:])
		buf.Write(src)
	}
	return nil
}

func marshalString(src string, buf *bytes.Buffer) error {
	var x [4]byte
	if len(src) <= math.MaxUint8 {
		buf.WriteByte(StringStr8Utf8Encoding)
		buf.WriteByte(uint8(len(src)))
		buf.WriteString(src)
	} else {
		buf.WriteByte(StringStr32Utf8Encoding)
		endian.PutUint32(x[:], uint32(len(src)))
		buf.Write(x[:])
		buf.WriteString(src)
	}
	return nil
}

func marshalSymbol(src string, buf *bytes.Buffer) error {
	return marshalString(src, buf)
}

func marshalMap(src *types.Struct, buf *bytes.Buffer) error {
	var x [4]byte
	if src == nil || src.Fields == nil {
		buf.WriteByte(Map8Encoding)
		buf.WriteByte(0)
		return nil
	}

	itemBuf := bytes.Buffer{}
	for key, value := range src.Fields {
		err := marshalString(key, &itemBuf)
		if err != nil {
			return errors.Wrapf(err, "marshal map item %s failed", key)
		}
		err = marshalValue(value, &itemBuf)
		if err != nil {
			return errors.Wrapf(err, "marshal map item %s value failed", key)
		}
	}

	if itemBuf.Len()+1 <= math.MaxUint8 && len(src.Fields)*2 <= math.MaxUint8 {
		buf.WriteByte(Map8Encoding)
		buf.WriteByte(uint8(itemBuf.Len() + 1))
		buf.WriteByte(uint8(len(src.Fields) * 2))
	} else {
		buf.WriteByte(Map32Encoding)
		endian.PutUint32(x[:], uint32(itemBuf.Len()+4))
		buf.Write(x[:])
		endian.PutUint32(x[:], uint32(len(src.Fields)*2))
		buf.Write(x[:])
	}

	buf.Write(itemBuf.Bytes())

	return nil
}

func marshalList(src *types.ListValue, buf *bytes.Buffer) error {
	if src == nil || len(src.Values) == 0 {
		buf.WriteByte(List0Encoding)
		return nil
	}

	itemBuf := bytes.Buffer{}
	for index, value := range src.Values {
		err := marshalValue(value, &itemBuf)
		if err != nil {
			return errors.Wrapf(err, "marshal list item at index %d failed", index)
		}
	}

	if itemBuf.Len()+1 <= math.MaxUint8 && len(src.Values) <= math.MaxUint8 {
		buf.WriteByte(List8Encoding)
		buf.WriteByte(uint8(itemBuf.Len() + 1))
		buf.WriteByte(uint8(len(src.Values)))
	} else {
		var x [4]byte
		buf.WriteByte(List32Encoding)
		endian.PutUint32(x[:], uint32(itemBuf.Len()+4))
		buf.Write(x[:])
		endian.PutUint32(x[:], uint32(len(src.Values)))
		buf.Write(x[:])
	}

	buf.Write(itemBuf.Bytes())

	return nil
}

func marshalValue(src *types.Value, buf *bytes.Buffer) error {
	switch src := src.Kind.(type) {
	case *types.Value_NullValue:
		buf.WriteByte(NullEncoding)
		return nil
	case *types.Value_NumberValue:
		if src.NumberValue == math.Round(src.NumberValue) {
			intValue := int64(src.NumberValue)
			if intValue >= math.MinInt8 && intValue <= math.MaxInt8 {
				return marshalByte(int8(intValue), buf)
			} else if intValue >= math.MinInt16 && intValue <= math.MaxInt16 {
				return marshalShort(int16(intValue), buf)
			} else if intValue >= math.MinInt32 && intValue <= math.MaxInt32 {
				return marshalInt(int32(intValue), buf)
			} else {
				return marshalLong(intValue, buf)
			}
		} else {
			return marshalDouble(src.NumberValue, buf)
		}
	case *types.Value_StringValue:
		return marshalString(src.StringValue, buf)
	case *types.Value_BoolValue:
		return marshalBoolean(src.BoolValue, buf)
	case *types.Value_StructValue:
		return marshalMap(src.StructValue, buf)
	case *types.Value_ListValue:
		return marshalList(src.ListValue, buf)
	default:
		return errors.Errorf("unhandled kind %T", src)
	}
}

func marshalSymbolArray(src []string, buf *bytes.Buffer) error {
	var x [4]byte

	if len(src) == 0 {
		buf.WriteByte(Array8Encoding)
		buf.WriteByte(0)
		return nil
	}

	max := 0
	for _, s := range src {
		if l := len(s); l > max {
			max = l
		}
	}

	itemBuf := bytes.Buffer{}
	for _, s := range src {
		if max <= math.MaxUint8 {
			itemBuf.WriteByte(uint8(len(s)))
		} else {
			endian.PutUint32(x[:], uint32(len(s)))
			itemBuf.Write(x[:])
		}
		itemBuf.WriteString(s)
	}

	if itemBuf.Len()+2 <= math.MaxUint8 && len(src) <= math.MaxUint8 {
		buf.WriteByte(Array8Encoding)
		buf.WriteByte(uint8(itemBuf.Len() + 2))
		buf.WriteByte(uint8(len(src)))
		if max <= math.MaxUint8 {
			buf.WriteByte(SymbolSym8Encoding)
		} else {
			buf.WriteByte(SymbolSym32Encoding)
		}
	} else {
		buf.WriteByte(Array32Encoding)
		endian.PutUint32(x[:], uint32(itemBuf.Len()+5))
		buf.Write(x[:])
		endian.PutUint32(x[:], uint32(len(src)))
		buf.Write(x[:])
		if max <= math.MaxUint8 {
			buf.WriteByte(SymbolSym8Encoding)
		} else {
			buf.WriteByte(SymbolSym32Encoding)
		}
	}

	buf.Write(itemBuf.Bytes())

	return nil
}

func marshalIETFLanguageTagArray(src []IETFLanguageTag, buf *bytes.Buffer) error {
	var x [4]byte

	if len(src) == 0 {
		buf.WriteByte(Array8Encoding)
		buf.WriteByte(0)
		return nil
	}

	max := 0
	for _, s := range src {
		if l := len(s); l > max {
			max = l
		}
	}

	itemBuf := bytes.Buffer{}
	for _, s := range src {
		if max <= math.MaxUint8 {
			itemBuf.WriteByte(uint8(len(s)))
		} else {
			endian.PutUint32(x[:], uint32(len(s)))
			itemBuf.Write(x[:])
		}
		itemBuf.WriteString(string(s))
	}

	if itemBuf.Len()+2 <= math.MaxUint8 && len(src) <= math.MaxUint8 {
		buf.WriteByte(Array8Encoding)
		buf.WriteByte(uint8(itemBuf.Len() + 2))
		buf.WriteByte(uint8(len(src)))
		if max <= math.MaxUint8 {
			buf.WriteByte(SymbolSym8Encoding)
		} else {
			buf.WriteByte(SymbolSym32Encoding)
		}
	} else {
		buf.WriteByte(Array32Encoding)
		endian.PutUint32(x[:], uint32(itemBuf.Len()+5))
		buf.Write(x[:])
		endian.PutUint32(x[:], uint32(len(src)))
		buf.Write(x[:])
		if max <= math.MaxUint8 {
			buf.WriteByte(SymbolSym8Encoding)
		} else {
			buf.WriteByte(SymbolSym32Encoding)
		}
	}

	buf.Write(itemBuf.Bytes())

	return nil
}

func marshalDeliveryStateUnion(src DeliveryState, buf *bytes.Buffer) error {
	describedBy, ok := src.(DescribedType)
	if !ok {
		return errors.Errorf("marshal delivery-state failed: %T is not described", src)
	}

	buf.WriteByte(DescriptorEncoding)
	err := marshalUlong(describedBy.Descriptor(), buf)
	if err != nil {
		return errors.Wrap(err, "marshal delivery-state failed")
	}

	marshaler, ok := src.(BufferMarshaler)
	if !ok {
		return errors.Errorf("marshal delivery-state failed: %T is not marshaler", src)
	}

	return errors.Wrap(marshaler.MarshalBuffer(buf), "marshal delivery-state failed")
}

func marshalMessageIDUnion(src MessageID, buf *bytes.Buffer) error {
	switch src := src.(type) {
	case MessageIDBinary:
		return marshalBinary([]byte(src), buf)
	case MessageIDString:
		return marshalString(string(src), buf)
	case MessageIDUUID:
		return marshalUUID(UUID(src), buf)
	case MessageIDUlong:
		return marshalUlong(uint64(src), buf)
	default:
		return errors.Errorf("marshal message-id failed: %T not handled", src)
	}
}

func marshalAddressUnion(src Address, buf *bytes.Buffer) error {
	switch src := src.(type) {
	case AddressString:
		return marshalString(string(src), buf)
	default:
		return errors.Errorf("marshal address failed: %T not handled", src)
	}
}

func marshalOutcomeUnion(src Outcome, buf *bytes.Buffer) error {
	describedBy, ok := src.(DescribedType)
	if !ok {
		return errors.Errorf("marshal outcome failed: %T is not described", src)
	}

	buf.WriteByte(DescriptorEncoding)
	err := marshalUlong(describedBy.Descriptor(), buf)
	if err != nil {
		return errors.Wrap(err, "marshal outcome failed")
	}

	marshaler, ok := src.(BufferMarshaler)
	if !ok {
		return errors.Errorf("marshal outcome failed: %T is not marshaler", src)
	}

	return errors.Wrap(marshaler.MarshalBuffer(buf), "marshal outcome failed")
}
