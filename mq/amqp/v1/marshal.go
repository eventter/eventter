package v1

import (
	"bytes"
	"math"
)

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
