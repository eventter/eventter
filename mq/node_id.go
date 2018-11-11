package mq

import (
	"strconv"
	"strings"
)

func NodeIDToString(id uint64) string {
	s := strconv.FormatUint(id, 16)
	if l := len(s); l < 16 {
		s = strings.Repeat("0", 16-l) + s
	}
	return s
}

func NodeIDFromString(s string) (uint64, error) {
	return strconv.ParseUint(s, 16, 64)
}

func MustIDFromString(s string) uint64 {
	id, err := NodeIDFromString(s)
	if err != nil {
		panic(err)
	}
	return id
}
