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

func NodeIDFromString(id string) (uint64, error) {
	return strconv.ParseUint(id, 16, 64)
}
