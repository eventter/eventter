package mq

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

func ParseNodeName(name string) (id uint64, hostname string, err error) {
	parts := strings.SplitN(name, "@", 2)
	if len(parts) != 2 {
		err = errors.New("expected 2 parts separated by @")
		return
	}

	id, err = strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return
	}

	return id, parts[1], nil
}
