// +build !dragonfly,!freebsd,!windows

package segmentfile

import (
	"golang.org/x/sys/unix"
)

const openSync = unix.O_DSYNC
