// +build !dragonfly,!freebsd,!windows

package segments

import (
	"golang.org/x/sys/unix"
)

const openSync = unix.O_DSYNC
