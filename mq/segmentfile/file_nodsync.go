// +build dragonfly freebsd windows

package segmentfile

import (
	"os"
)

const openSync = os.O_SYNC
