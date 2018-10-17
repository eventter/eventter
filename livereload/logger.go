//+build !production

package livereload

import (
	"log"
	"os"

	"eventter.io/livereload/logger"
	"eventter.io/livereload/stdlogger"
)

func newDefaultLogger(noColors bool) logger.Logger {
	return stdlogger.New(log.New(os.Stderr, "", 0), noColors)
}
