//+build production

package livereload

import (
	"errors"

	"eventter.io/livereload/logger"
)

var notSupported = errors.New("built without livereload support")

func newWorker(config *Config, master string) (Worker, error) {
	return nil, notSupported
}

func newMaster(config *Config) (Master, error) {
	return nil, notSupported
}

func newDefaultLogger(noColors bool) logger.Logger {
	return &nullLogger{}
}

type nullLogger struct{}

func (*nullLogger) Info(v ...interface{}) {
	// do nothing
}

func (*nullLogger) Infof(format string, v ...interface{}) {
	// do nothing
}

func (*nullLogger) Error(v ...interface{}) {
	// do nothing
}

func (*nullLogger) Errorf(format string, v ...interface{}) {
	// do nothing
}
