//+build production

package livereload

import (
	"errors"
)

const notSupported = errors.New("built without livereload support")

func newWorker(config *Config, master string) (Worker, error) {
	return nil, notSupported
}

func newMaster(config *Config) (Master, error) {
	return nil, notSupported
}
