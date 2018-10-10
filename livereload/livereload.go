package livereload

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	MasterEnv                  = "LIVERELOAD_MASTER"
	ConfigHashEnv              = "LIVERELOAD_MASTER_CONFIG_HASH"
	DefaultPort                = 20123
	ConfigHashMismatchExitCode = 3
)

type Config struct {
	Live         bool                 // If false, won't actually do master-worker setup and returns always only worker.
	Network      string               // Master socket network.
	Address      string               // Master socket address.
	Package      string               // Watch for changes in this package. If empty, will watch only for changes to the binary itself.
	GoGenerate   bool                 // If true, will run `go generate` before build for changed files.
	ReadyTimeout time.Duration        // Max time to wait for worker to report ready.
	Listeners    []ListenerDefinition // Listeners are created by master and passed to workers by FDs >=3.
}

func (c *Config) Hash() (uint64, error) {
	h := fnv.New64()
	if err := json.NewEncoder(h).Encode(c); err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}

type ListenerDefinition struct {
	Network string // `network` argument for `net.Listen()`
	Address string // `address` argument for `net.Listen()`
	HTTP    bool   // If true and worker crashed, or compilation failed, master will take over the socket and serve nice error page
}

type Master interface {
	// Will either return error right away, or takes over the process to do master stuff.
	Run() error
}

type Worker interface {
	// Create listener for `i`-th listener definition from the config. Multiple calls will create multiple listeners.
	Listen(i int) (net.Listener, error)
	// Report to master that this worker is ready.
	MarkReady() error
}

// Create livereload instance. Either `Master`, or `Worker` won't be nil, but not both.
func New(config *Config) (Master, Worker, error) {
	master := os.Getenv(MasterEnv)
	configHash := os.Getenv(ConfigHashEnv)

	if master != "" {
		masterConfigHash, err := strconv.ParseUint(configHash, 10, 64)
		if err != nil {
			return nil, nil, err
		}

		thisConfigHash, err := config.Hash()
		if err != nil {
			return nil, nil, err
		}

		if thisConfigHash != masterConfigHash {
			os.Exit(ConfigHashMismatchExitCode)
		}
	}

	if !config.Live {
		w, err := newMasterWorker(config)
		return nil, w, err
	}

	if config.Address == "" {
		config.Network = "tcp"
		config.Address = fmt.Sprintf(":%d", DefaultPort)
	}
	if config.Network == "" {
		if strings.Contains(config.Address, "/") || !strings.Contains(config.Address, ":") {
			config.Network = "unix"
		} else {
			config.Network = "tcp"
		}
	}

	if master == "" {
		m, err := newMaster(config)
		return m, nil, err
	} else {
		w, err := newWorker(config, master)
		return nil, w, err
	}
}

type masterWorker struct {
	config *Config
}

func newMasterWorker(config *Config) (Worker, error) {
	return &masterWorker{config}, nil
}

func (w *masterWorker) Listen(i int) (net.Listener, error) {
	l := w.config.Listeners[i]
	return net.Listen(l.Network, l.Address)
}

func (w *masterWorker) MarkReady() error {
	return nil
}
