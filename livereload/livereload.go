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

	"eventter.io/livereload/logger"
	"github.com/pkg/errors"
)

const (
	MasterEnv                  = "LIVERELOAD_MASTER"
	ConfigHashEnv              = "LIVERELOAD_MASTER_CONFIG_HASH"
	BuildTagsEnv               = "LIVERELOAD_BUILD_TAGS"
	BuildArgsEnv               = "LIVERELOAD_BUILD_ARGS"
	DefaultPort                = 20123
	DefaultReadyTimeout        = 10 * time.Second
	ConfigHashMismatchExitCode = 3
)

type Config struct {
	// If false, won't actually do master-worker setup and returns always only worker.
	Live bool
	// Master socket network.
	Network string
	// Master socket address.
	Address string
	// Watch for changes in this package. If empty, will watch only for changes to the binary itself.
	Package string
	// Build tags to be used when rebuilding package.
	BuildTags []string
	// Extra build command line arguments.
	BuildArgs []string
	// Max time to wait for worker to report readyRoute.
	ReadyTimeout time.Duration
	// Listeners are created by master and passed to workers by FDs >=3.
	Listeners []ListenerDefinition
	// Log messages will be passed to the instance. Defaults to stdlib log.
	Logger logger.Logger `json:"-"`
	// Whether log messages should NOT use pretty colors.
	NoColors bool
}

func (c *Config) hash() (uint64, error) {
	h := fnv.New64()
	if err := json.NewEncoder(h).Encode(c); err != nil {
		return 0, errors.Wrap(err, "config hash")
	}
	return h.Sum64(), nil
}

type ListenerDefinition struct {
	Network string // `network` argument for `net.Listen()`
	Address string // `address` argument for `net.Listen()`
}

// Master represents that this is a master process.
type Master interface {
	// Will either return error right away, or takes over the process to do master stuff.
	Run() error
}

// Worker represents that this is a worker process. When worker receives SIGTERM, it should terminate gracefully.
type Worker interface {
	// Create listener for `i`-th listener definition from the config. Multiple calls will create multiple listeners.
	Listen(i int) (net.Listener, error)
	// Report to master that this worker is ready.
	Ready() error
}

// Create livereload instance. Either `Master`, or `Worker` won't be nil, but not both.
func New(config *Config) (Master, Worker, error) {
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

	if config.BuildTags == nil {
		if buildTags := os.Getenv(BuildTagsEnv); buildTags != "" {
			config.BuildTags = strings.Split(buildTags, ",")
		}
	}

	if config.BuildArgs == nil {
		if buildArgs := os.Getenv(BuildArgsEnv); buildArgs != "" {
			config.BuildArgs = strings.Split(buildArgs, " ")
		}
	}

	if config.Logger == nil {
		config.Logger = newDefaultLogger(config.NoColors)
	}

	if config.ReadyTimeout == 0 {
		config.ReadyTimeout = DefaultReadyTimeout
	}

	masterEnv := os.Getenv(MasterEnv)
	configHashEnv := os.Getenv(ConfigHashEnv)

	if masterEnv != "" {
		masterConfigHash, err := strconv.ParseUint(configHashEnv, 10, 64)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "could not parse env variable [%s] with value [%s]", ConfigHashEnv, configHashEnv)
		}

		thisConfigHash, err := config.hash()
		if err != nil {
			return nil, nil, errors.Wrap(err, "could not create hash for config")
		}

		if thisConfigHash != masterConfigHash {
			os.Exit(ConfigHashMismatchExitCode)
		}
	}

	if !config.Live {
		w, err := newMasterWorker(config)
		return nil, w, err
	}

	if masterEnv == "" {
		m, err := newMaster(config)
		return m, nil, err
	} else {
		w, err := newWorker(config, masterEnv)
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

func (w *masterWorker) Ready() error {
	return nil
}
