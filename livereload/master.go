//+build !production

package livereload

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/logrusorgru/aurora"
	"github.com/pkg/errors"
	"github.com/yookoala/realpath"
)

type master struct {
	mu                 sync.RWMutex
	config             *Config
	configHash         uint64
	watcher            *fsnotify.Watcher
	watchedPackageDirs map[string]string
	listenerFiles      []*os.File
	runningWorker      *exec.Cmd
	startingWorker     *exec.Cmd
	colors             aurora.Aurora
	eventC             chan *event
	eventClients       map[chan *event]struct{}
}

func newMaster(config *Config) (m Master, err error) {
	configHash, err := config.Hash()
	if err != nil {
		return nil, errors.Wrap(err, "compute config hash")
	}
	return &master{
		config:             config,
		configHash:         configHash,
		watchedPackageDirs: make(map[string]string),
		colors:             aurora.NewAurora(!config.NoColors),
		eventC:             make(chan *event, 16),
		eventClients:       make(map[chan *event]struct{}),
	}, nil
}

type filer interface {
	File() (f *os.File, err error)
}

func (m *master) Run() (err error) {
	defer func() {
		for _, listenerFile := range m.listenerFiles {
			listenerFile.Close()
		}
	}()
	for _, listenerDefinition := range m.config.Listeners {
		listener, err := net.Listen(listenerDefinition.Network, listenerDefinition.Address)
		if err != nil {
			return errors.Wrapf(
				err,
				"could not create listener for [%s:%s]",
				listenerDefinition.Network,
				listenerDefinition.Address,
			)
		}

		filer, ok := listener.(filer)
		if !ok {
			listener.Close()
			return errors.New("listener cannot be converted to file")
		}

		listenerFile, err := filer.File()
		if err != nil {
			listener.Close()
			return errors.Wrap(err, "could not convert listener to file")
		}

		m.listenerFiles = append(m.listenerFiles, listenerFile)
	}

	masterListener, err := net.Listen(m.config.Network, m.config.Address)
	if err != nil {
		return errors.Wrapf(err, "could not start master HTTP listener")
	}
	defer masterListener.Close()

	errC := make(chan error)

	srv := &http.Server{Handler: m}
	defer func() {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		if err := srv.Shutdown(ctx); err != nil {
			m.config.Logger.Errorf("could not shutdown HTTP server: %s", err)
		}
	}()

	go func() {
		err := srv.Serve(masterListener)
		select {
		case errC <- err:
		default:
			// nobody is listening
		}
	}()

	m.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "could not create watcher")
	}
	defer m.watcher.Close()

	executablePath, err := os.Executable()
	if err != nil {
		return errors.Wrap(err, "could not get path to current executable")
	}
	executablePath, err = realpath.Realpath(executablePath)
	if err != nil {
		return errors.Wrap(err, "could not get real path of current executable")
	}

	if err = m.watcher.Add(executablePath); err != nil {
		return errors.Wrap(err, "watch of current executable failed")
	}

	if m.config.Package != "" {
		if err := m.watchPackage(m.config.Package); err != nil {
			return err
		}
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	m.config.Logger.Info(m.colors.Bold("master:"), m.colors.Green("started"))

	if err := m.start(executablePath); err != nil {
		return err
	}

	var (
		buildC        <-chan time.Time = nil
		startC        <-chan time.Time = nil
		dirtyPackages                  = make(map[string]bool)
	)

LOOP:
	for {
		select {
		case err, ok := <-m.watcher.Errors:
			if !ok {
				break LOOP
			}
			return errors.Wrap(err, "watcher error")

		case ev, ok := <-m.watcher.Events:
			if !ok {
				break LOOP
			}
			if ev.Op == fsnotify.Chmod {
				continue LOOP
			}

			if ev.Name == executablePath {
				if startC == nil {
					startC = time.After(200 * time.Millisecond)
				}
			} else {
				if filepath.Ext(ev.Name) != ".go" || strings.HasPrefix(filepath.Base(ev.Name), ".") {
					continue LOOP
				}

				fileDir := filepath.Dir(ev.Name)
				for pkgDir, pkg := range m.watchedPackageDirs {
					if fileDir == pkgDir {
						if !dirtyPackages[pkg] {
							dirtyPackages[pkg] = true
							m.config.Logger.Info(m.colors.Bold("watch:"), " ", pkg)
						}
						if buildC == nil {
							buildC = time.After(200 * time.Millisecond)
						}
						break
					}
				}
			}

		case <-buildC:
			buildC = nil

			for pkg, _ := range dirtyPackages {
				if err := m.watchPackage(pkg); err != nil {
					return err
				}
			}
			dirtyPackages = make(map[string]bool)

			if err := m.build(executablePath); err != nil {
				return err
			}

		case <-startC:
			startC = nil

			if err := m.start(executablePath); err != nil {
				return err
			}

		case ev := <-m.eventC:
			func() {
				m.mu.RLock()
				defer m.mu.RUnlock()
				for c, _ := range m.eventClients {
					select {
					case c <- ev:
					default:
						m.config.Logger.Errorf("could not emit event [%s] to a client", ev.name)
					}
				}
			}()

		case err := <-errC:
			return errors.Wrap(err, "server error")

		case sig := <-interrupt:
			func() {
				m.config.Logger.Infof("received signal [%s], shutting down", sig)
				ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
				if err := srv.Shutdown(ctx); err != nil {
					m.config.Logger.Errorf("could not shutdown HTTP server: %s", err)
				}

				m.mu.Lock()
				defer m.mu.Unlock()
				if m.startingWorker != nil {
					if err := m.startingWorker.Process.Kill(); err != nil {
						m.config.Logger.Errorf("could not kill starting worker: %s", err)
					}
					m.startingWorker = nil
				}
				if m.runningWorker != nil {
					if err := m.runningWorker.Process.Kill(); err != nil {
						m.config.Logger.Errorf("could not kill running worker: %s", err)
					}
					m.runningWorker = nil
				}

				os.Exit(0)
			}()

			break LOOP
		}
	}

	return nil
}
