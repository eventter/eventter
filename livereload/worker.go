//+build !production

package livereload

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
)

type worker struct {
	config    *Config
	client    *http.Client
	masterURL *url.URL
}

func newWorker(config *Config, master string) (Worker, error) {
	masterURL, err := url.Parse(master)
	if err != nil {
		return nil, err
	}

	return &worker{
		config:    config,
		client:    &http.Client{},
		masterURL: masterURL,
	}, nil
}

const firstListenFd = 3

func (w *worker) Listen(i int) (net.Listener, error) {
	fd := uintptr(firstListenFd + i)
	f := os.NewFile(fd, fmt.Sprintf("%s:%s", w.config.Listeners[i].Network, w.config.Listeners[i].Address))
	if f == nil {
		return nil, fmt.Errorf("could not create file for descriptor [%d]", fd)
	}
	return net.FileListener(f)
}

func (w *worker) MarkReady() error {
	readyURL := *w.masterURL
	readyURL.Path = fmt.Sprintf("/workers/%d/ready", os.Getpid())

	response, err := w.client.Do(&http.Request{
		Method: "PUT",
		URL:    w.masterURL,
	})

	if response.StatusCode < 200 || response.StatusCode > 299 {
		return fmt.Errorf("master responded with [%d]", response.StatusCode)
	}

	return err
}
