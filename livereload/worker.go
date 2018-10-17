//+build !production

package livereload

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"

	"github.com/pkg/errors"
)

type worker struct {
	config        *Config
	client        *http.Client
	masterHttpURL *url.URL
}

func newWorker(config *Config, master string) (Worker, error) {
	masterURL, err := url.Parse(master)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse master URL [%s]", master)
	}

	dialer := &net.Dialer{}

	httpURL, _ := url.Parse("http://master/")
	if masterURL.Host != "" {
		httpURL.Host = masterURL.Host
	}

	return &worker{
		config: config,
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					switch masterURL.Scheme {
					case "tcp", "tcp4", "tcp6":
						return dialer.DialContext(ctx, masterURL.Scheme, masterURL.Host)
					case "unix", "unixpacket":
						return dialer.DialContext(ctx, masterURL.Scheme, masterURL.Path)
					default:
						return nil, fmt.Errorf("could not dial network [%s]", masterURL.Scheme)
					}
				},
			},
		},
		masterHttpURL: httpURL,
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
	readyURL := *w.masterHttpURL
	readyURL.Path = fmt.Sprintf("/livereload/workers/%d/ready", os.Getpid())

	response, err := w.client.Do(&http.Request{
		Method: "PUT",
		URL:    &readyURL,
	})

	if response.StatusCode < 200 || response.StatusCode > 299 {
		return fmt.Errorf("master responded with [%d]", response.StatusCode)
	}

	return errors.Wrap(err, "ready request to master failed")
}
