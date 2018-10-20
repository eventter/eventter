//+build !production

package livereload

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"syscall"
)

var (
	readyRoute  = regexp.MustCompile(`^/livereload/workers/(?P<pid>\d+)/ready`)
	eventsRoute = regexp.MustCompile(`^/livereload/events$`)
)

func (m *master) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if matches := readyRoute.FindStringSubmatch(r.URL.Path); r.Method == "PUT" && matches != nil {
		pid, _ := strconv.Atoi(matches[1])
		m.handleWorkerReady(pid, w, r)

	} else if matches := eventsRoute.FindStringSubmatch(r.URL.Path); r.Method == "GET" && matches != nil {
		m.handleEvents(w, r)

	} else {
		http.NotFound(w, r)

		m.config.Logger.Info(m.colors.Bold("http:"), " ", fmt.Sprintf(
			`"%s %s %s" %d "%s"`,
			r.Method,
			r.URL.Path,
			r.Proto,
			404,
			r.Header.Get("User-Agent"),
		))
	}
}

func (m *master) handleWorkerReady(pid int, w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.startingWorker == nil || m.startingWorker.Process.Pid != pid {
		m.config.Logger.Errorf("received ready from process [%d], however, it's not currently starting worker", pid)
		http.Error(w, "400 bad worker pid", 400)
		return
	}

	m.config.Logger.Info(m.colors.Bold("worker/"+strconv.Itoa(pid)+":"), m.colors.Green("started"))

	if m.runningWorker != nil {
		if err := m.runningWorker.Process.Signal(syscall.SIGTERM); err != nil {
			m.config.Logger.Errorf("could not kill currently running worker: %s", err)
		}
		m.runningWorker = nil
	}

	m.runningWorker = m.startingWorker
	m.startingWorker = nil

	m.emit(&event{name: workerReadyEvent})

	w.WriteHeader(200)
	fmt.Fprint(w, "200 ok")
}

func (m *master) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "500 streaming not supported", 500)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(200)
	flusher.Flush()

	m.mu.RLock()
	ready := m.runningWorker != nil
	m.mu.RUnlock()

	if ready {
		writeEvent(w, &event{name: workerReadyEvent})
		flusher.Flush()
	}

	c := m.register()
	defer m.unregister(c)

	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-c:
			writeEvent(w, ev)
			flusher.Flush()
		}
	}
}

func writeEvent(w io.Writer, ev *event) (n int, err error) {
	return fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.name, ev.data)
}
