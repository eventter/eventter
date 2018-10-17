//+build !production

package livereload

import (
	"fmt"
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

		m.config.Logger.Info("new server-sent events connection")

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

	w.WriteHeader(200)
	fmt.Fprint(w, "200 ok")
}
