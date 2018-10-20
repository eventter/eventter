//+build !production

package livereload

const (
	buildStartEvent   = "build:start"
	buildStdoutEvent  = "build:stdout"
	buildStderrEvent  = "build:stderr"
	buildOkEvent      = "build:ok"
	buildErrorEvent   = "build:error"
	workerStartEvent  = "worker:start"
	workerStdoutEvent = "worker:stdout"
	workerStderrEvent = "worker:stderr"
	workerReadyEvent  = "worker:ready"
	workerErrorEvent  = "worker:error"
)

type event struct {
	name string
	data string
}

func (m *master) register() chan *event {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := make(chan *event, 16)
	m.eventClients[c] = struct{}{}
	return c
}

func (m *master) unregister(c chan *event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.eventClients, c)
}

func (m *master) emit(ev *event) {
	m.eventC <- ev
}
