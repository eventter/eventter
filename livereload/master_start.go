//+build !production

package livereload

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func (m *master) start(executablePath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.startingWorker != nil {
		if err := m.startingWorker.Process.Kill(); err != nil {
			return err
		}
		m.startingWorker = nil
	}

	worker := exec.Command(executablePath, os.Args[1:]...)
	worker.Env = append(worker.Env, os.Environ()...)
	switch m.config.Network {
	case "tcp", "tcp4", "tcp6":
		if strings.HasPrefix(m.config.Address, ":") {
			worker.Env = append(
				worker.Env,
				MasterEnv+"="+m.config.Network+"://localhost"+m.config.Address+"/",
			)
		} else if strings.HasPrefix(m.config.Address, "0.0.0.0:") {
			worker.Env = append(
				worker.Env,
				MasterEnv+"="+m.config.Network+"://localhost"+m.config.Address[7:]+"/",
			)
		} else if strings.HasPrefix(m.config.Address, "[::]:") {
			worker.Env = append(
				worker.Env,
				MasterEnv+"="+m.config.Network+"://localhost"+m.config.Address[4:]+"/",
			)
		} else {
			worker.Env = append(
				worker.Env,
				MasterEnv+"="+m.config.Network+"://"+m.config.Address+"/",
			)
		}
	case "unix", "unixpacket":
		path := strings.Replace(m.config.Address, string(os.PathSeparator), "/", -1)
		if !strings.HasPrefix(path, "/") {
			path = "/./" + path
		}
		worker.Env = append(
			worker.Env,
			MasterEnv+"="+"unix://"+path,
		)
	default:
		panic("cannot happen, net.Listen() would've failed")
	}
	worker.Env = append(worker.Env, ConfigHashEnv+"="+strconv.FormatUint(m.configHash, 10))
	worker.ExtraFiles = m.listenerFiles

	stdout, err := worker.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := worker.StderrPipe()
	if err != nil {
		return err
	}

	tag := "worker:"

	go func() {
		r := bufio.NewReader(stdout)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					m.config.Logger.Error("reading worker stdout error:", err)
				}
				break
			}
			io.WriteString(os.Stdout, line)
			m.emit(&event{name: workerStdoutEvent, data: strings.TrimRight(line, "\r\n")})
		}
	}()

	go func() {
		r := bufio.NewReader(stderr)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					m.config.Logger.Error("reading worker stderr error:", err)
				}
				break
			}
			io.WriteString(os.Stdout, line)
			m.emit(&event{name: workerStderrEvent, data: strings.TrimRight(line, "\r\n")})
		}
	}()

	if err := worker.Start(); err != nil {
		return err
	}

	m.emit(&event{name: workerStartEvent})

	tag = fmt.Sprintf("worker/%d:", worker.Process.Pid)
	m.startingWorker = worker

	go func() {
		err := worker.Wait()
		if err == nil {

			m.config.Logger.Info(m.colors.Bold(tag), m.colors.Red("worker exited"))
		} else {
			m.config.Logger.Info(m.colors.Bold(tag), m.colors.Red(err))

			m.mu.RLock()
			if worker == m.startingWorker || worker == m.runningWorker {
				m.emit(&event{name: workerErrorEvent})
			}
			m.mu.RUnlock()
		}
	}()

	return nil
}
