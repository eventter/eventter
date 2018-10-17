//+build !production

package livereload

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

func (m *master) build(executablePath string) error {
	var args []string

	args = append(args, "build")
	if m.config.BuildTags != nil {
		args = append(args, "-tags="+strings.Join(m.config.BuildTags, ","))
	}
	if m.config.BuildArgs != nil {
		args = append(args, m.config.BuildArgs...)
	} else {
		args = append(args, "-v")
	}
	newExecutablePath := filepath.Join(
		filepath.Dir(executablePath),
		"."+filepath.Base(executablePath)+"."+strconv.FormatInt(time.Now().UnixNano(), 10),
	)

	args = append(args, "-o="+newExecutablePath)
	args = append(args, m.config.Package)

	build := exec.Command("go", args...)

	buildStdout, err := build.StdoutPipe()
	if err != nil {
		return err
	}
	buildStderr, err := build.StderrPipe()
	if err != nil {
		return err
	}

	go func() {
		r := bufio.NewReader(buildStdout)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					m.config.Logger.Error("reading build stdout error:", err)
				}
				break
			}
			m.config.Logger.Info(m.colors.Bold("build:"), " ", strings.TrimRight(line, "\n"))
		}
	}()

	go func() {
		r := bufio.NewReader(buildStderr)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					m.config.Logger.Error("reading build stderr error:", err)
				}
				break
			}
			m.config.Logger.Info(m.colors.Bold("build:"), " ", strings.TrimRight(line, "\n"))
		}
	}()

	if err := build.Start(); err != nil {
		return err
	}

	err = build.Wait()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			m.config.Logger.Error(m.colors.Bold("build:"), " ", "returned non-zero exit code")
			// TODO: kill current worker & serve friendly error message instead
			return nil
		} else {
			return err
		}
	}

	m.config.Logger.Info(m.colors.Bold("build:"), m.colors.Green("succeeded"))
	if err := os.Rename(newExecutablePath, executablePath); err != nil {
		return errors.Wrap(err, "could not rename newly built executable")
	}

	return nil
}
