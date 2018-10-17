package stdlogger

import (
	"fmt"
	"log"

	"eventter.io/livereload/logger"
	"github.com/logrusorgru/aurora"
)

const (
	infoMarker  = "[info] "
	errorMarker = "[error] "
)

type stdLogger struct {
	*log.Logger
	aurora.Aurora
}

func New(l *log.Logger, noColors bool) logger.Logger {
	return &stdLogger{l, aurora.NewAurora(!noColors)}
}

func (l *stdLogger) Info(v ...interface{}) {
	l.Logger.Print(l.Aurora.Cyan(infoMarker), fmt.Sprint(v...))
}

func (l *stdLogger) Infof(format string, v ...interface{}) {
	l.Logger.Print(l.Aurora.Cyan(infoMarker), fmt.Sprintf(format, v...))
}

func (l stdLogger) Error(v ...interface{}) {
	l.Logger.Print(l.Aurora.Red(errorMarker), fmt.Sprint(v...))
}

func (l stdLogger) Errorf(format string, v ...interface{}) {
	l.Logger.Print(l.Aurora.Red(errorMarker), fmt.Sprintf(format, v...))
}
