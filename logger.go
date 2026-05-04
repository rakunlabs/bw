package bw

import (
	"fmt"
	"log/slog"
)

type logger struct {
	logger *slog.Logger
}

func newLogger() *logger {
	return &logger{
		logger: slog.Default().With(slog.String("component", "badger")),
	}
}

func (l *logger) Errorf(format string, v ...any) {
	l.logger.Error(fmt.Sprintf(format, v...))
}

func (l *logger) Warningf(format string, v ...any) {
	l.logger.Warn(fmt.Sprintf(format, v...))
}

func (l *logger) Infof(format string, v ...any) {
	l.logger.Info(fmt.Sprintf(format, v...))
}

func (l *logger) Debugf(format string, v ...any) {
	l.logger.Debug(fmt.Sprintf(format, v...))
}
