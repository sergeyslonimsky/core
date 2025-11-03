package di

import (
	"errors"
	"log/slog"
)

type ServiceContainer struct {
	closers []func() error
}

func NewServiceContainer() *ServiceContainer {
	return &ServiceContainer{
		closers: make([]func() error, 0),
	}
}

func (s *ServiceContainer) AddOnClose(callback func() error) {
	if s.closers == nil {
		slog.Warn("di.NewServiceContainer is not called")

		s.closers = make([]func() error, 0)
	}

	s.closers = append(s.closers, callback)
}

func (s *ServiceContainer) onClose() func() error {
	return func() error {
		var errs error

		for _, f := range s.closers {
			if err := f(); err != nil {
				errs = errors.Join(errs, err)
			}
		}

		return errs
	}
}
