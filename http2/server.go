package http2

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

const (
	defaultHTTPPort           = "80"
	defaultServerWriteTimeout = 10 * time.Second
	defaultServerReadTimeout  = 1 * time.Second
)

type Config struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type Server struct {
	mux         *http.ServeMux
	middlewares []func(http.Handler) http.Handler
	server      *http.Server
	listener    net.Listener
	port        string
}

func NewServer(config Config) *Server {
	mux := http.NewServeMux()

	if config.ReadTimeout == 0 {
		config.ReadTimeout = defaultServerReadTimeout
	}

	if config.WriteTimeout == 0 {
		config.WriteTimeout = defaultServerWriteTimeout
	}

	if config.Port == "" {
		config.Port = defaultHTTPPort
	}

	mux.HandleFunc("/health", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})

	server := &http.Server{ //nolint:exhaustruct
		Addr:         ":" + config.Port,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	}

	return &Server{ //nolint:exhaustruct
		mux:    mux,
		server: server,
		port:   config.Port,
	}
}

func (s *Server) Middleware(middleware func(http.Handler) http.Handler) {
	s.middlewares = append(s.middlewares, middleware)
}

func (s *Server) Mount(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

func (s *Server) WithListener(l net.Listener) {
	s.listener = l
}

func (s *Server) Run(ctx context.Context) error {
	var handler http.Handler

	handler = s.mux

	for _, middleware := range s.middlewares {
		handler = middleware(handler)
	}

	s.server.Handler = handler
	// Set the server's handler and base context
	s.server.BaseContext = func(net.Listener) context.Context {
		return ctx
	}

	// Channel to capture server errors
	srvErr := make(chan error, 1)

	// Start the server in a goroutine
	go func() {
		var err error
		if s.listener != nil {
			err = s.server.Serve(s.listener)
		} else {
			err = s.server.ListenAndServe()
		}

		// If the server exits, send the error (or nil) to srvErr
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		} else {
			srvErr <- nil
		}
	}()

	// Listen for context cancellation or server errors
	select {
	case <-ctx.Done():
		// If context is done, gracefully shut down the server
		return s.Shutdown(ctx)

	case err := <-srvErr:
		// If server exits with an error (or nil), return the error
		return err
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown server: %w", err)
	}

	return nil
}

func (s *Server) Address() string {
	return s.server.Addr
}
