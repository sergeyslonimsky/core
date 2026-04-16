// Package http2 provides an opinionated HTTP/2 server wrapper that
// implements lifecycle.Runner. It ships with built-in /livez, /readyz, and
// /metrics endpoints, a slog-aware recovery middleware, an opt-in otelhttp
// instrumentation hook, and a context-safe shutdown contract.
//
// Typical usage:
//
//	cfg := http2.Config{Port: "8080"}
//	srv := http2.NewServer(cfg,
//	    http2.WithOtel(),
//	    http2.WithRecovery(),
//	    http2.WithHealthcheckFrom(app),
//	)
//	srv.Mount("/api", apiHandler)
//
//	a := app.New()
//	a.Add(srv)
//	log.Fatal(a.Run())
package http2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/sergeyslonimsky/core/lifecycle"
)

const (
	defaultHTTPPort           = "80"
	defaultServerWriteTimeout = 10 * time.Second
	defaultServerReadTimeout  = 1 * time.Second
	defaultShutdownTimeout    = 10 * time.Second
)

// ErrServerAlreadyStarted is returned by Server.Run if the server has
// already been started. A Server instance is single-use: the underlying
// http.Server cannot be reused after Shutdown.
var ErrServerAlreadyStarted = errors.New("http2: server already started")

// Built-in route paths served by every Server.
const (
	// LivenessPath returns 200 OK unconditionally — a signal that the
	// process is up and the event loop is scheduling. Does NOT check
	// dependencies; that is /readyz's job.
	LivenessPath = "/livez"

	// ReadinessPath returns 200 OK if every Healthchecker wired via
	// WithHealthcheckFrom / WithReadyzCheck reports healthy, or 503 with
	// the error body otherwise.
	ReadinessPath = "/readyz"

	// MetricsPath serves the handler passed via WithMetricsHandler, or 404
	// if no handler was set. Typical use: pass the promhttp handler that
	// scrapes the otel PrometheusExporter.
	MetricsPath = "/metrics"
)

// Config holds the server's network and timeout settings. All fields are
// optional — zero values are replaced with defaults in NewServer.
type Config struct {
	// Port is the TCP port the listener binds to. Default: "80".
	Port string

	// ReadTimeout caps the time taken to read the full request including
	// body. Default: 1 second. Keep small for non-upload endpoints.
	ReadTimeout time.Duration

	// WriteTimeout caps the time taken to write the full response body.
	// For server-streaming endpoints this MUST be large — a 24h value
	// is common for long-lived SSE / server-streaming RPCs. Default: 10s.
	WriteTimeout time.Duration

	// ShutdownTimeout caps the graceful shutdown phase. If in-flight
	// requests do not complete within this window, the server is
	// force-closed. Default: 10 seconds.
	ShutdownTimeout time.Duration
}

// Server is an HTTP/2-capable http.Server wrapped with middleware chain,
// built-in /livez, /readyz, /metrics routes, and a context-safe graceful
// shutdown. Implements lifecycle.Runner.
type Server struct {
	cfg      Config
	logger   *slog.Logger
	mux      *http.ServeMux
	server   *http.Server
	listener net.Listener

	middlewares    []func(http.Handler) http.Handler
	otelEnabled    bool
	recoveryOn     bool
	healthchecker  lifecycle.Healthchecker
	extraReadyz    []readyzCheck
	metricsHandler http.Handler

	// started guards against concurrent / repeated Run calls. The
	// underlying http.Server cannot be reused after Shutdown, so a second
	// Run is a programming error rather than a valid lifecycle step.
	started atomic.Bool
}

type readyzCheck struct {
	name string
	fn   func(ctx context.Context) error
}

// Option configures a new Server.
type Option func(*Server)

// WithMiddleware appends one or more middleware functions to the chain. The
// chain is applied outer-first: the first middleware passed is the
// outermost wrapper (sees the request first, writes the response last).
//
// Built-in middlewares (WithOtel, WithRecovery) are applied AFTER user
// middlewares so that panics from inside user middleware are still caught
// and traces span user middleware work.
func WithMiddleware(mw ...func(http.Handler) http.Handler) Option {
	return func(s *Server) { s.middlewares = append(s.middlewares, mw...) }
}

// WithListener overrides the default TCP listener. Useful for tests
// (net.Listen on :0) or for exposing the server on a unix socket.
func WithListener(l net.Listener) Option {
	return func(s *Server) { s.listener = l }
}

// WithLogger attaches a *slog.Logger to the server. Used by the recovery
// middleware and for lifecycle events. Defaults to slog.Default() when
// omitted.
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) { s.logger = l }
}

// WithOtel enables OpenTelemetry instrumentation by wrapping the final
// handler with otelhttp.NewHandler. Uses the global TracerProvider and
// MeterProvider, so otel.Setup must have been called and registered with
// app.App before NewServer so the providers are non-noop.
//
// WithOtel records one span per request with the route pattern matched by
// the underlying ServeMux, including HTTP method, status code, and latency.
func WithOtel() Option {
	return func(s *Server) { s.otelEnabled = true }
}

// WithRecovery installs a panic-recovery middleware that logs the panic
// with a stack trace (via the configured slog.Logger) and responds with
// HTTP 500. Without this, a panic in a handler crashes the goroutine and
// terminates the connection without a response.
//
// Recommended for every production-facing server.
func WithRecovery() Option {
	return func(s *Server) { s.recoveryOn = true }
}

// WithHealthcheckFrom wires a lifecycle.Healthchecker (typically an
// *app.App) into the /readyz handler. On every /readyz request, the passed
// Healthchecker's Healthcheck method is invoked and its result aggregated
// with any WithReadyzCheck-registered checks.
//
// If no healthchecker is set, /readyz returns 200 OK unconditionally (same
// as /livez).
func WithHealthcheckFrom(h lifecycle.Healthchecker) Option {
	return func(s *Server) { s.healthchecker = h }
}

// WithReadyzCheck adds a named inline readiness check. Useful for app-level
// gates that are not tied to a registered Resource/Runner (e.g., "warm
// cache loaded", "migration finished").
//
// The check function receives the request's context and must respect its
// deadline. Returning a non-nil error fails the /readyz response.
func WithReadyzCheck(name string, fn func(ctx context.Context) error) Option {
	return func(s *Server) {
		s.extraReadyz = append(s.extraReadyz, readyzCheck{name: name, fn: fn})
	}
}

// WithMetricsHandler registers an http.Handler at MetricsPath. Typical use:
// pass the promhttp.Handler wrapping an otel Prometheus exporter. Kept as
// a generic Handler parameter so core/http2 does not depend on prometheus
// or otel/exporters/prometheus directly.
//
// Without this option, /metrics returns 404.
func WithMetricsHandler(h http.Handler) Option {
	return func(s *Server) { s.metricsHandler = h }
}

// NewServer constructs a Server with the given Config and options. Zero
// values in cfg are replaced with defaults (see defaultXxx constants).
//
// The returned *Server implements lifecycle.Runner. Register it with
// app.App via a.Add(srv); do not call Run directly outside of tests.
func NewServer(cfg Config, opts ...Option) *Server {
	if cfg.Port == "" {
		cfg.Port = defaultHTTPPort
	}

	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = defaultServerReadTimeout
	}

	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = defaultServerWriteTimeout
	}

	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}

	s := &Server{ //nolint:exhaustruct // listener, middlewares, otelEnabled etc. have valid zero values
		cfg:    cfg,
		logger: slog.Default(),
		mux:    http.NewServeMux(),
		server: &http.Server{ //nolint:exhaustruct // Handler/BaseContext set in Run
			Addr:         ":" + cfg.Port,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		},
	}

	for _, opt := range opts {
		opt(s)
	}

	s.mountBuiltins()

	return s
}

// Mount registers handler at pattern on the underlying ServeMux. Patterns
// follow net/http.ServeMux rules (see https://pkg.go.dev/net/http#ServeMux).
//
// Mount must be called before Run. Calling it concurrently with Run is
// undefined behavior.
func (s *Server) Mount(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

// Run starts the HTTP listener and blocks until ctx is cancelled or the
// server fails. On ctx cancellation, Run invokes Shutdown with a FRESH
// timeout context (NOT the already-cancelled ctx) so graceful drain has
// its full budget.
//
// Implements lifecycle.Runner.
//
// Run must be called at most once per Server instance — the underlying
// http.Server cannot be reused after Shutdown. A second call returns
// ErrServerAlreadyStarted.
func (s *Server) Run(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrServerAlreadyStarted
	}

	s.server.Handler = s.buildHandler()
	s.server.BaseContext = func(net.Listener) context.Context { return ctx }

	srvErr := make(chan error, 1)

	go func() {
		var err error
		if s.listener != nil {
			err = s.server.Serve(s.listener)
		} else {
			err = s.server.ListenAndServe()
		}

		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err

			return
		}

		srvErr <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()

		//nolint:contextcheck // fresh ctx: run ctx is already cancelled
		return s.Shutdown(shutdownCtx)

	case err := <-srvErr:
		return err
	}
}

// Shutdown initiates graceful shutdown bounded by ctx. Implements
// lifecycle.Resource.
//
// Can be called multiple times safely — after the first call, subsequent
// calls to the underlying http.Server.Shutdown return http.ErrServerClosed,
// which Shutdown treats as a no-op.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.server.Shutdown(ctx)
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return fmt.Errorf("shutdown http server: %w", err)
}

// Address returns the server's listen address in ":port" form.
func (s *Server) Address() string {
	return s.server.Addr
}

// mountBuiltins installs /livez, /readyz, and optionally /metrics.
func (s *Server) mountBuiltins() {
	s.mux.HandleFunc(LivenessPath, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	s.mux.HandleFunc(ReadinessPath, func(w http.ResponseWriter, r *http.Request) {
		if err := s.runReadyz(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)

			return
		}

		w.WriteHeader(http.StatusOK)
	})

	if s.metricsHandler != nil {
		s.mux.Handle(MetricsPath, s.metricsHandler)
	}
}

func (s *Server) runReadyz(ctx context.Context) error {
	var errs []error

	if s.healthchecker != nil {
		if err := s.healthchecker.Healthcheck(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	for _, c := range s.extraReadyz {
		if err := c.fn(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", c.name, err))
		}
	}

	return errors.Join(errs...)
}

// buildHandler composes the middleware chain. Order of wrapping (outermost
// first): user middlewares → recovery → otelhttp → mux. Recovery goes
// inside otelhttp so the trace still captures a 500-response for a
// recovered panic.
func (s *Server) buildHandler() http.Handler {
	var handler http.Handler = s.mux

	if s.recoveryOn {
		handler = recoveryMiddleware(s.logger)(handler)
	}

	if s.otelEnabled {
		handler = otelhttp.NewHandler(handler, "http-"+s.cfg.Port)
	}

	// Apply user middlewares OUTSIDE the built-in wrappers so they are
	// the outermost layer (first to see the request).
	for i := len(s.middlewares) - 1; i >= 0; i-- {
		handler = s.middlewares[i](handler)
	}

	return handler
}

// recoveryMiddleware catches panics from downstream handlers, logs them
// with a stack trace, and — if the response has not yet been committed —
// responds with HTTP 500. If the handler already called WriteHeader or
// wrote any body before panicking, the middleware only logs: overwriting
// headers after WriteHeader is a no-op, and writing a second response body
// can interleave with partial data and corrupt the HTTP framing.
func recoveryMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		//nolint:contextcheck // defer/recover cannot propagate context
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			//nolint:exhaustruct // wrote bool has valid zero value
			tracked := &trackingResponseWriter{ResponseWriter: w}

			defer func() {
				if rec := recover(); rec != nil {
					logger.ErrorContext(r.Context(), "http handler panic",
						slog.Any("panic", rec),
						slog.String("method", r.Method),
						slog.String("path", r.URL.Path),
						slog.Bool("response_committed", tracked.wrote),
						slog.String("stack", string(debug.Stack())),
					)

					if !tracked.wrote {
						http.Error(tracked, "internal server error", http.StatusInternalServerError)
					}
				}
			}()

			next.ServeHTTP(tracked, r)
		})
	}
}

// trackingResponseWriter wraps an http.ResponseWriter and records whether
// the response has been committed (WriteHeader or Write called). Used by
// recoveryMiddleware to decide whether it is safe to emit a 500 on panic.
type trackingResponseWriter struct {
	http.ResponseWriter

	wrote bool
}

func (t *trackingResponseWriter) WriteHeader(statusCode int) {
	t.wrote = true
	t.ResponseWriter.WriteHeader(statusCode)
}

func (t *trackingResponseWriter) Write(b []byte) (int, error) {
	t.wrote = true

	n, err := t.ResponseWriter.Write(b)
	if err != nil {
		return n, fmt.Errorf("write response: %w", err)
	}

	return n, nil
}

// Compile-time assertions.
var (
	_ lifecycle.Runner   = (*Server)(nil)
	_ lifecycle.Resource = (*Server)(nil)
)
