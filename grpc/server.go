// Package grpc provides an opinionated gRPC server wrapper that implements
// lifecycle.Runner. It standardises functional options, panic recovery,
// OpenTelemetry stats handler wiring, and a context-bounded graceful
// shutdown.
//
// The package mirrors the API shape of core/http2 — same option-style,
// same Run/Shutdown contract, same registration pattern with app.App —
// so a service that uses both feels uniform.
//
// Typical usage:
//
//	srv := grpc.NewServer(grpc.Config{Port: "50051"},
//	    grpc.WithOtel(),
//	    grpc.WithRecovery(),
//	)
//	srv.Mount(func(s *grpc.Server) {
//	    pb.RegisterMyServiceServer(s, myImpl)
//	})
//
//	a := app.New()
//	a.Add(srv)
//	log.Fatal(a.Run())
package grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpchealth "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/sergeyslonimsky/core/lifecycle"
)

// ErrServerAlreadyStarted is returned by Server.Run if the server has
// already been started. A Server instance is single-use: the underlying
// grpc.Server cannot be reused after GracefulStop.
var ErrServerAlreadyStarted = errors.New("grpc: server already started")

const (
	defaultGRPCPort        = "50051"
	defaultShutdownTimeout = 10 * time.Second
)

// Config holds the server's network and timeout settings. Both fields are
// optional — zero values are replaced with defaults in NewServer.
type Config struct {
	// Port is the TCP port the listener binds to. Default: "50051".
	Port string

	// ShutdownTimeout caps the graceful shutdown phase. After it elapses,
	// the server is force-stopped via grpc.Server.Stop. Default: 10s.
	ShutdownTimeout time.Duration
}

// Server wraps a *grpc.Server with lifecycle methods. Implements
// lifecycle.Runner so it can be registered with app.App.
type Server struct {
	server   *grpc.Server
	listener net.Listener
	port     string
	shutdown time.Duration

	// started guards against concurrent / repeated Run calls. The
	// underlying grpc.Server cannot be reused after GracefulStop.
	started atomic.Bool
}

// Option configures a new Server. Mirrors the functional-options pattern used
// by http2.Server for symmetric setup.
type Option func(*options)

type options struct {
	logger             *slog.Logger
	serverOptions      []grpc.ServerOption
	unaryInterceptors  []grpc.UnaryServerInterceptor
	streamInterceptors []grpc.StreamServerInterceptor
	statsHandlers      []stats.Handler
	listener           net.Listener
	healthChecker      lifecycle.Healthchecker
}

// WithListener overrides the default TCP listener. Useful for tests
// (net.Listen on :0, bufconn) or for exposing the server on a unix socket.
//
// Promoted from a method to an option in v2 for consistency with http2.
func WithListener(l net.Listener) Option {
	return func(o *options) { o.listener = l }
}

// WithLogger attaches a *slog.Logger to the server. Used by the recovery
// interceptor and lifecycle events. Defaults to slog.Default() when omitted.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithServerOptions passes arbitrary grpc.ServerOption values through. Escape
// hatch for keepalive, TLS credentials, message size limits, etc.
//
// Composes with the typed With* options — both are applied to the same
// underlying *grpc.Server.
func WithServerOptions(opts ...grpc.ServerOption) Option {
	return func(o *options) { o.serverOptions = append(o.serverOptions, opts...) }
}

// WithUnaryInterceptor appends unary interceptors. Multiple calls accumulate
// and are chained in registration order via grpc.ChainUnaryInterceptor. This
// is the gRPC analogue of http2.Server.WithMiddleware.
//
// Built-in interceptors (WithRecovery) are appended AFTER user interceptors
// so they sit innermost — closest to the handler — and catch panics from
// user-interceptor work.
func WithUnaryInterceptor(interceptors ...grpc.UnaryServerInterceptor) Option {
	return func(o *options) { o.unaryInterceptors = append(o.unaryInterceptors, interceptors...) }
}

// WithStreamInterceptor appends stream interceptors. Chained in registration
// order via grpc.ChainStreamInterceptor. See WithUnaryInterceptor for ordering
// rules.
func WithStreamInterceptor(interceptors ...grpc.StreamServerInterceptor) Option {
	return func(o *options) { o.streamInterceptors = append(o.streamInterceptors, interceptors...) }
}

// WithStatsHandler attaches a stats.Handler. Multiple calls accumulate and
// every handler receives every event, fanned out via an internal composite
// (see stats.go). This is preferable to passing multiple grpc.StatsHandler
// options directly, where ordering and override semantics are ambiguous.
func WithStatsHandler(h stats.Handler) Option {
	return func(o *options) { o.statsHandlers = append(o.statsHandlers, h) }
}

// WithOtel enables OpenTelemetry instrumentation by registering
// otelgrpc.NewServerHandler as a stats.Handler. Uses the global tracer and
// meter providers, so otel.Setup must have run and its Provider must be
// registered with app.App before NewServer is called.
//
// Equivalent to: WithStatsHandler(otelgrpc.NewServerHandler()).
func WithOtel() Option {
	return WithStatsHandler(otelgrpc.NewServerHandler())
}

// WithRecovery installs panic-recovery interceptors for both unary and
// stream RPCs. A panic is logged with stack trace via the configured slog
// logger and returned to the client as codes.Internal.
//
// Recommended for every production server.
func WithRecovery() Option {
	return func(o *options) {
		// Capture logger lazily — at apply time we may not have a logger yet.
		o.unaryInterceptors = append(o.unaryInterceptors, recoveryUnaryInterceptor(o))
		o.streamInterceptors = append(o.streamInterceptors, recoveryStreamInterceptor(o))
	}
}

// NewServer constructs a Server with the given Config and options. Zero
// values in cfg are replaced with defaults. The returned *Server implements
// lifecycle.Runner — register it with app.App.
func NewServer(config Config, opts ...Option) *Server {
	if config.Port == "" {
		config.Port = defaultGRPCPort
	}

	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = defaultShutdownTimeout
	}

	o := &options{logger: slog.Default()} //nolint:exhaustruct
	for _, apply := range opts {
		apply(o)
	}

	grpcOpts := append([]grpc.ServerOption{}, o.serverOptions...)

	if len(o.unaryInterceptors) > 0 {
		grpcOpts = append(grpcOpts, grpc.ChainUnaryInterceptor(o.unaryInterceptors...))
	}

	if len(o.streamInterceptors) > 0 {
		grpcOpts = append(grpcOpts, grpc.ChainStreamInterceptor(o.streamInterceptors...))
	}

	if handler := composeStatsHandlers(o.statsHandlers); handler != nil {
		grpcOpts = append(grpcOpts, grpc.StatsHandler(handler))
	}

	srv := grpc.NewServer(grpcOpts...)

	if o.healthChecker != nil {
		//nolint:exhaustruct // UnimplementedHealthServer has valid zero value
		grpchealth.RegisterHealthServer(srv, &healthService{checker: o.healthChecker})
	}

	//nolint:exhaustruct // started has valid zero value
	return &Server{
		server:   srv,
		listener: o.listener,
		port:     config.Port,
		shutdown: config.ShutdownTimeout,
	}
}

// Mount registers services on the underlying *grpc.Server. The provided
// callback receives the *grpc.Server so generated RegisterXxxServer
// functions can attach implementations.
//
// Mount must be called before Run; calling concurrently with Run is
// undefined behavior.
func (s *Server) Mount(mountFunc func(server *grpc.Server)) {
	mountFunc(s.server)
}

// Run blocks until ctx is cancelled or the server fails. On ctx
// cancellation, Shutdown is called with a fresh ShutdownTimeout context
// (NOT the already-cancelled ctx).
//
// Implements lifecycle.Runner.
//
// Run must be called at most once per Server instance — the underlying
// grpc.Server cannot be reused after GracefulStop. A second call returns
// ErrServerAlreadyStarted.
func (s *Server) Run(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrServerAlreadyStarted
	}

	listener := s.listener

	if listener == nil {
		var err error

		listener, err = (&net.ListenConfig{}).Listen(ctx, "tcp", ":"+s.port) //nolint:exhaustruct
		if err != nil {
			return fmt.Errorf("listen tcp :%s: %w", s.port, err)
		}
	}

	srvErr := make(chan error, 1)

	go func() {
		if err := s.server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			srvErr <- err

			return
		}

		srvErr <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdown)
		defer cancel()

		//nolint:contextcheck // fresh ctx: run ctx is already cancelled
		return s.Shutdown(shutdownCtx)

	case err := <-srvErr:
		return err
	}
}

// Shutdown gracefully stops the server, falling back to a hard Stop when
// ctx expires. Implements lifecycle.Resource.
//
// Idempotent: subsequent calls after the first complete return nil.
func (s *Server) Shutdown(ctx context.Context) error {
	done := make(chan struct{})

	go func() {
		s.server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.server.Stop()

		return fmt.Errorf("grpc graceful shutdown: %w", ctx.Err())
	}
}

// Address returns the server's listen address in ":port" form.
func (s *Server) Address() string {
	return ":" + s.port
}

// recoveryUnaryInterceptor returns an interceptor that recovers panics in
// unary handler invocations and converts them to codes.Internal errors.
// Logs the recovered value plus stack trace via the captured logger.
func recoveryUnaryInterceptor(o *options) grpc.UnaryServerInterceptor {
	return func( //nolint:nonamedreturns // named returns required for defer-based panic recovery
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				o.logger.ErrorContext(ctx, "grpc unary handler panic",
					slog.Any("panic", rec),
					slog.String("method", info.FullMethod),
					slog.String("stack", string(debug.Stack())),
				)

				err = status.Error(codes.Internal, "internal server error")
			}
		}()

		return handler(ctx, req)
	}
}

// recoveryStreamInterceptor mirrors recoveryUnaryInterceptor for streaming
// RPCs.
func recoveryStreamInterceptor(o *options) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				o.logger.ErrorContext(ss.Context(), "grpc stream handler panic",
					slog.Any("panic", rec),
					slog.String("method", info.FullMethod),
					slog.String("stack", string(debug.Stack())),
				)

				err = status.Error(codes.Internal, "internal server error")
			}
		}()

		return handler(srv, ss)
	}
}

// Compile-time assertions.
var (
	_ lifecycle.Runner   = (*Server)(nil)
	_ lifecycle.Resource = (*Server)(nil)
)
