// Package app provides the top-level orchestrator that ties every
// lifecycle.Runner and lifecycle.Resource registered by a service into a
// single managed lifecycle: signal-aware startup, errgroup-based run, and
// two-phase LIFO shutdown with a bounded timeout.
//
// A typical main.go looks like:
//
//	func main() {
//	    a := app.New(app.WithShutdownTimeout(30 * time.Second))
//	    a.Add(otelProvider, db, redisClient)   // Resources
//	    a.Add(grpcServer, httpServer)          // Runners
//	    if err := a.Run(); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//
// See the package README.md for the full semantics of the two-phase lifecycle
// and the rationale for LIFO shutdown ordering.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/sergeyslonimsky/core/lifecycle"
)

// defaultShutdownTimeout is the upper bound on the Shutdown phase when the
// caller has not supplied one via WithShutdownTimeout.
const defaultShutdownTimeout = 30 * time.Second

// App is the top-level lifecycle orchestrator. It collects Runner and
// Resource components via Add, starts all Runners when Run is called, and
// shuts them down in reverse registration order when the context is
// cancelled (typically by SIGINT/SIGTERM).
//
// App is safe for single-goroutine use during setup. Add must not be called
// after Run — doing so is a programming error and will panic.
type App struct {
	logger          *slog.Logger
	signals         []os.Signal
	shutdownTimeout time.Duration

	mu        sync.Mutex
	started   bool
	runners   []lifecycle.Runner
	resources []lifecycle.Resource
}

// Option configures an App. Options are applied in order by New.
type Option func(*App)

// WithLogger injects a *slog.Logger that App will use for lifecycle events
// (startup, shutdown order, errors). As a convenience it also calls
// slog.SetDefault so third-party libraries and the stdlib log package route
// through the same handler.
//
// If no logger is provided, App uses slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(a *App) {
		a.logger = l
		slog.SetDefault(l)
	}
}

// WithShutdownTimeout overrides the default 30-second shutdown budget. The
// timeout bounds the total time allowed for the shutdown phase — Runners and
// Resources share this single deadline, applied via context.WithTimeout in
// Run.
//
// Setting a timeout shorter than any individual component's graceful-stop
// duration will cause that component to be force-closed — this is the correct
// behavior for bounded-duration shutdowns, but callers should size the
// timeout accordingly.
func WithShutdownTimeout(d time.Duration) Option {
	return func(a *App) { a.shutdownTimeout = d }
}

// WithSignals overrides the default signal set (SIGINT, SIGTERM) that
// triggers App shutdown. Pass an empty slice to disable signal-driven
// shutdown entirely — the app will then only stop when Run's context (if
// supplied via an external mechanism) is cancelled.
func WithSignals(sigs ...os.Signal) Option {
	return func(a *App) { a.signals = sigs }
}

// New creates an App with the given options. Defaults:
//
//   - Logger: slog.Default()
//   - Shutdown timeout: 30 seconds
//   - Signals: SIGINT, SIGTERM
func New(opts ...Option) *App {
	a := &App{ //nolint:exhaustruct // mu, started, runners, resources have valid zero values
		logger:          slog.Default(),
		signals:         []os.Signal{os.Interrupt, syscall.SIGTERM},
		shutdownTimeout: defaultShutdownTimeout,
	}
	for _, opt := range opts {
		opt(a)
	}

	return a
}

// Add registers one or more components. Each argument must implement
// lifecycle.Runner or lifecycle.Resource. Runners go to the runners slice
// (Run + Shutdown); pure Resources go to the resources slice (Shutdown only).
// Since Runner embeds Resource, the type switch checks Runner first to avoid
// double-registration.
//
// Add panics if a component implements neither interface — this indicates a
// programming error at startup and there is no reasonable recovery. The
// panic message includes the component's type for easy diagnosis.
//
// Add must not be called after Run. Calling it concurrently with Run is
// undefined behavior.
//
// Prefer AddRunner and AddResource when the component's role is known at
// the call site — they give compile-time type safety.
func (a *App) Add(components ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.started {
		panic("app.App.Add: called after Run")
	}

	for _, c := range components {
		switch v := c.(type) {
		case lifecycle.Runner:
			a.runners = append(a.runners, v)
		case lifecycle.Resource:
			a.resources = append(a.resources, v)
		default:
			panic(fmt.Sprintf(
				"app.App.Add: %T implements neither lifecycle.Runner nor lifecycle.Resource",
				c,
			))
		}
	}
}

// AddRunner registers one or more Runners. Equivalent to Add with
// compile-time type safety — the compiler rejects non-Runner arguments.
//
// Must not be called after Run; panics otherwise.
func (a *App) AddRunner(runners ...lifecycle.Runner) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.started {
		panic("app.App.AddRunner: called after Run")
	}

	a.runners = append(a.runners, runners...)
}

// AddResource registers one or more Resources. Equivalent to Add with
// compile-time type safety — the compiler rejects non-Resource arguments.
//
// Note: a lifecycle.Runner is also a lifecycle.Resource (Runner embeds
// Resource), so passing a Runner here registers it as Resource-only —
// its Run method will NOT be invoked. Use AddRunner when Run is required.
//
// Must not be called after Run; panics otherwise.
func (a *App) AddResource(resources ...lifecycle.Resource) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.started {
		panic("app.App.AddResource: called after Run")
	}

	a.resources = append(a.resources, resources...)
}

// Run starts every registered Runner concurrently, installs signal handlers,
// and blocks until:
//
//  1. A signal from WithSignals is received, OR
//  2. A Runner's Run method returns a non-nil error, OR
//  3. All Runners complete normally (rare — typically only in tests).
//
// On any of these, Run begins the shutdown phase:
//
//   - A fresh context with WithShutdownTimeout deadline is created.
//   - Runners have Shutdown called in reverse registration order (so
//     frontends stop accepting traffic before backends are closed).
//   - Resources then have Shutdown called in reverse registration order.
//   - All shutdown errors, plus the original run error (if any), are joined
//     and returned via errors.Join.
//
// A context.Canceled from the first phase is treated as normal termination
// and not included in the returned error.
func (a *App) Run() error {
	a.mu.Lock()
	a.started = true
	runners := a.runners
	resourceCount := len(a.resources)
	a.mu.Unlock()

	ctx, stop := signal.NotifyContext(context.Background(), a.signals...)
	defer stop()

	a.logger.InfoContext(ctx, "app starting",
		slog.Int("runners", len(runners)),
		slog.Int("resources", resourceCount),
	)

	g, gctx := errgroup.WithContext(ctx)
	for _, r := range runners {
		g.Go(func() error {
			if err := r.Run(gctx); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("%T: %w", r, err)
			}

			return nil
		})
	}

	runErr := g.Wait()

	// If g.Wait returned because of a runner error (not signal), the
	// signal-aware ctx may still be live. We proceed to shutdown regardless —
	// a failed runner is a trigger to stop the rest cleanly.
	a.logger.Info("app shutting down", slog.Any("runErr", runErr))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
	defer cancel()

	shutdownErr := a.shutdown(shutdownCtx)

	if runErr != nil || shutdownErr != nil {
		return errors.Join(runErr, shutdownErr)
	}

	return nil
}

// Healthcheck aggregates every registered component that implements
// lifecycle.Healthchecker. It returns nil if all checkers report healthy, or
// errors.Join of every failing checker's error otherwise.
//
// Typical use: mount this on /readyz inside an http2.Server via
// http2.WithHealthcheckFrom(a).
//
// Components that do not implement lifecycle.Healthchecker are silently
// skipped — they contribute neither success nor failure to the result.
//
// Healthcheck is safe to call concurrently and at any time, including after
// Run has returned. It does not take App-level locks — checkers are iterated
// from the slices populated at startup.
func (a *App) Healthcheck(ctx context.Context) error {
	var errs []error

	for _, r := range a.runners {
		if hc, ok := r.(lifecycle.Healthchecker); ok {
			if err := hc.Healthcheck(ctx); err != nil {
				errs = append(errs, fmt.Errorf("%T: %w", r, err))
			}
		}
	}

	for _, r := range a.resources {
		if hc, ok := r.(lifecycle.Healthchecker); ok {
			if err := hc.Healthcheck(ctx); err != nil {
				errs = append(errs, fmt.Errorf("%T: %w", r, err))
			}
		}
	}

	return errors.Join(errs...)
}

// Logger returns the *slog.Logger configured via WithLogger, or slog.Default()
// if none was set. Components that want to emit lifecycle-scoped logs can
// retrieve the app's logger here.
func (a *App) Logger() *slog.Logger {
	return a.logger
}

// shutdown is the reverse-registration-order teardown. Runners first
// (stop accepting work), then Resources (release connections).
func (a *App) shutdown(ctx context.Context) error {
	var errs []error

	for i := len(a.runners) - 1; i >= 0; i-- {
		r := a.runners[i]
		if err := r.Shutdown(ctx); err != nil {
			a.logger.Error("runner shutdown failed",
				slog.String("component", fmt.Sprintf("%T", r)),
				slog.Any("err", err),
			)
			errs = append(errs, fmt.Errorf("shutdown %T: %w", r, err))
		}
	}

	for i := len(a.resources) - 1; i >= 0; i-- {
		r := a.resources[i]
		if err := r.Shutdown(ctx); err != nil {
			a.logger.Error("resource shutdown failed",
				slog.String("component", fmt.Sprintf("%T", r)),
				slog.Any("err", err),
			)
			errs = append(errs, fmt.Errorf("shutdown %T: %w", r, err))
		}
	}

	return errors.Join(errs...)
}

// Compile-time check that App satisfies lifecycle.Healthchecker. This lets
// an App be passed wherever a Healthchecker is expected (e.g., as the
// argument to http2.WithHealthcheckFrom).
var _ lifecycle.Healthchecker = (*App)(nil)
