# app

The top-level lifecycle orchestrator. Collects `lifecycle.Runner` and `lifecycle.Resource` components, starts runners concurrently under an `errgroup`, handles SIGINT/SIGTERM, and performs two-phase LIFO shutdown with a bounded timeout.

The single entry point to the application lifecycle. Handles signal-driven shutdown, resource ordering, and healthcheck aggregation.

## When to use

Always — every service built on `core` should use `app.App` as the single entry point to the lifecycle. It removes ~15 lines of boilerplate from every `main.go` and enforces a consistent shutdown contract across the service.

## Quickstart

```go
package main

import (
    "log"
    "time"

    "github.com/sergeyslonimsky/core/app"
    coregrpc "github.com/sergeyslonimsky/core/grpc"
    "github.com/sergeyslonimsky/core/http2"
)

func main() {
    httpSrv := http2.NewServer(http2.Config{Port: "8080"})
    grpcSrv := coregrpc.NewServer(coregrpc.Config{Port: "50051"})

    a := app.New(app.WithShutdownTimeout(30 * time.Second))
    a.Add(httpSrv, grpcSrv)

    if err := a.Run(); err != nil {
        log.Fatal(err)
    }
}
```

## API

```go
func New(opts ...Option) *App
func WithLogger(l *slog.Logger) Option
func WithShutdownTimeout(d time.Duration) Option
func WithSignals(sigs ...os.Signal) Option

func (a *App) Add(components ...any)                        // type-switch dispatch (Runner vs Resource)
func (a *App) AddRunner(runners ...lifecycle.Runner)        // compile-time typed
func (a *App) AddResource(resources ...lifecycle.Resource)  // compile-time typed
func (a *App) Run() error
func (a *App) Healthcheck(ctx context.Context) error
func (a *App) Logger() *slog.Logger
```

Prefer `AddRunner` / `AddResource` when the role is known at the call site — the compiler rejects mistakes instead of the runtime type switch panicking.

## Options

- `WithLogger(*slog.Logger)` — inject a logger and also set `slog.SetDefault`. Default: `slog.Default()`.
- `WithShutdownTimeout(time.Duration)` — upper bound on the Shutdown phase. Default: 30s.
- `WithSignals(...os.Signal)` — signals that trigger shutdown. Default: `os.Interrupt`, `syscall.SIGTERM`. Pass an empty list to disable signal-driven shutdown (useful in tests).

## `Add` — dispatch rules

```go
a.Add(otelProvider, db, redisClient)   // Resources — registered for Shutdown only
a.Add(httpServer, grpcServer)           // Runners   — registered for Run + Shutdown
```

Each argument must implement `lifecycle.Runner` or `lifecycle.Resource`. Since `Runner` embeds `Resource`, the type switch checks `Runner` first to avoid double-registration.

**`Add` panics** if a component implements neither interface or is called after `Run` — both are startup-time programming errors.

## Lifecycle semantics

### Startup

1. `Run` creates a signal-aware context via `signal.NotifyContext`.
2. All registered `Runner`s are started concurrently in an `errgroup`.
3. `Run` blocks until the first of:
   - A signal is received, OR
   - Any runner returns a non-nil error, OR
   - All runners complete (rare, typically only in tests).

### Shutdown (two-phase, LIFO)

When `Run` unblocks, the shutdown phase begins:

1. A fresh context is built with `context.WithTimeout(context.Background(), shutdownTimeout)` — **NOT** the already-cancelled run context.
2. Every `Runner` has `Shutdown(shutdownCtx)` called in **reverse registration order**. Runners registered last shut down first — so frontends stop accepting traffic before backends are closed.
3. Every `Resource` has `Shutdown(shutdownCtx)` called in **reverse registration order**. Typical pattern: OTel is registered first so it shuts down last, letting other components flush telemetry.
4. All errors are joined via `errors.Join` and returned. `context.Canceled` from the run phase is treated as normal termination and omitted.

### Recommended registration order

```go
a.Add(otelProvider)     // Resource: registered first → shuts down LAST
a.Add(sqlDB)            // Resource
a.Add(redisClient)      // Resource
a.Add(kafkaProducer)    // Resource
a.Add(grpcServer)       // Runner: registered before http → shuts down second
a.Add(httpServer)       // Runner: registered last → shuts down FIRST
```

Under SIGTERM, the sequence becomes:

```
httpServer.Shutdown → grpcServer.Shutdown      (stop accepting traffic)
   ↓
kafkaProducer.Shutdown → redisClient.Shutdown → sqlDB.Shutdown   (close pools)
   ↓
otelProvider.Shutdown                                             (flush telemetry)
```

## Healthcheck aggregation

```go
err := a.Healthcheck(ctx)
```

Iterates every component that implements `lifecycle.Healthchecker` and returns `errors.Join` of all failures, or nil if all are healthy. `App` itself implements `Healthchecker`, so you can wire it into an HTTP `/readyz` handler via:

```go
httpServer := http2.NewServer(cfg, http2.WithHealthcheckFrom(a))
```

## Extending

No `Unwrap` — `App` is a terminal orchestrator, not a wrapper. If you need lower-level control, build your own runner atop `errgroup` and call `Run(ctx)` from your code. But almost every case that looks like it needs that is better served by adding more `Runner`s to the same `App`.

## Testing

```go
// Disable signal handling so tests don't interact with the process's signals.
a := app.New(app.WithSignals())

// Inject a short shutdown timeout to keep tests fast.
a := app.New(app.WithShutdownTimeout(100*time.Millisecond))
```

See `app_test.go` for LIFO-ordering, healthcheck, and timeout tests using a `recorder`-based fake.

## See also

- [`core/lifecycle`](../lifecycle/README.md) — the `Resource`, `Runner`, `Healthchecker` interfaces.
- [`core/di`](../di/README.md) — optional thin container that returns a typed `Config`/`Services` pair for a consistent DI pattern.
