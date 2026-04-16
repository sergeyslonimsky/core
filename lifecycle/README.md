# lifecycle

Three interfaces that every component registered with `core/app` must implement in some combination: `Resource`, `Runner`, and `Healthchecker`. This is a zero-dependency leaf package designed to break import cycles between `core/app` and the individual service packages (`http2`, `grpc`, `redis`, ...).

## When to use

- Implement `Resource` when your type holds connections, pools, or background goroutines that need to be cleaned up on application shutdown (database clients, message publishers, SDK providers).
- Implement `Runner` when your type's primary purpose is blocking background work (HTTP servers, gRPC servers, message consumers).
- Implement `Healthchecker` when your component can answer "am I ready to serve traffic?" cheaply (connection pings, cache status).

## Quickstart

```go
package myservice

import (
    "context"
    "sync"

    "github.com/sergeyslonimsky/core/lifecycle"
)

// Resource example: a client with a background refresh goroutine.
type Client struct {
    mu     sync.Mutex
    cancel context.CancelFunc
}

func New(ctx context.Context) *Client {
    bgCtx, cancel := context.WithCancel(context.Background())
    c := &Client{cancel: cancel}
    go c.refreshLoop(bgCtx)
    return c
}

func (c *Client) Shutdown(ctx context.Context) error {
    c.cancel()
    return nil
}

// Compile-time assertion.
var _ lifecycle.Resource = (*Client)(nil)
```

## Interfaces

### Resource

```go
type Resource interface {
    Shutdown(ctx context.Context) error
}
```

Stateful holder with no long-running foreground work. `Shutdown` must be idempotent and honor ctx deadlines.

### Runner

```go
type Runner interface {
    Resource
    Run(ctx context.Context) error
}
```

Extends `Resource` with `Run(ctx)`, which blocks until ctx is cancelled or a fatal error occurs. Runners are orchestrated by `core/app` inside an `errgroup`.

Returning `context.Canceled` or `nil` from `Run` on ctx cancellation is treated as success.

### Healthchecker

```go
type Healthchecker interface {
    Healthcheck(ctx context.Context) error
}
```

Optional. `core/app.App.Healthcheck` discovers implementors via type assertion and aggregates their results.

## Lifecycle

This package itself has no lifecycle — it declares interfaces only. Components that implement these interfaces are registered with `core/app.App` via `a.Add(component)`.

Registration order matters: `app.App` shuts down in reverse order, so earlier-registered components are closed later. Typical pattern:

```go
a.Add(otelProvider)  // registered first → closed last → flushes remaining telemetry
a.Add(db)            // closed before otel
a.Add(redis)         // closed before db
a.Add(httpServer)    // registered last → closed first → stops accepting traffic first
```

## Testing

Components that implement these interfaces should add a compile-time assertion in the same package:

```go
var (
    _ lifecycle.Resource      = (*Client)(nil)
    _ lifecycle.Runner        = (*Server)(nil)
    _ lifecycle.Healthchecker = (*Client)(nil)
)
```

This catches interface drift at compile time rather than in integration tests.

## See also

- [`core/app`](../app/README.md) — the orchestrator that consumes Resource/Runner/Healthchecker.
