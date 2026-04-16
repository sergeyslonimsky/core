# grpc

Opinionated gRPC server wrapper that implements `lifecycle.Runner`. Mirrors `core/http2`'s API shape (functional options, fresh-context Shutdown, single registration with `app.App`).

## When to use

Any gRPC-facing endpoint of a service: ConnectRPC handlers, etcd v3 emulation, internal service-to-service RPCs.

## Quickstart

```go
package main

import (
    "log"

    coregrpc "github.com/sergeyslonimsky/core/grpc"
    "github.com/sergeyslonimsky/core/app"
    "google.golang.org/grpc"

    pb "myapp/proto"
)

func main() {
    srv := coregrpc.NewServer(coregrpc.Config{Port: "50051"},
        coregrpc.WithRecovery(),
        coregrpc.WithOtel(),
    )
    srv.Mount(func(s *grpc.Server) {
        pb.RegisterMyServiceServer(s, &myImpl{})
    })

    a := app.New()
    a.Add(srv)
    log.Fatal(a.Run())
}
```

## Configuration

```go
type Config struct {
    Port            string        // Default "50051"
    ShutdownTimeout time.Duration // Default 10s
}
```

## Options

### Lifecycle / wiring

- `WithListener(net.Listener)` — override the default TCP listener (tests, unix sockets, bufconn).
- `WithLogger(*slog.Logger)` — used by `WithRecovery` to log panics. Default: `slog.Default()`.

### Server-level escape hatch

- `WithServerOptions(...grpc.ServerOption)` — pass any underlying `grpc.ServerOption` (keepalive, TLS credentials, max message size).

### Interceptors and observability

- `WithUnaryInterceptor(...grpc.UnaryServerInterceptor)` — append unary interceptors. Multiple calls accumulate; chained in registration order.
- `WithStreamInterceptor(...grpc.StreamServerInterceptor)` — same, for streaming RPCs.
- `WithStatsHandler(stats.Handler)` — append a stats handler. Multiple calls fan out via internal composite (no override).
- `WithOtel()` — convenience: `WithStatsHandler(otelgrpc.NewServerHandler())`.
- `WithRecovery()` — install panic-recovery interceptors for unary + stream that log and return `codes.Internal`.
- `WithHealthService(lifecycle.Healthchecker)` — registers a `grpc.health.v1.Health` service backed by the given Healthchecker (typically an `*app.App`). Answers `SERVING` / `NOT_SERVING` via `Check`. `Watch` is not implemented. Works with Envoy and k8s gRPC health probes.

## Observability (`WithOtel`)

Calls `otelgrpc.NewServerHandler()` from `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc` and registers it via `WithStatsHandler`. Uses the global tracer/meter providers.

**Call `otel.Setup` first** and register the provider with `app.App` before constructing the gRPC server, or instrumentation will be noop.

`WithOtel` and `WithStatsHandler` compose freely — register otelgrpc plus your own custom stats handler (e.g., a `ClientRegistry` bridge):

```go
srv := coregrpc.NewServer(cfg,
    coregrpc.WithOtel(),
    coregrpc.WithStatsHandler(myClientRegistryStatsHandler),
)
```

Both handlers receive every event.

## Health checks

```go
a := app.New()
a.Add(otelProvider, db, redisClient)

grpcSrv := coregrpc.NewServer(cfg,
    coregrpc.WithRecovery(),
    coregrpc.WithOtel(),
    coregrpc.WithHealthService(a),   // gRPC health.v1 answers SERVING/NOT_SERVING via app.App.Healthcheck
)
a.Add(grpcSrv)
```

`app.App.Healthcheck` aggregates every registered component that implements `lifecycle.Healthchecker`, so this picks up sql/redis/kafka health automatically.

## Lifecycle

Implements `lifecycle.Runner`. Register with `app.App.Add(srv)`. The app handles startup, signal-driven shutdown, and ordering — never call `srv.Run`/`srv.Shutdown` from your code outside of tests.

`Run` is single-use — a second call on the same `*Server` is a programming error because the underlying `grpc.Server` cannot be reused after `GracefulStop`.

### Shutdown semantics

`Run` blocks until ctx is cancelled, then calls `Shutdown` with a fresh context built from `Config.ShutdownTimeout`. `Shutdown` runs `grpc.Server.GracefulStop` in a goroutine and races it against the ctx — if the context expires before graceful drain completes, it calls `grpc.Server.Stop` (force close).

## Mounting services

```go
srv.Mount(func(s *grpc.Server) {
    pb.RegisterFooServer(s, fooImpl)
    pb.RegisterBarServer(s, barImpl)
})
```

The callback receives the underlying `*grpc.Server` so generated `RegisterXxxServer` functions can attach implementations. Call once during setup before `Run`.

## Extending

There is no `Unwrap` accessor by design — server-level customisation goes through `WithServerOptions`. The `Mount` callback already exposes the underlying `*grpc.Server` for service registration, which covers every common extension point.

## Testing

```go
l, _ := net.Listen("tcp", "127.0.0.1:0")
srv := coregrpc.NewServer(coregrpc.Config{}, coregrpc.WithListener(l))
go srv.Run(ctx)
// ... dial l.Addr().String()
```

## See also

- [`core/app`](../app/README.md) — register your server here.
- [`core/lifecycle`](../lifecycle/README.md) — the `Runner` contract.
- [`core/http2`](../http2/README.md) — sibling HTTP server with the same lifecycle shape.
