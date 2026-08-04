# http2

HTTP/2-capable server with built-in `/livez`, `/readyz`, `/metrics`, opt-in OpenTelemetry instrumentation, panic recovery, and a context-safe graceful shutdown. Implements `lifecycle.Runner`.

## When to use

Any HTTP-facing endpoint of a service: REST APIs, ConnectRPC handlers, SPA static-asset serving, gRPC-Web, server-streaming SSE.

## Quickstart

```go
package main

import (
    "log"
    "time"

    "github.com/sergeyslonimsky/core/app"
    "github.com/sergeyslonimsky/core/http2"
)

func main() {
    srv := http2.NewServer(http2.Config{
        Port:         "8080",
        WriteTimeout: 24 * time.Hour, // for server-streaming endpoints
    },
        http2.WithRecovery(),
        http2.WithOtel(),
    )
    srv.Mount("/api", apiHandler())

    a := app.New()
    a.Add(srv)
    log.Fatal(a.Run())
}
```

## Configuration

```go
type Config struct {
    Port            string        // Default "80"
    ReadTimeout     time.Duration // Default 1s
    WriteTimeout    time.Duration // Default 10s. For streaming, set to hours.
    ShutdownTimeout time.Duration // Default 10s
}
```

App-level mapping example:

```go
http2.Config{
    Port:            raw.GetStringOrDefault("http.frontend.port", "8080"),
    ReadTimeout:     raw.GetDuration("http.frontend.read_timeout"),
    WriteTimeout:    raw.GetDuration("http.frontend.write_timeout"),
    ShutdownTimeout: raw.GetDuration("http.frontend.shutdown_timeout"),
}
```

## Options

- `WithMiddleware(mw ...func(http.Handler) http.Handler)` — append user middlewares; outer-first ordering.
- `WithListener(l net.Listener)` — override the default TCP listener (tests, unix sockets).
- `WithLogger(*slog.Logger)` — logger used by recovery middleware and lifecycle events. Default: `slog.Default()`.
- `WithOtel()` — wrap final handler with `otelhttp.NewHandler` using global tracer/meter providers.
- `WithRecovery()` — install panic-recovery middleware that logs the stack and replies 500.
- `WithHealthcheckFrom(lifecycle.Healthchecker)` — wire `/readyz` aggregation (typically pass `*app.App`).
- `WithReadyzCheck(name string, fn func(ctx) error)` — add an inline readiness check.
- `WithMetricsHandler(http.Handler)` — install a `/metrics` handler (typically `promhttp.Handler` over an otel Prometheus exporter).

## Built-in routes

| Path | Behavior |
|---|---|
| `GET /livez` | Always 200. The process is up. |
| `GET /readyz` | 200 if every Healthchecker (`WithHealthcheckFrom` + `WithReadyzCheck`) is healthy, else 503 with the failed errors in the body. |
| `GET /metrics` | The handler from `WithMetricsHandler`, or 404 if none was provided. |

## Observability (`WithOtel`)

Wraps the final handler chain with `otelhttp.NewHandler(handler, "http-"+cfg.Port)`. Uses the global `TracerProvider` and `MeterProvider`. **Call `otel.Setup` and register the provider with `app.App` BEFORE constructing this server**, or the providers will be noop:

```go
otelSDK, err := otel.Setup(ctx, cfg.OTel)
// ... handle err
a.Add(otelSDK)            // register first → shut down last → flushes telemetry
srv := http2.NewServer(cfg.Frontend, http2.WithOtel())
a.Add(srv)
```

Imports `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp`.

## Lifecycle

`http2.Server` implements `lifecycle.Runner`. Register with `app.App.Add(srv)`. The app handles startup, signal-driven shutdown, and ordering — never call `srv.Run`/`srv.Shutdown` from your code outside of tests.

`Run` is single-use — a second call on the same `*Server` returns `ErrServerAlreadyStarted` because the underlying `http.Server` cannot be reused after `Shutdown`.

Built-in middlewares are layered as `user middlewares → recovery → otelhttp → mux`. User middlewares are outermost so panics inside them are still caught by recovery, and traces span user-middleware work.

### Recovery behaviour

`WithRecovery` wraps the response writer to track whether the handler already committed the response. If a panic happens AFTER any write (status code or body), the middleware logs the panic but does NOT attempt to emit `500 internal server error` — overwriting committed headers is a no-op and re-writing the body would corrupt HTTP framing. The already-partially-written response reaches the client as-is.

If the panic happens BEFORE the response was committed, a clean `500 internal server error` is emitted.

### Shutdown semantics

`Run` blocks until ctx is cancelled, then calls `Shutdown` with a **fresh timeout context** built from `cfg.ShutdownTimeout` — NOT the already-cancelled run ctx. This ensures in-flight requests are drained within the configured timeout rather than being force-closed immediately.

A regression test in `server_test.go` (`TestServer_Shutdown_FreshContext_DoesNotShortCircuitOnCancelledRunCtx`) covers this.

## Extending

There is no `Unwrap` method. The underlying `http.ServeMux` and `http.Server` are package-private — extend by:
- Adding more middleware via `WithMiddleware`.
- Mounting handlers via `Mount(pattern, handler)`.
- Composing your own handler stack and passing the result to `Mount("/", composed)`.

## Testing

```go
l, _ := net.Listen("tcp", "127.0.0.1:0")  // random port
srv := http2.NewServer(http2.Config{}, http2.WithListener(l))
go srv.Run(ctx)
// ... test against l.Addr()
```

See `server_test.go` for the full test suite covering routes, middleware order, panic recovery, healthchecks, and the shutdown bug regression.

## Outbound client (`NewClient`)

Symmetric to `NewServer` for the outbound direction — a tuned `*http.Client` for service-to-service and third-party API calls:

```go
client := http2.NewClient(
    http2.WithResponseHeaderTimeout(30 * time.Second), // slow upstream TTFB
    http2.WithClientOtel(),
    http2.WithRoundTripper(func(next http.RoundTripper) http.RoundTripper {
        return authTransport{next: next, apiKey: cfg.APIKey} // injects a header, then delegates
    }),
)
```

- Builds a fresh `*http.Transport` (not a clone of `http.DefaultTransport`) and tunes pool/timeout fields: `WithMaxIdleConns`, `WithMaxIdleConnsPerHost`, `WithDialTimeout`, `WithKeepAlive`, `WithTLSHandshakeTimeout`, `WithResponseHeaderTimeout`, `WithIdleConnTimeout`. `Proxy: http.ProxyFromEnvironment` and `ForceAttemptHTTP2: true` are always set — the latter is required because a custom `DialContext`/`TLSClientConfig` otherwise makes `net/http` skip its automatic HTTP/2 upgrade.
- `WithTLSConfig(*tls.Config)` — mTLS client certs, a custom root CA pool, or (non-prod only) `InsecureSkipVerify`.
- `WithTransportOptions(func(*http.Transport))` — escape hatch for `*http.Transport` fields with no dedicated option (`MaxConnsPerHost`, `DisableCompression`, ...), applied after every named option.
- `WithRoundTripper(func(http.RoundTripper) http.RoundTripper)` — layer cross-cutting concerns above the transport: auth headers, API keys, HMAC signing, custom retries. First-passed wrapper is outermost (sees the request first), mirroring `WithMiddleware`'s ordering on the server side.
- `WithClientOtel(opts ...otelhttp.Option)` — opt-in otelhttp instrumentation, matching every other `core/*` package's `WithOtel` convention (disabled unless passed). Named `WithClientOtel` rather than `WithOtel` only because both `Option` types live in this package and Go doesn't allow overloading. Always innermost in the RoundTripper chain, so a span covers only the actual network round trip, not time spent in `WithRoundTripper` wrappers. Pass `otelhttp.WithMetricAttributesFn(...)` to trim cardinality.
- `http.Client.Timeout` is intentionally left unset — bound calls with `context.WithTimeout`/`WithDeadline`, not a client-wide wall clock.

## See also

- [`core/app`](../app/README.md) — register your server here.
- [`core/lifecycle`](../lifecycle/README.md) — the `Runner` / `Healthchecker` contracts.
- [`core/grpc`](../grpc/README.md) — sibling gRPC server with the same lifecycle shape.
