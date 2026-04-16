# otel

OpenTelemetry SDK bootstrap for traces, metrics, and logs over OTLP/HTTP. Returns a `*Provider` that implements `lifecycle.Resource` so the SDK is shut down with the rest of the service — last in the LIFO chain so other components can finish flushing telemetry first.

Once `Setup` has run, the global `TracerProvider`, `MeterProvider`, and `LoggerProvider` are non-noop and discoverable by every other core/* package via their `WithOtel()` options.

## When to use

Always, in production. For local dev or tests without a collector, set `Config.Disabled = true` — Setup returns a noop Provider and the same code path works. An empty `OTelHost` with `Disabled=false` is an error (`ErrEmptyOTelHost`) so a missing env var fails loudly instead of silently disabling observability.

## Quickstart

```go
package main

import (
    "context"
    "log"

    "github.com/sergeyslonimsky/core/app"
    "github.com/sergeyslonimsky/core/otel"
)

func main() {
    ctx := context.Background()

    p, err := otel.Setup(ctx, otel.Config{
        OTelHost:      "otel-collector:4318",
        ServiceName:   "myservice",
        ServiceVersion: "v1.2.3",
        K8SNamespace:  "production",
        K8SPodName:    os.Getenv("HOSTNAME"),
        EnableTracer:  true,
        EnableMetrics: true,
        EnableLogger:  true,
    })
    if err != nil {
        log.Fatal(err)
    }

    a := app.New()
    a.Add(p)               // FIRST → shuts down LAST → final telemetry flush
    // ... add other Resources and Runners
    log.Fatal(a.Run())
}
```

## Configuration

```go
type Config struct {
    Disabled       bool    // explicit opt-out: returns noop Provider when true

    OTelHost       string  // OTLP/HTTP endpoint (host:port). Required unless Disabled.
    ServiceName    string  // service.name resource attr (required when enabled)
    ServiceVersion string  // service.version
    HostName       string  // host.name
    K8SNamespace   string  // k8s.namespace.name
    K8SNodeName    string  // k8s.node.name
    K8SPodName     string  // k8s.pod.name

    EnableTracer   bool
    EnableMetrics  bool
    EnableLogger   bool
}
```

## Options

- `WithMetricsSendInterval(d time.Duration)` — exporter cadence. Default: 3s.
- `WithHistogramBuckets(boundaries []float64)` — explicit histogram bucket boundaries (ms). Default: `[5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]`.

## Lifecycle ordering — critical

OTel must be **registered first** with `app.App` so it shuts down **last**. Otherwise the SDK closes before other components finish emitting their final telemetry, and you lose the most interesting spans (the shutdown ones).

```go
a.Add(otelProvider)  // ← first, shuts down LAST
a.Add(db)
a.Add(redisClient)
a.Add(httpServer)    // ← last, shuts down FIRST
```

## Why opt-in (`WithOtel`) per package, not auto-discover?

Every other core/* package has an opt-in `WithOtel()` option that reads the global providers Setup installs. We chose explicit opt-in over auto-detect so:

- A code reviewer sees exactly which servers/clients are instrumented.
- Disabling instrumentation is a localised one-line change, not a global flag.
- Tests can construct a server without inadvertently picking up real exporters via the global state.

The flow:

1. `otel.Setup` runs first, sets globals.
2. Each per-package `WithOtel()` reads `otel.GetTracerProvider()` / `otel.GetMeterProvider()` at construction time.
3. Without `Setup`, the globals are noop and all `WithOtel()` calls become free.

## Disabling explicitly (`Disabled = true`)

For local dev or tests without a collector:

```go
p, err := otel.Setup(ctx, otel.Config{Disabled: true})
```

- Setup logs `"otel disabled via Config.Disabled"` and returns a `*Provider` with no shutdown funcs.
- All `WithOtel()` calls in other packages still work — they just attach noop providers.

If `Disabled=false` but `OTelHost` is empty, Setup returns `ErrEmptyOTelHost`. This is intentional: a missing `OTEL_HOST` env var should fail the boot, not silently disable observability.

## Lifecycle

`*Provider` implements `lifecycle.Resource`. `Shutdown` invokes every registered SDK shutdown (trace, metrics, logger) in registration order and joins errors. Idempotent and concurrent-safe (guarded by `sync.Once`): repeat calls return the same cached error.

## Testing

For tests that need to verify spans, run a Jaeger / OTLP collector in a container or use an in-memory exporter:

```go
exp := tracetest.NewInMemoryExporter()
tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
otel.SetTracerProvider(tp)
// ... run code under test, then exp.GetSpans() returns recorded spans
```

Skip `core/otel.Setup` entirely in tests — it creates real network exporters.

## See also

- [`core/app`](../app/README.md) — register the Provider here, FIRST.
- [`core/lifecycle`](../lifecycle/README.md) — the `Resource` contract.
- Per-package `WithOtel()` integrations: [`http2`](../http2/README.md), [`grpc`](../grpc/README.md), [`sql`](../sql/README.md), [`redis`](../redis/README.md), `kafka`, `rabbitmq`.
