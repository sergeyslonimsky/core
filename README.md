# core

[![CI](https://github.com/sergeyslonimsky/core/actions/workflows/ci.yml/badge.svg)](https://github.com/sergeyslonimsky/core/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/sergeyslonimsky/core)](https://go.dev/doc/go1.25)
[![Go Report Card](https://goreportcard.com/badge/github.com/sergeyslonimsky/core)](https://goreportcard.com/report/github.com/sergeyslonimsky/core)
[![Latest Release](https://img.shields.io/github/v/release/sergeyslonimsky/core)](https://github.com/sergeyslonimsky/core/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Go framework for bootstrapping observable production services. Provides one unified lifecycle contract (`Resource` / `Runner`), one option pattern, and opt-in OpenTelemetry across HTTP, gRPC, SQL, Redis, Kafka, RabbitMQ — so a fully observable service can be assembled in ~25 lines of `main.go`.

## Quickstart — minimal observable service

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/sergeyslonimsky/core/app"
    coregrpc "github.com/sergeyslonimsky/core/grpc"
    "github.com/sergeyslonimsky/core/http2"
    "github.com/sergeyslonimsky/core/otel"
    "github.com/sergeyslonimsky/core/redis"
)

func main() {
    ctx := context.Background()

    otelSDK, err := otel.Setup(ctx, otel.Config{
        OTelHost:      "otel-collector:4318",
        ServiceName:   "myservice",
        EnableTracer:  true,
        EnableMetrics: true,
    })
    if err != nil { log.Fatal(err) }

    redisClient, err := redis.New(ctx, redis.Config{Host: "redis", Port: "6379"}, redis.WithOtel())
    if err != nil { log.Fatal(err) }

    httpServer := http2.NewServer(http2.Config{Port: "8080"},
        http2.WithOtel(),
        http2.WithRecovery(),
    )
    grpcServer := coregrpc.NewServer(coregrpc.Config{Port: "50051"},
        coregrpc.WithOtel(),
        coregrpc.WithRecovery(),
    )

    a := app.New(app.WithShutdownTimeout(30 * time.Second))
    a.Add(otelSDK, redisClient)         // Resources: Redis closes first, OTel last
    a.Add(grpcServer, httpServer)       // Runners: HTTP stops first, gRPC second

    httpServer.Mount("/", myHandler())  // mount your routes
    log.Fatal(a.Run())
}
```

This service:

- Handles SIGINT / SIGTERM gracefully.
- Shuts down in LIFO order: HTTP stops accepting → gRPC stops → Redis closes → OTel flushes telemetry last.
- Exposes `/livez`, `/readyz`, `/metrics` automatically on the HTTP server.
- Emits OpenTelemetry traces and metrics for every HTTP request, gRPC RPC, and Redis command.

## Core concepts

`core` revolves around three small interfaces in [`core/lifecycle`](./lifecycle/README.md):

```go
type Resource interface {
    Shutdown(ctx context.Context) error
}

type Runner interface {
    Resource
    Run(ctx context.Context) error
}

type Healthchecker interface {
    Healthcheck(ctx context.Context) error
}
```

- **`Resource`** — components that hold connections / pools but don't block the caller (Redis, SQL, Kafka producer, OTel SDK, RabbitMQ publisher).
- **`Runner`** — components whose primary contract is long-running background work (HTTP/gRPC servers, Kafka consumer, RabbitMQ consumer host).
- **`Healthchecker`** — optional. Aggregated by `app.App.Healthcheck` and surfaced on `/readyz`.

Everything else in `core` is a typed wrapper that implements one or more of these interfaces and registers with [`core/app`](./app/README.md).

### Two-phase lifecycle

```
Phase A: construct Resources, register with app.App in the order they should be shut down LAST
Phase B: app.Run starts every Runner concurrently in errgroup; blocks on signal
Phase C: SIGTERM → Runners shutdown LIFO (frontends first), then Resources LIFO (DB / Redis / OTel last)
```

## Package map

| Package | Role | Description | Docs |
|---|---|---|---|
| [`lifecycle`](./lifecycle/README.md) | Interfaces | `Resource`, `Runner`, `Healthchecker` | [docs](./lifecycle/README.md) |
| [`app`](./app/README.md) | Orchestrator | Lifecycle entry point: `New`, `Add`, `Run` | [docs](./app/README.md) |
| [`di`](./di/README.md) | Helper | viper config loader + thin generic Container | [docs](./di/README.md) |
| [`http2`](./http2/README.md) | Runner | HTTP/2 server with `/livez`, `/readyz`, `/metrics` | [docs](./http2/README.md) |
| [`grpc`](./grpc/README.md) | Runner | gRPC server with interceptors and stats handlers | [docs](./grpc/README.md) |
| [`otel`](./otel/README.md) | Resource | OpenTelemetry SDK bootstrap (OTLP/HTTP) | [docs](./otel/README.md) |
| [`sql`](./sql/README.md) | Resource | sqlx wrapper with pool tuning, transactions, OTel | [docs](./sql/README.md) |
| [`sql/postgres`](./sql/postgres/README.md) | Helper | PostgreSQL DSN builder | [docs](./sql/postgres/README.md) |
| [`redis`](./redis/README.md) | Resource | go-redis wrapper with OTel | [docs](./redis/README.md) |
| [`kafka`](./kafka/README.md) | Runner+Resource | franz-go wrapper. Producer is Resource, Consumer is Runner | [docs](./kafka/README.md) |
| [`rabbitmq`](./rabbitmq/README.md) | Runner+Resource | AMQP wrapper. Publisher is Resource, ConsumerHost is Runner | [docs](./rabbitmq/README.md) |
| [`jwt`](./jwt/README.md) | Stateless | `Signer`/`Verifier` interfaces + AuthService | [docs](./jwt/README.md) |
| [`jwt/hs256`](./jwt/hs256/README.md) | Stateless | HMAC-SHA256 implementation of jwt.Signer/Verifier | [docs](./jwt/hs256/README.md) |
| `internal/integration` | Internal | Integration tests + testcontainers helpers (separate go module — deps don't leak to consumers) | — |

## Observability — opt-in across the stack

Every package that touches the network has a `WithOtel()` option that wires the appropriate upstream instrumentation library:

| Package | Underlying integration |
|---|---|
| `http2` | `otelhttp.NewHandler` |
| `grpc` | `otelgrpc.NewServerHandler` |
| `sql` | `otelsqlx.ConnectContext` |
| `redis` | `redisotel.InstrumentTracing` + `InstrumentMetrics` |
| `kafka` | `kotel` hooks via `kgo.WithHooks` |
| `rabbitmq` | manual span injection (no upstream auto-wirer yet) |

All read the global `TracerProvider` / `MeterProvider` set by `otel.Setup`. **Always register `otel.Setup`'s Provider FIRST with `app.App`** so it shuts down LAST — that lets every other component flush its remaining telemetry before exporters close.

For local dev or tests without a collector, pass `otel.Config{Disabled: true}` — an empty `OTelHost` with `Disabled=false` is treated as misconfiguration and returns `ErrEmptyOTelHost`.

## Application lifecycle pattern

A typical app structures its DI as:

```
internal/di/config/        — viper-key → typed core configs (no mapstructure tags; explicit GetString/GetDuration calls)
internal/di/service/
    infrastructure.go      — low-level Resources (DB, Redis, Kafka, RabbitMQ, OTel)
    repositories.go        — depend on infrastructure
    services.go            — depend on repositories
    handlers.go            — depend on services
    router.go              — depend on handlers
    manager.go             — composes all of the above into a Manager
cmd/service/main.go        — ~25 lines: wire Manager to http2/grpc servers, register with app.App, call a.Run()
```

The thin generic `di.NewContainer[Config, *Manager](ctx, config.NewConfig, service.NewServiceManager)` is available for the callback-style bootstrap, but plain function calls in main work just as well. `service.NewServiceManager` follows the `di.ServicesInit[C, S]` signature — it returns `(services S, cleanup func(context.Context) error, err error)` so partial startup failures can release any already-constructed resources before the container returns.

See per-package READMEs for the constructor-by-constructor migration story and the [`di`](./di/README.md) package for the configuration loader.

## Configuration

`core/di.NewConfig` loads viper from (in order, with later sources overriding):

1. Files (`APP_CONFIG_FILE_PATHS`)
2. Static etcd (`APP_CONFIG_ETCD_STATIC_PATHS`)
3. Dynamic etcd with watching (`APP_CONFIG_ETCD_DYNAMIC_PATHS`)
4. Environment variables (`AutomaticEnv` with `"." → "_"` replacer; `http.frontend.port` reads `HTTP_FRONTEND_PORT`)

Required env vars:

- `APP_ENV` — `dev`, `prod`, etc. Used in etcd paths and exposed via `cfg.GetAppEnv()`.
- `APP_SERVICE_NAME` — used in etcd paths and as default OTel `service.name`.

See the [`di` README](./di/README.md) for the full list and details.

## Testing

For unit tests, prefer in-memory fakes (`miniredis`, `sqlmock`). Core's own integration tests live in `internal/integration/` (a separate Go module so testcontainers deps don't leak to consumers).

## Versioning

`core` follows semver.

## Make targets

```bash
make test                # unit tests with race detector
make test-integration    # integration tests in internal/integration (requires Docker)
make lint                # golangci-lint
make bench               # run benchmarks
make gci                 # format with gofumpt + gci
```

## License

MIT — see [LICENSE](./LICENSE).
