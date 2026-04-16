# redis

Wrapper around `github.com/redis/go-redis/v9` that adds a lifecycle contract: `Shutdown(ctx)`, `Healthcheck(ctx)`, opt-in OpenTelemetry instrumentation, and an `Unwrap()` accessor for power users.

Includes `Shutdown(ctx)` for proper connection pool cleanup and `Healthcheck(ctx)` for readiness probes.

## When to use

Any service that needs Redis: caching, session storage, rate limiting, pubsub fan-out, distributed locks. The convenience methods cover Get/Set/Del; for everything else, use `Unwrap()`.

## Quickstart

```go
package main

import (
    "context"
    "log"

    "github.com/sergeyslonimsky/core/app"
    "github.com/sergeyslonimsky/core/redis"
)

func main() {
    ctx := context.Background()

    client, err := redis.New(ctx, redis.Config{
        Host: "localhost",
        Port: "6379",
    },
        redis.WithOtel(),
        redis.WithPoolSize(50),
    )
    if err != nil {
        log.Fatal(err)
    }

    a := app.New()
    a.Add(client)  // registers Shutdown + Healthcheck

    // ... use client.Get/Set/Del or client.Unwrap() inside your handlers
}
```

## Configuration

```go
type Config struct {
    Host     string  // Required
    Port     string  // Required
    Password string  // Optional
    DB       int     // Optional, default 0
}
```

App-level mapping example:

```go
redis.Config{
    Host:     raw.GetString("redis.host"),
    Port:     raw.GetString("redis.port"),
    Password: raw.GetString("redis.password"),
    DB:       raw.GetInt("redis.db"),
}
```

## Options

- `WithLogger(*slog.Logger)` — used for lifecycle events. Default: `slog.Default()`.
- `WithPoolSize(n int)` — connection pool size. Default: go-redis chooses `10*runtime.NumCPU()`.
- `WithReadTimeout(time.Duration)` — per-command read timeout. Default: 3s (go-redis).
- `WithWriteTimeout(time.Duration)` — per-command write timeout. Default: same as ReadTimeout.
- `WithOtel()` — instrument every command with OpenTelemetry traces and metrics via redisotel.

## Observability (`WithOtel`)

Calls `redisotel.InstrumentTracing(client)` and `redisotel.InstrumentMetrics(client)` from `github.com/redis/go-redis/extra/redisotel/v9`. Uses the global tracer/meter providers — call `otel.Setup` and register the provider with `app.App` before constructing the client.

One span per command is created with the command name, key (where applicable), and result/error. Pool metrics (active connections, hits, misses) are reported via the meter provider.

## Lifecycle

`*Client` implements `lifecycle.Resource` + `lifecycle.Healthchecker`. Register with `app.App.Add(client)`. Recommended ordering inside the resource list:

```go
a.Add(otelProvider)  // first → shuts down last
a.Add(db)            // before redis
a.Add(redisClient)   // before HTTP/gRPC servers
a.Add(httpServer)    // last → shuts down first
```

`Shutdown` calls the underlying `*redis.Client.Close()`. The `ctx` parameter is ignored (go-redis's Close is synchronous).

`Healthcheck` issues a single `PING` and returns the error if the command fails or the deadline expires.

## Extending

```go
rc := client.Unwrap()
pipe := rc.Pipeline()
// ... pipelines, scripts, pubsub, transactions
```

The returned `*redis.Client` is owned by the wrapper. Don't call `Close()` on it directly — always go through `Shutdown`.

## Testing

For unit tests, use an in-memory fake like `miniredis`:

```go
import "github.com/alicebob/miniredis/v2"

mr := miniredis.RunT(t)
client, err := redis.New(ctx, redis.Config{
    Host: mr.Host(),
    Port: mr.Port(),
})
```

## See also

- [`core/app`](../app/README.md) — register the client here.
- [`core/lifecycle`](../lifecycle/README.md) — the `Resource` / `Healthchecker` contracts.
- [`core/otel`](../otel/README.md) — bootstrap that powers `WithOtel`.
