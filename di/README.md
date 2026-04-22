# di

viper-backed configuration loader and a thin generic `Container` wrapper. Together they handle:

- Loading config from file, static etcd, and dynamic (watched) etcd.
- Environment-variable overrides with automatic `HTTP_FRONTEND_PORT ↔ http.frontend.port` key conversion.
- A two-step bootstrap (`initConfig → initServices`) via `NewContainer`.

The package does NOT handle cleanup — every component lifecycle flows through `core/app` via `lifecycle.Resource`.

## When to use

- Use `di.NewConfig(ctx)` to build a viper-backed `*Config` for any service.
- Use `di.NewContainer(...)` if you like the callback-style bootstrap (`backend-master` uses this). Otherwise call `config.NewConfig` and `service.NewManager` directly in `main.go` — the container is optional sugar.

## Quickstart

### Config only

```go
cfg, err := di.NewConfig(ctx)
if err != nil {
    log.Fatal(err)
}

port := cfg.GetStringOrDefault("http.frontend.port", "8080")
writeTimeout := cfg.GetDuration("http.frontend.write_timeout")
```

### Container

```go
import (
    "github.com/sergeyslonimsky/core/app"
    "github.com/sergeyslonimsky/core/di"

    "myapp/internal/di/config"
    "myapp/internal/di/service"
)

func main() {
    container, err := di.NewContainer(
        context.Background(),
        config.NewConfig,
        service.NewServiceManager,
    )
    if err != nil {
        log.Fatal(err)
    }

    cfg, svc := container.Config, container.Services

    a := app.New()
    svc.RegisterResources(a)
    a.Add(svc.HTTPServer, svc.GRPCServer)
    log.Fatal(a.Run())
}
```

## Config sources

Loaded in order, with later sources overriding earlier ones for the same keys:

1. **File** — `app.config.file.paths` (comma-separated) or `app.config.file.path`. YAML by default.
2. **Static etcd** — `app.config.etcd.static.paths` + `app.config.etcd.endpoint`. Loaded once, no watching.
3. **Dynamic etcd** — `app.config.etcd.dynamic.paths`. Subscribed via native `clientv3.Watch` on every path (not just the first) until context cancellation. Only PUT events are applied — DELETE is ignored. On watch disconnect (compaction, network drop, server restart), each watcher reconnects with exponential backoff (1s → 30s cap) and re-syncs the key via Get so updates missed during the outage are caught up.
4. **Environment variables** — always override. `AutomaticEnv` with `"." → "_"` replacer. So `http.frontend.port` reads `HTTP_FRONTEND_PORT`.

## API

### Loader

```go
func NewConfig(ctx context.Context) (*Config, error)
```

### Getters

```go
func (*Config) GetString(key string) string
func (*Config) GetStringOrDefault(key, defaultValue string) string
func (*Config) GetStringSlice(key string) []string
func (*Config) GetInt(key string) int
func (*Config) GetBool(key string) bool
func (*Config) GetDuration(key string) time.Duration
func (*Config) GetAppEnv() string
func (*Config) GetServiceName() string
func (*Config) GetStorage() *viper.Viper   // escape hatch — prefer the typed getters
```

### Watchers (for dynamic etcd values)

Each `Watch*` returns a closure that re-reads the key every call, so you get live-updated values without re-wiring:

```go
getConfirmLink := cfg.WatchString("frontend.confirm_email_link")
// later: getConfirmLink() returns the current value at that moment
```

### Container

```go
type Container[C, S any] struct {
    Config   C
    Services S
}

// ServicesInit builds the Services graph and MAY return a cleanup closure
// that releases any already-constructed resources if err is non-nil.
// NewContainer invokes the cleanup before returning the init error.
type ServicesInit[C, S any] func(
    ctx context.Context,
    cfg C,
) (services S, cleanup func(context.Context) error, err error)

func NewContainer[C, S any](
    ctx context.Context,
    initConfig func(context.Context) (C, error),
    initServices ServicesInit[C, S],
) (*Container[C, S], error)
```

The cleanup closure lets partial-failure startup paths release resources (open DB pools, connected redis clients, started otel providers). Return `nil` for the cleanup when the initializer has nothing to release. On success, NewContainer discards the cleanup — runtime teardown is driven by `app.App` via `lifecycle.Resource`.

```go
func NewServiceManager(ctx context.Context, cfg Config) (*Manager, func(context.Context) error, error) {
    db, err := sql.New(ctx, cfg.DB)
    if err != nil {
        return nil, nil, fmt.Errorf("init db: %w", err)
    }

    cleanup := func(ctx context.Context) error { return db.Shutdown(ctx) }

    redisClient, err := redis.New(ctx, cfg.Redis)
    if err != nil {
        return nil, cleanup, fmt.Errorf("init redis: %w", err)
    }

    cleanup = func(ctx context.Context) error {
        return errors.Join(redisClient.Shutdown(ctx), db.Shutdown(ctx))
    }

    return &Manager{DB: db, Redis: redisClient}, nil, nil // success path: cleanup discarded
}
```

## Why no `Bind[T]` or `mapstructure` tags?

Core-package `Config` structs (e.g., `http2.Config`, `redis.Config`) are plain structs with no reflection metadata. Consumer apps define their own app-level `config.Config` that composes core configs and maps viper keys to fields explicitly:

```go
// app/internal/di/config/config.go
type Config struct {
    Frontend http2.Config
    Redis    redis.Config
}

func NewConfig(ctx context.Context) (Config, error) {
    raw, err := di.NewConfig(ctx)
    if err != nil {
        return Config{}, err
    }
    return Config{
        Frontend: http2.Config{
            Port:         raw.GetStringOrDefault("http.frontend.port", "8080"),
            ReadTimeout:  raw.GetDuration("http.frontend.read_timeout"),
            WriteTimeout: raw.GetDuration("http.frontend.write_timeout"),
        },
        Redis: redis.Config{
            Host: raw.GetString("redis.host"),
            Port: raw.GetString("redis.port"),
        },
    }, nil
}
```

Rationale: core packages stay usable without viper (tests can construct `Config{}` literals directly), and the viper-key → field mapping is visible and reviewable in the app rather than hidden behind tags.

## See also

- [`core/app`](../app/README.md) — where Resources/Runners from the Services graph get registered.
- [`core/lifecycle`](../lifecycle/README.md) — the cleanup contract that replaces the old `ServiceContainer.AddOnClose`.
