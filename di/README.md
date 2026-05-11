# di

Configuration loader for services built on core. Loads from yaml files,
env vars, and etcd v3 (static + dynamic with native push-Watch). Generic
typed API. Lock-free reads via atomic snapshot.

## Quick start

```go
cfg, err := di.NewConfig(ctx)
if err != nil {
    log.Fatal(err)
}

port := di.Get[string](cfg, "http.public.port")
timeout := di.GetOrDefault[time.Duration](cfg, "http.timeout", 30*time.Second)
currentRPS := di.Live[int](cfg, "limits.rps") // re-read on each call() for runtime tunables
```

## Sources and priority

```
env vars  >  etcd dynamic  >  etcd static  >  yaml file  >  defaults
```

Any key can be overridden via env. If env is absent, the highest-priority
source wins: dynamic > static > file > default.

| Layer         | Who manages       | When changes                              |
|---------------|-------------------|-------------------------------------------|
| env           | operator (deploy) | process restart                           |
| etcd dynamic  | operator (live)   | runtime — pushed via etcd Watch           |
| etcd static   | operator (deploy) | process restart                           |
| yaml file     | repo / image      | image rebuild                             |
| defaults      | core / app code   | code change                               |

## Bootstrap configuration

The following keys are read from **env only** (they decide where other
configuration comes from):

- `APP_ENV` (default: `dev`) — environment label, used in the etcd path
  prefix.
- `APP_SERVICE_NAME` — required when etcd is configured. Used in the
  etcd path prefix.
- `APP_CONFIG_FILE_PATHS` — comma-separated list of yaml file paths or
  directories (directories load `config.yaml`).
- `APP_CONFIG_FILE_PATH` — legacy single path.
- `APP_CONFIG_ETCD_ENDPOINT` — etcd address (required when any etcd
  path is set).
- `APP_CONFIG_ETCD_STATIC_PATHS` — comma-separated list of paths read
  once at startup.
- `APP_CONFIG_ETCD_DYNAMIC_PATHS` — comma-separated list of paths
  watched for live updates.
- `APP_CONFIG_ETCD_PATH` — legacy single dynamic path.
- `APP_CONFIG_ETCD_REQUEST_TIMEOUT` — Go-duration string ("5s",
  "500ms", "1m") bounding every etcd Get during static load and
  watcher resync. Default `5s`. A malformed value fails startup with
  `ErrInvalidEtcdRequestTimeout`.

Etcd full key = `{APP_ENV}/{APP_SERVICE_NAME}/{path}`.

## Public API

```go
func Get[T ValueType](cfg *Config, key string) T
func GetOrDefault[T ValueType](cfg *Config, key string, def T) T
func Live[T ValueType](cfg *Config, key string) func() T
func LiveOrDefault[T ValueType](cfg *Config, key string, def T) func() T
```

- `Get` — read once.
- `GetOrDefault` — read once with a fallback.
- `Live` — closure that re-reads on every call. Use for hot-reloadable
  values (rate limits, feature flags) that may change at runtime via
  etcd dynamic.
- `LiveOrDefault` — `Live` with a fallback.

`ValueType` constraint:
`string | int | int64 | bool | float64 | time.Duration | []string`.

Methods on `*Config`: `GetAppEnv()`, `GetServiceName()` — captured from
env at startup, immutable for the process lifetime.

## Defaults semantics

`GetOrDefault` returns the default **only when the key is absent or the
stored value can't be coerced to T**. It does NOT return the default
when the value is zero (`0`, `""`, empty slice). If you want
zero-as-absent semantics, wrap manually:

```go
v := di.Get[int](cfg, "k")
if v <= 0 {
    v = fallback
}
```

## Lifecycle

`NewConfig(ctx)` is **fail-fast**: errors when etcd is configured but
unreachable, when paths can't be parsed, or when `APP_SERVICE_NAME` is
empty with etcd configured. After successful start, etcd watch errors
are logged but do not propagate — operators can fix etcd without
restarting the service.

The dynamic watcher runs until `ctx` is cancelled, then closes the etcd
client. Pass a long-lived `ctx` (typically the app's main context).

## Container helper

`Container[C, S]` and `NewContainer` provide a two-step bootstrap (init
config → init services). `ServicesInit` may return a `Rollback` invoked
**only when initialization fails** — it releases partially-constructed
resources. Runtime teardown of healthy resources is the job of
`lifecycle.Resource` registered with `app.App`; on success the
`Rollback` is discarded. See `container.go` godoc for details.

## Migration from the typed-method API

| Was                                 | Now                                        |
|-------------------------------------|--------------------------------------------|
| `cfg.GetString("k")`                | `di.Get[string](cfg, "k")`                 |
| `cfg.GetStringOrDefault("k", "x")`  | `di.GetOrDefault[string](cfg, "k", "x")`   |
| `cfg.GetInt("k")`                   | `di.Get[int](cfg, "k")`                    |
| `cfg.GetBool("k")`                  | `di.Get[bool](cfg, "k")`                   |
| `cfg.GetDuration("k")`              | `di.Get[time.Duration](cfg, "k")`          |
| `cfg.GetStringSlice("k")`           | `di.Get[[]string](cfg, "k")`               |
| `cfg.WatchString("k")`              | `di.Live[string](cfg, "k")`                |
| `cfg.WatchInt("k")`                 | `di.Live[int](cfg, "k")`                   |
| `cfg.WatchBool("k")`                | `di.Live[bool](cfg, "k")`                  |
| `cfg.WatchDuration("k")`            | `di.Live[time.Duration](cfg, "k")`         |
| `cfg.WatchStringSlice("k")`         | `di.Live[[]string](cfg, "k")`              |
| `cfg.GetStorage().GetString("k")`   | `di.Get[string](cfg, "k")` (no direct storage access anymore) |

For consumers that embed the config (`type AppConfig struct { *di.Config; ... }`),
use `di.Get[T](appCfg.Config, "k")` — explicit access to the embedded
pointer.
