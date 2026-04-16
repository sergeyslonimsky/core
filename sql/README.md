# sql

Wrapper around `github.com/jmoiron/sqlx` with the lifecycle contract: typed `Config`, functional `Options`, `Shutdown(ctx)`, `Healthcheck(ctx)`, opt-in OpenTelemetry instrumentation, and a transaction `Manager` that threads tx state through `context.Context`.

## When to use

Any service that needs an SQL database. Currently `sql/postgres` is the only provided driver helper, but `sql.Config{DriverName, DataSource}` works with any database/sql-compatible driver — bring your own.

## Quickstart

```go
package main

import (
    "context"
    "log"

    "github.com/sergeyslonimsky/core/app"
    "github.com/sergeyslonimsky/core/sql"
    "github.com/sergeyslonimsky/core/sql/postgres"
)

func main() {
    ctx := context.Background()

    pgCfg := postgres.Config{
        Host: "localhost", Port: "5432",
        User: "app", Password: "secret", Name: "myapp",
    }

    db, err := sql.New(ctx, sql.Config{
        DriverName: pgCfg.Driver(),
        DataSource: pgCfg.DSN(),
    },
        sql.WithOtel(),
        sql.WithMaxOpenConns(25),
        sql.WithConnMaxLifetime(30 * time.Minute),
    )
    if err != nil {
        log.Fatal(err)
    }

    a := app.New()
    a.Add(db)  // registers Shutdown + Healthcheck
}
```

## Configuration

```go
type Config struct {
    DriverName string  // e.g., "postgres"
    DataSource string  // driver-specific DSN
}
```

For PostgreSQL, use the `postgres` subpackage:

```go
type postgres.Config struct {
    Host, Port, User, Password, Name, SSLMode string
}

func (c Config) DSN() string     // libpq-format connection string
func (c Config) Driver() string  // "postgres"
```

## Options

- `WithLogger(*slog.Logger)` — used for lifecycle events. Default: `slog.Default()`.
- `WithOtel()` — route through `otelsqlx.ConnectContext` for per-query tracing.
- `WithMaxOpenConns(n int)` — pool's max open connections (default: unlimited).
- `WithMaxIdleConns(n int)` — pool's max idle connections (default: 2).
- `WithConnMaxLifetime(d time.Duration)` — recycle connections after this age.
- `WithConnMaxIdleTime(d time.Duration)` — close connections idle for this long.

## Observability (`WithOtel`)

Replaces `sqlx.ConnectContext` with `otelsqlx.ConnectContext` from `github.com/uptrace/opentelemetry-go-extra/otelsqlx`. Every `*Context` query produces a span with the SQL statement (sanitized), driver name, and result/error.

Uses the global tracer/meter providers — call `otel.Setup` before `sql.New` and register the provider with `app.App`.

## Lifecycle

`*sql.DB` implements `lifecycle.Resource` + `lifecycle.Healthchecker`. Register with `app.App.Add(db)`.

```go
a.Add(otelProvider)  // first → shuts down last
a.Add(db)            // before redis / kafka producers
a.Add(httpServer)    // last → shuts down first
```

`Shutdown` calls `*sqlx.DB.Close()`. `Healthcheck` issues `PingContext`.

## Transactions

```go
manager := sql.NewManager(db)

err := manager.WithTx(ctx, func(txCtx context.Context) error {
    q := manager.GetQuerier(txCtx)
    // q is the active *Tx — pass to repositories so they participate in the same transaction
    return userRepo.Update(txCtx, user)
})
```

Repositories should always call `manager.GetQuerier(ctx)` — outside `WithTx` it returns `*sql.DB`, inside it returns the active `Tx`. This lets repositories be unaware of whether they are inside a transaction.

Panic safety: if the callback panics, `WithTx` rolls the transaction back before the panic re-propagates. Callers don't need a manual `defer recover`.

## Generic executors

```go
import "github.com/Masterminds/squirrel"

qb := squirrel.Select("id", "name").From("users").Where(squirrel.Eq{"id": userID})
user, err := sql.Get[User](ctx, manager.GetQuerier(ctx), qb)
```

Available: `Get[T]`, `Select[T]`, `Exec` — all accept any `Querier`.

## Extending

```go
sx := db.Unwrap()
// raw *sqlx.DB for NamedExec, batch operations, custom row scans, etc
```

Owned by the wrapper — never call `Close` directly; always go through `Shutdown`.

## Testing

```go
import "github.com/DATA-DOG/go-sqlmock"

mockDB, mock, _ := sqlmock.New()
db := sql.NewFromSqlx(sqlx.NewDb(mockDB, "postgres"))
manager := sql.NewManager(db)
// ... set up mock expectations and run repository code
```


## See also

- [`core/app`](../app/README.md) — register the DB here.
- [`core/lifecycle`](../lifecycle/README.md) — the `Resource` / `Healthchecker` contracts.
- [`core/otel`](../otel/README.md) — bootstrap that powers `WithOtel`.
