# sql/pgx

PostgreSQL-specific config helper for `core/sql`, backed by the **pgx stdlib**
driver. Builds a libpq-format DSN from typed fields and registers the `pgx`
driver via blank import.

Drop-in alternative to [`core/sql/postgres`](../postgres) (lib/pq). Pick a driver
by importing exactly one of these subpackages — this package never imports
`core/sql/postgres`, so it does not pull in `lib/pq`. Use `sql/pgx` when you need
pgx specifically: richer Postgres type handling, or libraries that assume pgx
(e.g. [River](https://riverqueue.com)'s `database/sql` driver).

See [`core/sql`](../README.md) for the database wrapper itself; this subpackage
only provides the connection-string helper.

## Quickstart

```go
import (
    "github.com/sergeyslonimsky/core/sql"
    "github.com/sergeyslonimsky/core/sql/pgx"
)

pgCfg := pgx.Config{
    Host:     "localhost",
    Port:     "5432",
    User:     "app",
    Password: "secret",
    Name:     "myapp",
    SSLMode:  "disable",
}

db, err := sql.New(ctx, sql.Config{
    DriverName: pgCfg.Driver(),
    DataSource: pgCfg.DSN(),
}, sql.WithOtel())
```

## API

```go
type Config struct {
    Host, Port, User, Password, Name, SSLMode string
}

func (Config) DSN() string     // libpq connection string (pgx parses keyword form)
func (Config) Driver() string  // "pgx"
```

`Config` is field-compatible with `core/sql/postgres.Config`, so switching
drivers is a one-line import/type change. `SSLMode` defaults to `"disable"` when
empty; other valid values: `"require"`, `"verify-ca"`, `"verify-full"` (libpq
conventions).
