# sql/postgres

PostgreSQL-specific config helper for `core/sql`. Builds a libpq-format DSN from typed fields and registers the `lib/pq` driver via blank import.

See [`core/sql`](../README.md) for the database wrapper itself; this subpackage only provides the connection-string helper.

## Quickstart

```go
import (
    "github.com/sergeyslonimsky/core/sql"
    "github.com/sergeyslonimsky/core/sql/postgres"
)

pgCfg := postgres.Config{
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

func (Config) DSN() string     // libpq connection string
func (Config) Driver() string  // "postgres"
```

`SSLMode` defaults to `"disable"` when empty. Other valid values: `"require"`, `"verify-ca"`, `"verify-full"` (libpq conventions).
