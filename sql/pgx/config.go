// Package pgx provides a typed Config for PostgreSQL connections that produces
// a DSN string consumable by core/sql.New, registering the pgx stdlib driver.
//
// Importing this package registers github.com/jackc/pgx/v5/stdlib via blank
// import under the database/sql driver name "pgx", so consuming code does not
// need a separate driver import. pgx is the standard PostgreSQL driver for
// core consumers — chosen for richer Postgres type support (NUMERIC, arrays),
// proper context-cancellation of in-flight queries, and libraries that assume
// pgx (such as River's database/sql driver).
package pgx

import (
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // register the "pgx" database/sql driver
)

// Config describes a PostgreSQL connection. Plain fields, no struct tags —
// consumer apps map their viper keys to fields explicitly inside their own
// config.NewConfig().
type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string

	// SSLMode follows libpq conventions: "disable", "require",
	// "verify-ca", "verify-full". Defaults to "disable" when empty.
	SSLMode string
}

// DSN returns a libpq-format connection string. The pgx stdlib driver parses
// this keyword/value form. Use as the DataSource for sql.Config:
//
//	pgCfg := pgx.Config{...}
//	db, err := sql.New(ctx, sql.Config{
//	    DriverName: pgCfg.Driver(),
//	    DataSource: pgCfg.DSN(),
//	}, sql.WithOtel())
func (c Config) DSN() string {
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%s sslmode=%s",
		c.User, c.Password, c.Name, c.Host, c.Port, c.sslMode(),
	)
}

// Driver returns the database/sql driver name ("pgx"). Convenience for passing
// into sql.Config.DriverName.
func (c Config) Driver() string {
	return "pgx"
}

func (c Config) sslMode() string {
	if c.SSLMode == "" {
		return "disable"
	}

	return c.SSLMode
}
