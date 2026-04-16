// Package postgres provides a typed Config for PostgreSQL connections that
// produces a DSN string consumable by core/sql.New.
//
// Importing this package also registers the lib/pq driver via blank import,
// so consuming code does not need a separate driver import.
package postgres

import (
	"fmt"

	_ "github.com/lib/pq" // register the postgres driver
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

// DSN returns a libpq-format connection string. Use as the DataSource for
// sql.Config:
//
//	pgCfg := postgres.Config{...}
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

// Driver returns the database/sql driver name ("postgres"). Convenience for
// passing into sql.Config.DriverName.
func (c Config) Driver() string {
	return "postgres"
}

func (c Config) sslMode() string {
	if c.SSLMode == "" {
		return "disable"
	}

	return c.SSLMode
}
