// Package sql wraps github.com/jmoiron/sqlx with the lifecycle contract
// used across core: typed Config, functional Options, Shutdown / Healthcheck,
// and an opt-in OpenTelemetry instrumentation pipeline.
//
// The package also exposes a transaction Manager (see manager.go) that
// threads transaction state through context.Context and a small set of
// generic squirrel-friendly executors (see executor.go).
//
// Typical usage:
//
//	db, err := sql.New(ctx, sql.Config{
//	    DriverName: pgCfg.Driver(),
//	    DataSource: pgCfg.DSN(),
//	},
//	    sql.WithOtel(),
//	    sql.WithMaxOpenConns(25),
//	)
//	if err != nil { log.Fatal(err) }
//
//	a := app.New()
//	a.Add(db)   // registers Shutdown + Healthcheck
package sql

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/sergeyslonimsky/core/lifecycle"
)

// Config describes a single SQL database to connect to. Plain fields, no
// struct tags — consumer apps map their viper keys to fields explicitly
// inside their own config.NewConfig().
type Config struct {
	// DriverName is the database/sql driver name (e.g., "postgres", "mysql").
	// Typically obtained from a postgres.Config via .Driver().
	DriverName string

	// DataSource is the driver-specific connection string. Typically
	// obtained from a postgres.Config via .DSN().
	DataSource string
}

// DB wraps a *sqlx.DB and provides Querier semantics, transaction support,
// and lifecycle conformance. Implements lifecycle.Resource and
// lifecycle.Healthchecker.
//
// Replaces the v1 DBConn type. The interface that was also called DB has
// been removed — use *sql.DB directly, or define your own narrow interface
// in consuming code if you need to mock at the database boundary.
type DB struct {
	db           *sqlx.DB
	logger       *slog.Logger
	shutdownOnce sync.Once
	shutdownErr  error
}

// Option configures a new DB.
type Option func(*options)

type options struct {
	logger          *slog.Logger
	otelEnabled     bool
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
}

// WithLogger attaches a *slog.Logger used for lifecycle events. Defaults to
// slog.Default() when omitted.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithOtel enables OpenTelemetry instrumentation by routing the connection
// through otelsqlx, which produces a span per query with attributes for the
// SQL statement, driver name, and result/error status.
//
// Uses the global tracer/meter providers — call otel.Setup and register
// the provider with app.App before constructing the DB so the providers
// are non-noop.
func WithOtel() Option {
	return func(o *options) { o.otelEnabled = true }
}

// WithMaxOpenConns sets the maximum number of open connections to the
// database. Default: database/sql's default (0 = unlimited). Recommended
// to set explicitly in production (typical values: 10-50).
func WithMaxOpenConns(n int) Option {
	return func(o *options) { o.maxOpenConns = n }
}

// WithMaxIdleConns sets the maximum number of idle connections kept in the
// pool. Default: database/sql's default (2). Should be ≤ MaxOpenConns.
func WithMaxIdleConns(n int) Option {
	return func(o *options) { o.maxIdleConns = n }
}

// WithConnMaxLifetime sets the maximum amount of time a connection may be
// reused. Default: 0 (connections reused forever). Use a finite value
// (e.g., 30 minutes) to encourage healthy rotation behind load balancers.
func WithConnMaxLifetime(d time.Duration) Option {
	return func(o *options) { o.connMaxLifetime = d }
}

// WithConnMaxIdleTime sets the maximum amount of time a connection may be
// idle before it is closed. Default: 0 (no idle limit). Use to recycle
// connections that haven't been used recently (e.g., 5 minutes).
func WithConnMaxIdleTime(d time.Duration) Option {
	return func(o *options) { o.connMaxIdleTime = d }
}

// New connects to the database, applies pool tuning options, optionally
// installs OpenTelemetry instrumentation, and verifies the connection.
//
// Replaces the v1 NewDB and NewDBWithOTel constructors — pass WithOtel()
// for the latter behavior.
//
// Returns an error if the connection fails. Callers should treat this as
// fatal during startup.
func New(ctx context.Context, cfg Config, opts ...Option) (*DB, error) {
	o := &options{logger: slog.Default()} //nolint:exhaustruct
	for _, apply := range opts {
		apply(o)
	}

	var (
		sqlxDB *sqlx.DB
		err    error
	)

	if o.otelEnabled {
		sqlxDB, err = otelsqlx.ConnectContext(
			ctx,
			cfg.DriverName,
			cfg.DataSource,
			otelsql.WithAttributes(semconv.DBSystemKey.String(cfg.DriverName)),
		)
	} else {
		sqlxDB, err = sqlx.ConnectContext(ctx, cfg.DriverName, cfg.DataSource)
	}

	if err != nil {
		return nil, fmt.Errorf("connect database: %w", err)
	}

	if o.maxOpenConns > 0 {
		sqlxDB.SetMaxOpenConns(o.maxOpenConns)
	}

	if o.maxIdleConns > 0 {
		sqlxDB.SetMaxIdleConns(o.maxIdleConns)
	}

	if o.connMaxLifetime > 0 {
		sqlxDB.SetConnMaxLifetime(o.connMaxLifetime)
	}

	if o.connMaxIdleTime > 0 {
		sqlxDB.SetConnMaxIdleTime(o.connMaxIdleTime)
	}

	//nolint:exhaustruct // shutdownOnce/shutdownErr have valid zero values
	return &DB{
		db:     sqlxDB,
		logger: o.logger,
	}, nil
}

// NewFromSqlx wraps an existing *sqlx.DB. Useful in tests with sqlmock.
//
// The wrapper does not change the underlying *sqlx.DB's pool settings — if
// pool tuning is needed, configure it on the *sqlx.DB before calling.
func NewFromSqlx(db *sqlx.DB) *DB {
	//nolint:exhaustruct // shutdownOnce/shutdownErr have valid zero values
	return &DB{
		db:     db,
		logger: slog.Default(),
	}
}

// Unwrap returns the underlying *sqlx.DB. Use for operations not covered by
// Querier (raw NamedExec, batch operations, custom row scans, etc).
//
// Owned by the wrapper — do not Close directly; always go through Shutdown.
func (db *DB) Unwrap() *sqlx.DB {
	return db.db
}

// GetContext, SelectContext, ExecContext implement Querier — see manager.go.

// GetContext executes the query and scans the single result row into dest.
// See sqlx.DB.GetContext for semantics.
func (db *DB) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	if err := db.db.GetContext(ctx, dest, query, args...); err != nil {
		return fmt.Errorf("sqlx.GetContext: %w", err)
	}

	return nil
}

// SelectContext executes the query and scans every result row into the
// dest slice. See sqlx.DB.SelectContext for semantics.
func (db *DB) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	if err := db.db.SelectContext(ctx, dest, query, args...); err != nil {
		return fmt.Errorf("sqlx.SelectContext: %w", err)
	}

	return nil
}

// ExecContext executes a non-result query (INSERT/UPDATE/DELETE).
func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (stdsql.Result, error) {
	res, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlx.ExecContext: %w", err)
	}

	return res, nil
}

// BeginTx starts a transaction. The returned Tx implements Querier so it
// can be passed to the generic executors (Get, Select, Exec) in this
// package.
func (db *DB) BeginTx(ctx context.Context, opts *stdsql.TxOptions) (Tx, error) {
	tx, err := newTx(ctx, db.db, opts)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}

	return tx, nil
}

// Shutdown closes the underlying *sqlx.DB, releasing the connection pool.
// Implements lifecycle.Resource.
//
// Idempotent and concurrent-safe: the close runs exactly once; every
// subsequent call returns the same result.
//
// The ctx parameter is currently ignored — sqlx/database/sql's Close is
// synchronous and not context-aware. The signature matches lifecycle.Resource
// for uniform integration with app.App.
func (db *DB) Shutdown(_ context.Context) error {
	db.shutdownOnce.Do(func() {
		if err := db.db.Close(); err != nil {
			db.shutdownErr = fmt.Errorf("close sqlx db: %w", err)
		}
	})

	return db.shutdownErr
}

// Healthcheck pings the database. Implements lifecycle.Healthchecker.
//
// Cheap (single PING) and respects ctx via PingContext.
func (db *DB) Healthcheck(ctx context.Context) error {
	if err := db.db.PingContext(ctx); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}

	return nil
}

// Compile-time assertions.
var (
	_ lifecycle.Resource      = (*DB)(nil)
	_ lifecycle.Healthchecker = (*DB)(nil)
	_ Querier                 = (*DB)(nil)
)
