package sql

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
)

// txKey is the private context key used to thread an active transaction
// through callback chains inside WithTx. Using a struct{} type (rather
// than a string) removes any collision surface with keys defined in other
// packages and keeps the value zero-sized.
type txKey struct{}

// Querier is the minimal query interface implemented by both *DB and Tx.
// Generic executors (Get, Select, Exec in executor.go) accept a Querier so
// they work uniformly inside or outside a transaction.
type Querier interface {
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	ExecContext(ctx context.Context, query string, args ...any) (stdsql.Result, error)
}

// Manager is the transaction orchestration interface used by repositories
// that want to participate in WithTx-controlled transactions without owning
// the transaction lifetime themselves.
//
// Repositories should call manager.GetQuerier(ctx) on every query — the
// returned Querier is either the active transaction (if WithTx wrapped
// the call chain) or the underlying *DB.
type Manager interface {
	// WithTx runs callback inside a single transaction. The callback's ctx
	// carries the transaction; subsequent GetQuerier(ctx) calls return the
	// transaction's Querier. On error, the transaction is rolled back and
	// the callback's error is returned. On success, the transaction is
	// committed.
	WithTx(ctx context.Context, callback func(context.Context) error) error

	// GetQuerier returns the active transaction's Querier if ctx was
	// produced by WithTx; otherwise returns the manager's underlying *DB.
	GetQuerier(ctx context.Context) Querier
}

// DBManager is the default Manager implementation backed by a *DB.
type DBManager struct {
	db *DB
}

// NewManager wraps a *DB in a transaction Manager.
func NewManager(db *DB) *DBManager {
	return &DBManager{db: db}
}

// GetQuerier — see Manager.GetQuerier.
func (m *DBManager) GetQuerier(ctx context.Context) Querier {
	tx, ok := ctx.Value(txKey{}).(Querier)
	if !ok {
		return m.db
	}

	return tx
}

// WithTx — see Manager.WithTx.
//
// Panic safety: if callback panics, the transaction is rolled back before
// the panic propagates. This prevents leaked transactions that would
// otherwise linger until the database's server-side idle timeout.
func (m *DBManager) WithTx(ctx context.Context, callback func(context.Context) error) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	txCtx := context.WithValue(ctx, txKey{}, tx)

	defer func() {
		if p := recover(); p != nil {
			// Best-effort rollback on panic, then re-panic to preserve the
			// original crash. Rollback error is intentionally discarded:
			// a panic is already a hard failure and the tx will be aborted
			// by the server on connection close anyway.
			_ = tx.Rollback()

			panic(p)
		}
	}()

	if err = callback(txCtx); err != nil {
		return errors.Join(err, tx.Rollback())
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// Compile-time check.
var _ Manager = (*DBManager)(nil)
