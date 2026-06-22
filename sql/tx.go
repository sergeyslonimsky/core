package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type Tx interface {
	Querier
	Commit() error
	Rollback() error
}

type sqlTx struct {
	tx *sqlx.Tx
}

// UnwrapTx returns the underlying *sql.Tx if ctx carries a WithTx-controlled
// transaction (true), or nil and false otherwise.
//
// It exists for libraries that must enlist in the caller's transaction through
// their own *sql.Tx-based API (e.g. a durable job queue's transactional
// enqueue) so that enqueue commits atomically with the surrounding business
// writes. Prefer Manager.GetQuerier for ordinary queries; reach for UnwrapTx
// only when an external API demands the raw *sql.Tx.
func UnwrapTx(ctx context.Context) (*sql.Tx, bool) {
	t, ok := ctx.Value(txKey{}).(*sqlTx)
	if !ok || t == nil || t.tx == nil {
		return nil, false
	}

	return t.tx.Tx, true
}

func newTx(ctx context.Context, db *sqlx.DB, opts *sql.TxOptions) (*sqlTx, error) {
	sqlxTx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("begin txx: %w", err)
	}

	return &sqlTx{tx: sqlxTx}, nil
}

func (t *sqlTx) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	if err := t.tx.GetContext(ctx, dest, query, args...); err != nil {
		return fmt.Errorf("get %s: %w", query, err)
	}

	return nil
}

func (t *sqlTx) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	if err := t.tx.SelectContext(ctx, dest, query, args...); err != nil {
		return fmt.Errorf("select: %w", err)
	}

	return nil
}

func (t *sqlTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	res, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}

	return res, nil
}

func (t *sqlTx) Commit() error {
	if err := t.tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func (t *sqlTx) Rollback() error {
	if err := t.tx.Rollback(); err != nil {
		return fmt.Errorf("rollback: %w", err)
	}

	return nil
}
