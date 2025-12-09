package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type txKeyType string

const txContextKey txKeyType = "core.sql.tx"

type Querier interface {
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Manager interface {
	WithTx(ctx context.Context, callback func(context.Context) error) error
	GetQuerier(ctx context.Context) Querier
}

type DBManager struct {
	db DB
}

func NewManager(db DB) *DBManager {
	return &DBManager{
		db: db,
	}
}

func (m *DBManager) GetQuerier(ctx context.Context) Querier {
	tx, ok := ctx.Value(txContextKey).(Querier)
	if !ok {
		return m.db
	}

	return tx
}

func (m *DBManager) WithTx(ctx context.Context, callback func(context.Context) error) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	txCtx, cancel := context.WithCancel(context.WithValue(ctx, txContextKey, tx))

	defer cancel()

	if err = callback(txCtx); err != nil {
		return errors.Join(err, tx.Rollback())
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
