package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type Querier interface {
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Manager interface {
	GetDB() Querier
	WithTx(ctx context.Context, callback func(q Querier) error) error
}

type DBManager struct {
	db DB
}

func NewManager(db DB) *DBManager {
	return &DBManager{
		db: db,
	}
}

func (m *DBManager) GetDB() Querier {
	return m.db
}

func (m *DBManager) WithTx(ctx context.Context, callback func(q Querier) error) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if err = callback(tx); err != nil {
		return errors.Join(err, tx.Rollback())
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
