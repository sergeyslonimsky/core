package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type DB interface {
	Querier
	GetSQLX() *sqlx.DB
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
}

type DBConn struct {
	db *sqlx.DB
}

type config interface {
	GetDriverName() string
	GetDataSource() string
}

func NewDB(ctx context.Context, config config) (*DBConn, error) {
	sqlxDB, err := sqlx.ConnectContext(ctx, config.GetDriverName(), config.GetDataSource())
	if err != nil {
		return nil, fmt.Errorf("failed to connect database: %w", err)
	}

	return &DBConn{db: sqlxDB}, nil
}

func NewDBFromSqlx(db *sqlx.DB) (*DBConn, error) {
	return &DBConn{db: db}, nil
}

func (db *DBConn) GetSQLX() *sqlx.DB {
	return db.db
}

func (db *DBConn) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	if err := db.db.GetContext(ctx, dest, query, args...); err != nil {
		return fmt.Errorf("sqlx.GetContext: %w", err)
	}

	return nil
}

func (db *DBConn) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	if err := db.db.SelectContext(ctx, dest, query, args...); err != nil {
		return fmt.Errorf("select sqlx: %w", err)
	}

	return nil
}

func (db *DBConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	res, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("exec sqlx: %w", err)
	}

	return res, nil
}

func (db *DBConn) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	tx, err := newTx(ctx, db.db, opts)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}

	return tx, nil
}
