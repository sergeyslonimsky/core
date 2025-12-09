package sql_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	csql "github.com/sergeyslonimsky/core/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagerWithTx(t *testing.T) {
	t.Parallel()

	type ResultStruct struct {
		ID   string `db:"id"`
		Name string `db:"name"`
	}

	tests := []struct {
		name     string
		mockFunc func(mock sqlmock.Sqlmock)
		err      error
	}{
		{
			name: "successful query with transaction",
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery("SELECT id, name FROM test_table WHERE id = ?").
					WithArgs("test").
					WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow("test", "testName"))
				mock.ExpectCommit()
				mock.ExpectClose()
			},
		},
		{
			name: "error in query with transaction",
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery("SELECT id, name FROM test_table WHERE id = ?").
					WithArgs("test").
					WillReturnError(sql.ErrNoRows)
				mock.ExpectRollback()
				mock.ExpectClose()
			},
			err: sql.ErrNoRows,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			manager, sqlMock := setupTestDB(t)
			tt.mockFunc(sqlMock)

			ctx := t.Context()

			err := manager.WithTx(ctx, func(tCtx context.Context) error {
				q := manager.GetQuerier(tCtx)

				_, ok := q.(csql.Tx)
				assert.True(t, ok)

				qb := squirrel.Select("id", "name").From("test_table").Where(squirrel.Eq{"id": "test"})

				_, err := csql.Get[ResultStruct](tCtx, q, qb)
				if err != nil {
					return err
				}

				return nil
			})

			if tt.err != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			_, ok := manager.GetQuerier(ctx).(csql.Tx)
			assert.False(t, ok)
		})
	}
}

func TestManagerQuerier(t *testing.T) {
	t.Parallel()

	type ResultStruct struct {
		ID   string `db:"id"`
		Name string `db:"name"`
	}

	tests := []struct {
		name     string
		mockFunc func(mock sqlmock.Sqlmock)
		err      error
	}{
		{
			name: "successful query",
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT id, name FROM test_table WHERE id = ?").
					WithArgs("test").
					WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow("test", "testName"))
				mock.ExpectClose()
			},
		},
		{
			name: "error in query",
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT id, name FROM test_table WHERE id = ?").
					WithArgs("test").
					WillReturnError(sql.ErrNoRows)
				mock.ExpectClose()
			},
			err: sql.ErrNoRows,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			manager, sqlMock := setupTestDB(t)
			tt.mockFunc(sqlMock)

			ctx := t.Context()

			q := manager.GetQuerier(ctx)

			_, ok := q.(csql.Tx)
			assert.False(t, ok)

			qb := squirrel.Select("id", "name").From("test_table").Where(squirrel.Eq{"id": "test"})

			_, err := csql.Get[ResultStruct](ctx, q, qb)

			if tt.err != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func setupTestDB(t *testing.T) (*csql.DBManager, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	dbConn, err := csql.NewDBFromSqlx(sqlx.NewDb(db, "postgres"))
	require.NoError(t, err)

	return csql.NewManager(dbConn), mock
}
