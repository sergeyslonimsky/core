package sql

import (
	"context"
	"fmt"

	"github.com/Masterminds/squirrel"
)

func Get[T any](ctx context.Context, q Querier, qb squirrel.Sqlizer) (T, error) {
	var result T

	query, args, err := qb.ToSql()
	if err != nil {
		return result, fmt.Errorf("build query: %w", err)
	}

	if err = q.GetContext(ctx, &result, query, args...); err != nil {
		return result, fmt.Errorf("select query: %w", err)
	}

	return result, nil
}

func Select[T any](ctx context.Context, q Querier, qb squirrel.Sqlizer) ([]T, error) {
	result := make([]T, 0)

	query, args, err := qb.ToSql()
	if err != nil {
		return result, fmt.Errorf("build query: %w", err)
	}

	if err = q.SelectContext(ctx, &result, query, args...); err != nil {
		return result, fmt.Errorf("select query: %w", err)
	}

	return result, nil
}

func Exec(ctx context.Context, q Querier, qb squirrel.Sqlizer) (int64, error) {
	query, args, err := qb.ToSql()
	if err != nil {
		return 0, fmt.Errorf("build query: %w", err)
	}

	result, err := q.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("exec query: %w", err)
	}

	rowsCount, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}

	return rowsCount, nil
}
