package sql

import (
	"context"
	"fmt"

	"github.com/Masterminds/squirrel"
)

// toSQL builds qb and normalizes its placeholders to Postgres' "$1, $2, ..."
// form, regardless of the PlaceholderFormat the caller's builder used.
//
// squirrel's package-level constructors (squirrel.Select, squirrel.Insert,
// ...) default to "?" placeholders, which Postgres rejects — callers who
// forget an explicit .PlaceholderFormat(squirrel.Dollar) get a runtime SQL
// syntax error that unit tests against sqlmock won't catch, since sqlmock
// matches the query string/regex rather than parsing real SQL. Normalizing
// here makes every generic executor Postgres-safe by default.
//
// Idempotent for builders that already used Dollar: ReplacePlaceholders only
// rewrites literal "?" runs, so a query with no "?" left passes through
// unchanged. Note this means a query containing a literal "?" (e.g.
// Postgres's jsonb "?"/"?|"/"?&" existence operators) will be mis-rewritten —
// a pre-existing squirrel/Dollar limitation, not something introduced here.
func toSQL(qb squirrel.Sqlizer) (string, []any, error) {
	query, args, err := qb.ToSql()
	if err != nil {
		return "", nil, fmt.Errorf("build query: %w", err)
	}

	query, err = squirrel.Dollar.ReplacePlaceholders(query)
	if err != nil {
		return "", nil, fmt.Errorf("normalize placeholders: %w", err)
	}

	return query, args, nil
}

func Get[T any](ctx context.Context, q Querier, qb squirrel.Sqlizer) (T, error) {
	var result T

	query, args, err := toSQL(qb)
	if err != nil {
		return result, err
	}

	if err = q.GetContext(ctx, &result, query, args...); err != nil {
		return result, fmt.Errorf("select query: %w", err)
	}

	return result, nil
}

func Select[T any](ctx context.Context, q Querier, qb squirrel.Sqlizer) ([]T, error) {
	result := make([]T, 0)

	query, args, err := toSQL(qb)
	if err != nil {
		return result, err
	}

	if err = q.SelectContext(ctx, &result, query, args...); err != nil {
		return result, fmt.Errorf("select query: %w", err)
	}

	return result, nil
}

func Exec(ctx context.Context, q Querier, qb squirrel.Sqlizer) (int64, error) {
	query, args, err := toSQL(qb)
	if err != nil {
		return 0, err
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
