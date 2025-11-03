package sql

import (
	"context"
	"fmt"

	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func NewDBWithOTel(ctx context.Context, cfg config) (*DBConn, error) {
	db, err := otelsqlx.ConnectContext(
		ctx,
		cfg.GetDriverName(),
		cfg.GetDataSource(),
		otelsql.WithAttributes(semconv.DBSystemKey.String(cfg.GetDriverName())),
	)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	return &DBConn{db: db}, nil
}
