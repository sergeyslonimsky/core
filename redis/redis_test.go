package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sergeyslonimsky/core/redis"
)

func TestNew_DefaultConnMaxLifetime(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	client, err := redis.New(t.Context(), redis.Config{Host: mr.Host(), Port: mr.Port()})
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Shutdown(context.WithoutCancel(t.Context())) })

	assert.Equal(t, 30*time.Minute, client.Unwrap().Options().ConnMaxLifetime,
		"default ConnMaxLifetime must be 30m for healthy rotation behind an LB/failover")
}

func TestNew_WithConnMaxLifetimeZero_OptsOutOfDefault(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	client, err := redis.New(t.Context(), redis.Config{Host: mr.Host(), Port: mr.Port()}, redis.WithConnMaxLifetime(0))
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Shutdown(context.WithoutCancel(t.Context())) })

	assert.Zero(t, client.Unwrap().Options().ConnMaxLifetime,
		"explicit WithConnMaxLifetime(0) must still opt back into go-redis's unlimited default")
}

func TestNew_WithConnMaxLifetime_Override(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	client, err := redis.New(
		t.Context(), redis.Config{Host: mr.Host(), Port: mr.Port()},
		redis.WithConnMaxLifetime(5*time.Minute),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Shutdown(context.WithoutCancel(t.Context())) })

	assert.Equal(t, 5*time.Minute, client.Unwrap().Options().ConnMaxLifetime)
}

// spanNames extracts span names from a recorder for a readable assertion.
func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	names := make([]string, 0, len(spans))
	for _, s := range spans {
		names = append(names, s.Name())
	}

	return names
}

func TestNew_WithOtel_Default_TracesDial(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	// t.Context() is already cancelled when cleanups run.
	t.Cleanup(func() { _ = tp.Shutdown(context.WithoutCancel(t.Context())) })

	client, err := redis.New(
		t.Context(), redis.Config{Host: mr.Host(), Port: mr.Port()},
		redis.WithOtel(), redis.WithTracerProvider(tp),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Shutdown(context.WithoutCancel(t.Context())) })

	require.NoError(t, client.Set(t.Context(), "k", "v", 0))

	assert.Contains(t, spanNames(recorder.Ended()), "redis.dial",
		"default WithOtel() must still trace connection dials for existing callers")
}

// TestNew_WithoutDialSpans_SuppressesDialSpan: a caller that finds dial spans
// noisy (they carry no request context, so every dial becomes its own
// orphaned root trace) can suppress just those with WithoutDialSpans, while
// command tracing keeps working.
func TestNew_WithoutDialSpans_SuppressesDialSpan(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	// t.Context() is already cancelled when cleanups run.
	t.Cleanup(func() { _ = tp.Shutdown(context.WithoutCancel(t.Context())) })

	client, err := redis.New(
		t.Context(), redis.Config{Host: mr.Host(), Port: mr.Port()},
		redis.WithOtel(), redis.WithoutDialSpans(), redis.WithTracerProvider(tp),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Shutdown(context.WithoutCancel(t.Context())) })

	require.NoError(t, client.Set(t.Context(), "k", "v", 0))

	names := spanNames(recorder.Ended())
	assert.NotContains(t, names, "redis.dial", "WithoutDialSpans must suppress the dial span")
	assert.NotEmpty(t, names, "command tracing must still work with the dial filter on")
}
