package redis_test

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/redis"
)

func TestNew_DefaultConnMaxLifetime(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	client, err := redis.New(t.Context(), redis.Config{Host: mr.Host(), Port: mr.Port()})
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Shutdown(t.Context()) })

	assert.Equal(t, 30*time.Minute, client.Unwrap().Options().ConnMaxLifetime,
		"default ConnMaxLifetime must be 30m for healthy rotation behind an LB/failover")
}

func TestNew_WithConnMaxLifetimeZero_OptsOutOfDefault(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	client, err := redis.New(t.Context(), redis.Config{Host: mr.Host(), Port: mr.Port()}, redis.WithConnMaxLifetime(0))
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Shutdown(t.Context()) })

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

	t.Cleanup(func() { _ = client.Shutdown(t.Context()) })

	assert.Equal(t, 5*time.Minute, client.Unwrap().Options().ConnMaxLifetime)
}
