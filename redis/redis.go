// Package redis wraps github.com/redis/go-redis/v9 with the lifecycle
// contract used across core: a typed Config, functional Options, and
// Shutdown / Healthcheck methods so a Client can be registered with app.App.
//
// The Client is a lifecycle.Resource (not a Runner) — its only background
// work is the connection pool maintained by go-redis, which is an
// implementation detail. Callers use the typed Get/Set/Del helpers or call
// Unwrap to access the underlying *redis.Client for advanced operations
// (pipelines, pubsub, scripts).
//
// Typical usage:
//
//	client, err := redis.New(ctx, cfg, redis.WithOtel())
//	if err != nil { log.Fatal(err) }
//
//	a := app.New()
//	a.Add(client)   // registers Shutdown + Healthcheck
package redis

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/sergeyslonimsky/core/lifecycle"
)

// Client is a thin wrapper around *redis.Client that adds lifecycle
// conformance and cheap convenience methods. For anything beyond Get/Set/Del,
// call Unwrap to get the underlying *redis.Client.
//
// Implements lifecycle.Resource and lifecycle.Healthchecker.
type Client struct {
	client       *redis.Client
	logger       *slog.Logger
	shutdownOnce sync.Once
	shutdownErr  error
}

// Option configures a new Client.
type Option func(*options)

type options struct {
	logger       *slog.Logger
	poolSize     int
	readTimeout  time.Duration
	writeTimeout time.Duration
	otelEnabled  bool
}

// WithLogger attaches a *slog.Logger used for lifecycle events. Defaults to
// slog.Default() when omitted.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithPoolSize overrides the connection pool size. Default: go-redis uses
// 10*runtime.NumCPU() if unset.
func WithPoolSize(n int) Option {
	return func(o *options) { o.poolSize = n }
}

// WithReadTimeout overrides the per-command read timeout. Default: go-redis
// uses 3 seconds if unset.
func WithReadTimeout(d time.Duration) Option {
	return func(o *options) { o.readTimeout = d }
}

// WithWriteTimeout overrides the per-command write timeout. Default: go-redis
// uses ReadTimeout if unset.
func WithWriteTimeout(d time.Duration) Option {
	return func(o *options) { o.writeTimeout = d }
}

// WithOtel enables OpenTelemetry tracing and metrics on every Redis command
// via go-redis's redisotel extension. Uses the global tracer and meter
// providers, so otel.Setup must run and be registered with app.App before
// New so the providers are non-noop.
//
// One span per command is created with the command name, key (if available),
// and result/error status.
func WithOtel() Option {
	return func(o *options) { o.otelEnabled = true }
}

// New connects to Redis and verifies the connection with a Ping. Returns a
// Client ready for use.
//
// Returns an error if the initial Ping fails — callers should treat this as
// fatal and not continue startup.
func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	o := &options{logger: slog.Default()} //nolint:exhaustruct
	for _, apply := range opts {
		apply(o)
	}

	rOpts := &redis.Options{ //nolint:exhaustruct
		Addr:         cfg.addr(),
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     o.poolSize,
		ReadTimeout:  o.readTimeout,
		WriteTimeout: o.writeTimeout,
	}

	rc := redis.NewClient(rOpts)

	if o.otelEnabled {
		if err := redisotel.InstrumentTracing(rc); err != nil {
			return nil, fmt.Errorf("instrument tracing: %w", err)
		}

		if err := redisotel.InstrumentMetrics(rc); err != nil {
			return nil, fmt.Errorf("instrument metrics: %w", err)
		}
	}

	if err := rc.Ping(ctx).Err(); err != nil {
		// Best-effort close on failed startup — don't leak the pool.
		_ = rc.Close()

		return nil, fmt.Errorf("ping redis at %s: %w", cfg.addr(), err)
	}

	//nolint:exhaustruct // shutdownOnce/shutdownErr have valid zero values
	return &Client{client: rc, logger: o.logger}, nil
}

// Unwrap returns the underlying *redis.Client. Use for operations not
// covered by the convenience methods (pipelines, transactions, pubsub,
// scripts, custom commands).
//
// The returned pointer is owned by Client — do not Close it directly;
// always go through Shutdown.
func (c *Client) Unwrap() *redis.Client {
	return c.client
}

// Shutdown closes the underlying Redis client and releases the connection
// pool. Implements lifecycle.Resource.
//
// Idempotent and concurrent-safe: the close runs exactly once; every
// subsequent call returns the same result.
//
// The ctx parameter is currently ignored — go-redis's Close is synchronous
// and not context-aware. The signature matches lifecycle.Resource for
// uniform integration with app.App.
func (c *Client) Shutdown(_ context.Context) error {
	c.shutdownOnce.Do(func() {
		if err := c.client.Close(); err != nil {
			c.shutdownErr = fmt.Errorf("close redis client: %w", err)
		}
	})

	return c.shutdownErr
}

// Healthcheck pings the Redis server. Implements lifecycle.Healthchecker.
//
// Cheap (single PING command) and respects ctx deadlines via go-redis's
// per-command timeout pipeline.
func (c *Client) Healthcheck(ctx context.Context) error {
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}

	return nil
}

// Get returns the string value at key, or an error. Wraps redis.Client.Get
// for the common case.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	result, err := c.client.Get(ctx, key).Result()
	if err != nil {
		return "", fmt.Errorf("get key %q: %w", key, err)
	}

	return result, nil
}

// Set stores value at key with the given TTL. A ttl of 0 means no expiration.
func (c *Client) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	if err := c.client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("set key %q: %w", key, err)
	}

	return nil
}

// Del removes one or more keys. Returns nil even if a key did not exist.
func (c *Client) Del(ctx context.Context, keys ...string) error {
	if err := c.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("del keys %v: %w", keys, err)
	}

	return nil
}

// Compile-time assertions.
var (
	_ lifecycle.Resource      = (*Client)(nil)
	_ lifecycle.Healthchecker = (*Client)(nil)
)
