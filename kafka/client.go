package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sergeyslonimsky/core/lifecycle"
)

// Client is a base wrapper around *kgo.Client. Use this when you need to
// build your own producer or consumer logic on top of franz-go primitives.
// For ready-made producers and consumers, see Producer and Consumer.
//
// Implements lifecycle.Resource and lifecycle.Healthchecker.
type Client struct {
	client       *kgo.Client
	logger       *slog.Logger
	shutdownOnce sync.Once
}

// ClientConfig describes the seed brokers and client identity for a Client.
type ClientConfig struct {
	// Brokers is the seed broker list. Required.
	Brokers []string

	// ClientID is the kgo.ClientID. Optional.
	ClientID string
}

// NewClient creates a base franz-go client.
func NewClient(_ context.Context, cfg ClientConfig, opts ...Option) (*Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, ErrEmptyBrokersList
	}

	o := &options{logger: slog.Default()} //nolint:exhaustruct
	for _, apply := range opts {
		apply(o)
	}

	kgoOpts := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}
	if cfg.ClientID != "" {
		kgoOpts = append(kgoOpts, kgo.ClientID(cfg.ClientID))
	}

	kgoOpts = append(kgoOpts, o.applyOtel()...)
	kgoOpts = append(kgoOpts, o.extraKgo...)

	c, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("create franz-go client: %w", err)
	}

	//nolint:exhaustruct // shutdownOnce has valid zero value
	return &Client{
		client: c,
		logger: o.logger,
	}, nil
}

// Unwrap returns the underlying *kgo.Client. Use for advanced operations
// (admin, transactions, custom hooks). Owned by Client — don't Close it
// directly; always go through Shutdown.
func (c *Client) Unwrap() *kgo.Client {
	return c.client
}

// Shutdown closes the underlying client. Implements lifecycle.Resource.
//
// Idempotent and concurrent-safe: the close runs exactly once.
//
// kgo.Client.Close blocks until in-flight produces drain. The ctx is
// currently advisory — kgo does not honor it directly — but the signature
// matches lifecycle.Resource for uniform integration with app.App.
func (c *Client) Shutdown(_ context.Context) error {
	c.shutdownOnce.Do(func() {
		c.client.Close()
	})

	return nil
}

// Healthcheck verifies that the client has discovered at least one broker.
// Implements lifecycle.Healthchecker.
func (c *Client) Healthcheck(_ context.Context) error {
	if len(c.client.DiscoveredBrokers()) == 0 {
		return ErrNoBrokersDiscovered
	}

	return nil
}

// Compile-time assertions.
var (
	_ lifecycle.Resource      = (*Client)(nil)
	_ lifecycle.Healthchecker = (*Client)(nil)
)
