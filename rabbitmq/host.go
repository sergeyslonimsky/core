package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/furdarius/rabbitroutine"
	"golang.org/x/sync/errgroup"

	"github.com/sergeyslonimsky/core/lifecycle"
)

// ConsumerHost is a Runner that owns a connection and runs a set of
// rabbitroutine.Consumer instances under it.
//
// Implements lifecycle.Runner.
type ConsumerHost struct {
	conn      ConnectorInterface
	consumers []rabbitroutine.Consumer
	logger    *slog.Logger

	mu       sync.Mutex
	cancelFn context.CancelFunc
	done     chan struct{}
}

// HostOption configures a ConsumerHost.
type HostOption func(*hostOptions)

type hostOptions struct {
	logger        *slog.Logger
	connector     ConnectorInterface
	connectorOpts []ConnectorOption
}

// WithHostLogger attaches a *slog.Logger used for lifecycle events.
func WithHostLogger(l *slog.Logger) HostOption {
	return func(o *hostOptions) { o.logger = l }
}

// WithHostConnector lets the caller inject a pre-built connector (e.g.,
// shared with a Publisher).
func WithHostConnector(c ConnectorInterface) HostOption {
	return func(o *hostOptions) { o.connector = c }
}

// WithHostConnectorOptions threads ConnectorOption values into the
// internally-created *Connector when WithHostConnector is not used.
func WithHostConnectorOptions(opts ...ConnectorOption) HostOption {
	return func(o *hostOptions) { o.connectorOpts = append(o.connectorOpts, opts...) }
}

// NewConsumerHost constructs a ConsumerHost. Use AddConsumer to register
// consumers before calling Run (typically via app.App).
func NewConsumerHost(cfg Config, opts ...HostOption) *ConsumerHost {
	o := &hostOptions{logger: slog.Default()} //nolint:exhaustruct
	for _, apply := range opts {
		apply(o)
	}

	conn := o.connector
	if conn == nil {
		conn = NewConnector(cfg, o.connectorOpts...)
	}

	return &ConsumerHost{ //nolint:exhaustruct // mu, cancelFn, done have valid zero values
		conn:      conn,
		consumers: make([]rabbitroutine.Consumer, 0),
		logger:    o.logger,
	}
}

// AddConsumer registers a rabbitroutine.Consumer to be started when Run is
// called. Must be called before Run.
func (h *ConsumerHost) AddConsumer(consumer rabbitroutine.Consumer) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.consumers = append(h.consumers, consumer)
}

// Run dials the broker, then starts every registered consumer concurrently.
// Blocks until ctx is cancelled or any consumer/dial returns an error.
//
// Implements lifecycle.Runner.
func (h *ConsumerHost) Run(ctx context.Context) error {
	h.mu.Lock()

	if len(h.consumers) == 0 {
		h.mu.Unlock()

		return nil
	}

	consumers := append([]rabbitroutine.Consumer{}, h.consumers...)
	innerCtx, cancel := context.WithCancel(ctx)
	h.cancelFn = cancel
	h.done = make(chan struct{})
	h.mu.Unlock()

	defer close(h.done)

	g, gctx := errgroup.WithContext(innerCtx)

	g.Go(func() error {
		if err := h.conn.Connect(gctx); err != nil {
			return fmt.Errorf("connect rabbitmq: %w", err)
		}

		return nil
	})

	for _, consumer := range consumers {
		g.Go(func() error {
			if err := h.conn.StartConsumer(gctx, consumer); err != nil {
				return fmt.Errorf("start rabbitmq consumer: %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("rabbitmq host: %w", err)
	}

	return nil
}

// Shutdown signals Run to stop and waits for it to drain. Implements
// lifecycle.Resource.
func (h *ConsumerHost) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	cancel := h.cancelFn
	done := h.done
	h.cancelFn = nil
	h.mu.Unlock()

	if cancel == nil {
		return nil
	}

	cancel()

	if done == nil {
		return nil
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("rabbitmq host shutdown: %w", ctx.Err())
	}
}

// Compile-time assertions.
var (
	_ lifecycle.Runner   = (*ConsumerHost)(nil)
	_ lifecycle.Resource = (*ConsumerHost)(nil)
)
