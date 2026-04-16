package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/furdarius/rabbitroutine"
)

const (
	defaultReconnectionAttempts uint = 3
	defaultConnectionWaitTime        = 2 * time.Second
)

// ConnectorInterface is the surface used by Publisher and ConsumerHost to
// drive the underlying rabbitroutine.Connector. Mainly an extension seam:
// tests can substitute fakes; production code uses *Connector.
//
// The concrete struct is Connector; the interface is ConnectorInterface
// to keep call sites readable and avoid name collision.
type ConnectorInterface interface {
	// Connect blocks until the broker is reachable and the connection is
	// established. Subsequent reconnects are handled internally by
	// rabbitroutine.
	Connect(ctx context.Context) error

	// Inner returns the underlying *rabbitroutine.Connector for advanced
	// use (custom listeners, channel pools).
	Inner() *rabbitroutine.Connector

	// StartConsumer attaches a rabbitroutine.Consumer to the connector and
	// runs its loop until ctx is cancelled.
	StartConsumer(ctx context.Context, consumer rabbitroutine.Consumer) error
}

// connectorOptions holds settings applied via ConnectorOption.
type connectorOptions struct {
	rabbitRoutineConfig rabbitroutine.Config

	retriedListener      func(rabbitroutine.Retried)
	dialedListener       func(rabbitroutine.Dialed)
	amqpNotifiedListener func(rabbitroutine.AMQPNotified)
}

// ConnectorOption configures a Connector.
type ConnectorOption func(*connectorOptions)

// WithReconnectAttempts overrides how many reconnect attempts the
// underlying rabbitroutine.Connector makes before giving up. Default: 3.
func WithReconnectAttempts(n uint) ConnectorOption {
	return func(o *connectorOptions) { o.rabbitRoutineConfig.ReconnectAttempts = n }
}

// WithReconnectWait overrides the delay between reconnect attempts.
// Default: 2 seconds.
func WithReconnectWait(d time.Duration) ConnectorOption {
	return func(o *connectorOptions) { o.rabbitRoutineConfig.Wait = d }
}

// WithRetriedListener attaches a callback fired on every reconnect retry.
// Useful for surfacing reconnect activity in logs or metrics.
func WithRetriedListener(fn func(rabbitroutine.Retried)) ConnectorOption {
	return func(o *connectorOptions) { o.retriedListener = fn }
}

// WithDialedListener attaches a callback fired on every successful dial.
func WithDialedListener(fn func(rabbitroutine.Dialed)) ConnectorOption {
	return func(o *connectorOptions) { o.dialedListener = fn }
}

// WithAMQPNotifiedListener attaches a callback fired when the broker sends
// an AMQP notification (channel close, blocked, etc).
func WithAMQPNotifiedListener(fn func(rabbitroutine.AMQPNotified)) ConnectorOption {
	return func(o *connectorOptions) { o.amqpNotifiedListener = fn }
}

// Connector wraps a rabbitroutine.Connector with reconnect configuration
// and listener wiring. Created via NewConnector or implicitly inside
// NewPublisher / NewConsumerHost.
type Connector struct {
	cfg  Config
	conn *rabbitroutine.Connector
}

// NewConnector builds a Connector from the given Config and options. Does
// NOT dial — use Connect (or have NewPublisher / NewConsumerHost call it
// for you).
func NewConnector(cfg Config, opts ...ConnectorOption) *Connector {
	o := &connectorOptions{ //nolint:exhaustruct
		rabbitRoutineConfig: rabbitroutine.Config{
			ReconnectAttempts: defaultReconnectionAttempts,
			Wait:              defaultConnectionWaitTime,
		},
	}
	for _, apply := range opts {
		apply(o)
	}

	conn := rabbitroutine.NewConnector(o.rabbitRoutineConfig)

	if o.retriedListener != nil {
		conn.AddRetriedListener(o.retriedListener)
	}

	if o.dialedListener != nil {
		conn.AddDialedListener(o.dialedListener)
	}

	if o.amqpNotifiedListener != nil {
		conn.AddAMQPNotifiedListener(o.amqpNotifiedListener)
	}

	return &Connector{cfg: cfg, conn: conn}
}

// Connect dials the broker. Subsequent reconnects are managed internally.
// Blocks until the first connection is established or ctx expires.
func (c *Connector) Connect(ctx context.Context) error {
	if err := c.conn.Dial(ctx, c.cfg.DSN()); err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}

	return nil
}

// Inner returns the underlying *rabbitroutine.Connector. Use for advanced
// listener wiring or to integrate with code that expects the raw type.
func (c *Connector) Inner() *rabbitroutine.Connector {
	return c.conn
}

// StartConsumer attaches a rabbitroutine.Consumer and runs its loop until
// ctx is cancelled. Typically called by ConsumerHost.Run, not directly.
func (c *Connector) StartConsumer(ctx context.Context, consumer rabbitroutine.Consumer) error {
	if err := c.conn.StartConsumer(ctx, consumer); err != nil {
		return fmt.Errorf("start consumer: %w", err)
	}

	return nil
}

// Compile-time check.
var _ ConnectorInterface = (*Connector)(nil)
