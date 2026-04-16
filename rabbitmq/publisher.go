package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/furdarius/rabbitroutine"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/sergeyslonimsky/core/lifecycle"
)

const (
	defaultMaxAttempts     = 16
	defaultLinearDelayTime = 10 * time.Millisecond
	defaultDialTimeout     = 5 * time.Second
)

// PublishOpts captures per-publish AMQP message metadata. Body is always
// JSON-encoded from the message argument; the fields here populate the
// envelope only.
type PublishOpts struct {
	ContentType string
	MessageID   string
	AppID       string
	UserID      string
	Priority    uint8
	Type        string
}

// Publisher publishes JSON messages to RabbitMQ exchanges with automatic
// retry/reconnect via rabbitroutine.RetryPublisher.
//
// Publisher is a Resource (not a Runner): the connection's reconnect loop is
// an internal goroutine started in the constructor and stopped in Shutdown.
//
// Implements lifecycle.Resource.
type Publisher struct {
	conn      ConnectorInterface
	publisher rabbitroutine.Publisher
	logger    *slog.Logger

	mu       sync.Mutex
	cancelBg context.CancelFunc
	bgDone   chan struct{}
}

// PublisherOption configures a Publisher.
type PublisherOption func(*publisherOptions)

type publisherOptions struct {
	logger             *slog.Logger
	connector          ConnectorInterface
	maxPublishAttempts uint
	publishLinearDelay time.Duration
	connectorOpts      []ConnectorOption
}

// WithPublisherLogger attaches a *slog.Logger used for lifecycle events.
// Defaults to slog.Default() when omitted.
func WithPublisherLogger(l *slog.Logger) PublisherOption {
	return func(o *publisherOptions) { o.logger = l }
}

// WithPublisherConnector lets callers inject a pre-built ConnectorInterface
// (e.g., shared between Publisher and ConsumerHost, or a fake in tests).
// When omitted, NewPublisher constructs its own *Connector from the Config.
func WithPublisherConnector(c ConnectorInterface) PublisherOption {
	return func(o *publisherOptions) { o.connector = c }
}

// WithPublishMaxAttempts overrides rabbitroutine.PublishMaxAttemptsSetup.
// Default: 16.
func WithPublishMaxAttempts(n uint) PublisherOption {
	return func(o *publisherOptions) { o.maxPublishAttempts = n }
}

// WithPublishLinearDelay overrides the linear retry delay between publish
// attempts. Default: 10ms.
func WithPublishLinearDelay(d time.Duration) PublisherOption {
	return func(o *publisherOptions) { o.publishLinearDelay = d }
}

// WithPublisherConnectorOptions threads ConnectorOption values into the
// internally-created *Connector when WithPublisherConnector is not used.
// Ignored when an external connector is supplied.
func WithPublisherConnectorOptions(opts ...ConnectorOption) PublisherOption {
	return func(o *publisherOptions) { o.connectorOpts = append(o.connectorOpts, opts...) }
}

// NewPublisher constructs a Publisher and dials the broker. Returns once
// the connection is established (or fails).
//
// Lifetime of the reconnect loop:
//
//   - The ctx passed here bounds only the INITIAL dial. Once NewPublisher
//     returns successfully, the internal reconnect loop is intentionally
//     detached from that ctx (rooted in context.Background) — otherwise a
//     short-lived startup ctx would kill reconnection during normal
//     operation.
//   - To stop the reconnect loop, call Shutdown. This is the ONLY correct
//     way to terminate the Publisher; relying on external ctx cancellation
//     will not stop the loop.
//
// If no connector is injected via WithPublisherConnector, an internal
// *Connector is built from cfg + WithPublisherConnectorOptions.
//
//nolint:contextcheck // background ctx for reconnect loop outlives the constructor's ctx by design — see godoc
func NewPublisher(ctx context.Context, cfg Config, opts ...PublisherOption) (*Publisher, error) {
	o := &publisherOptions{ //nolint:exhaustruct
		logger:             slog.Default(),
		maxPublishAttempts: defaultMaxAttempts,
		publishLinearDelay: defaultLinearDelayTime,
	}
	for _, apply := range opts {
		apply(o)
	}

	conn := o.connector
	if conn == nil {
		conn = NewConnector(cfg, o.connectorOpts...)
	}

	pool := rabbitroutine.NewPool(conn.Inner())
	ensure := rabbitroutine.NewEnsurePublisher(pool)
	pub := rabbitroutine.NewRetryPublisher(
		ensure,
		rabbitroutine.PublishMaxAttemptsSetup(o.maxPublishAttempts),
		rabbitroutine.PublishDelaySetup(rabbitroutine.LinearDelay(o.publishLinearDelay)),
	)

	bgCtx, cancel := context.WithCancel(context.Background())
	bgDone := make(chan struct{})

	// Background reconnect loop. The rabbitroutine.Connector internally
	// keeps trying to reconnect when its dial loop is running.
	go func() {
		defer close(bgDone)

		if err := conn.Connect(bgCtx); err != nil && bgCtx.Err() == nil {
			o.logger.ErrorContext(bgCtx, "rabbitmq publisher reconnect loop ended",
				slog.Any("err", err),
			)
		}
	}()

	// Wait briefly for the initial dial. rabbitroutine doesn't expose a
	// "ready" signal directly, so we do a short ctx-bounded handshake by
	// trying to obtain a channel from the pool.
	dialCtx, dialCancel := context.WithTimeout(ctx, defaultDialTimeout)
	defer dialCancel()

	if _, err := pool.ChannelWithConfirm(dialCtx); err != nil {
		cancel()
		<-bgDone

		return nil, fmt.Errorf("initial rabbitmq dial: %w", err)
	}

	return &Publisher{ //nolint:exhaustruct // mu has valid zero value
		conn:      conn,
		publisher: pub,
		logger:    o.logger,
		cancelBg:  cancel,
		bgDone:    bgDone,
	}, nil
}

// Publish JSON-encodes message and publishes it to exchange with routingKey
// and the metadata in opts. Wraps rabbitroutine.RetryPublisher.Publish, so
// transient failures retry up to WithPublishMaxAttempts.
func (p *Publisher) Publish(
	ctx context.Context,
	exchange, routingKey string,
	message any,
	opts PublishOpts,
) error {
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	if err := p.publisher.Publish(ctx, exchange, routingKey, amqp.Publishing{ //nolint:exhaustruct
		ContentType: opts.ContentType,
		Priority:    opts.Priority,
		MessageId:   opts.MessageID,
		Type:        opts.Type,
		UserId:      opts.UserID,
		AppId:       opts.AppID,
		Body:        body,
	}); err != nil {
		return fmt.Errorf("publish to %s/%s: %w", exchange, routingKey, err)
	}

	return nil
}

// Shutdown stops the background reconnect loop and waits for it to drain.
// Implements lifecycle.Resource.
//
// Safe to call multiple times.
func (p *Publisher) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cancelBg == nil {
		return nil
	}

	p.cancelBg()
	p.cancelBg = nil

	select {
	case <-p.bgDone:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("rabbitmq publisher shutdown: %w", ctx.Err())
	}
}

// Compile-time assertion.
var _ lifecycle.Resource = (*Publisher)(nil)
