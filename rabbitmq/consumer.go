package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ExchangeKind enumerates the AMQP exchange types supported by Consumer.
type ExchangeKind string

const (
	ExchangeKindDirect  ExchangeKind = "direct"
	ExchangeKindFanout  ExchangeKind = "fanout"
	ExchangeKindHeaders ExchangeKind = "headers"
	ExchangeKindTopic   ExchangeKind = "topic"
)

const (
	defaultConsumePrefetchCount = 1
	defaultConsumePrefetchSize  = 0
	defaultConsumeGlobal        = false
	defaultWorkerCount          = 1
)

// ErrUnsupportedMessage is returned by the default body marshaller when the
// AMQP delivery's ContentType is not "application/json" and no custom
// marshaller was provided.
var ErrUnsupportedMessage = errors.New("unsupported message type")

// BodyMarshaller decodes a raw AMQP delivery body into a typed payload.
type BodyMarshaller[I any] func(body []byte, payload *I) error

// Configuration structures forwarded to the underlying amqp.Channel calls
// during Declare / Consume.
type (
	// ExchangeConfig governs ch.ExchangeDeclare.
	ExchangeConfig struct {
		Name       string
		Kind       ExchangeKind
		Durable    bool
		AutoDelete bool
		Internal   bool
		NoWait     bool
		Args       amqp.Table
	}

	// QueueConfig governs ch.QueueDeclare.
	QueueConfig struct {
		Durable    bool
		AutoDelete bool
		Exclusive  bool
		NoWait     bool
		Args       amqp.Table
	}

	// BindQueueConfig governs ch.QueueBind.
	BindQueueConfig struct {
		Exchange   string
		RoutingKey string
		NoWait     bool
		Args       amqp.Table
	}

	// ConsumerConfig governs ch.Consume.
	ConsumerConfig struct {
		AutoAck   bool
		Exclusive bool
		NoLocal   bool
		NoWait    bool
		Args      amqp.Table
	}

	// ConsumeOpts governs ch.Qos prefetch settings.
	ConsumeOpts struct {
		PrefetchCount int
		PrefetchSize  int
		Global        bool
	}
)

// consumerBase holds all payload-type-independent Consumer settings.
// Separated from Consumer[I] so that ConsumerOption can remain non-generic
// — the only type-dependent knob (the body marshaller) is configured via
// Consumer.SetMarshaller, not through the options pipeline.
type consumerBase struct {
	queue           string
	consumerCfg     ConsumerConfig
	exchangeConfig  *ExchangeConfig
	queueConfig     *QueueConfig
	bindQueueConfig *BindQueueConfig
	consumeOpts     ConsumeOpts
	workerCount     int
	logger          *slog.Logger
}

// ConsumerOption configures a Consumer. Non-generic so option sites don't
// need to repeat the payload type parameter.
//
//	rabbitmq.NewConsumer[OrderEvent]("orders", processor,
//	    rabbitmq.WithExchange(exchangeCfg),
//	    rabbitmq.WithQueue(queueCfg),
//	)
//
// To install a custom body marshaller (which IS type-dependent), use
// Consumer.SetMarshaller after construction.
type ConsumerOption func(*consumerBase)

// WithConsumerConfig overrides the consume-call settings (AutoAck,
// Exclusive, etc.).
func WithConsumerConfig(c ConsumerConfig) ConsumerOption {
	return func(cs *consumerBase) { cs.consumerCfg = c }
}

// WithExchange instructs Declare to create an exchange.
func WithExchange(c ExchangeConfig) ConsumerOption {
	return func(cs *consumerBase) { cs.exchangeConfig = &c }
}

// WithQueue instructs Declare to create the queue.
func WithQueue(c QueueConfig) ConsumerOption {
	return func(cs *consumerBase) { cs.queueConfig = &c }
}

// WithBindQueue instructs Declare to bind the queue to the exchange.
func WithBindQueue(c BindQueueConfig) ConsumerOption {
	return func(cs *consumerBase) { cs.bindQueueConfig = &c }
}

// WithConsumeOpts overrides QoS prefetch settings.
func WithConsumeOpts(c ConsumeOpts) ConsumerOption {
	return func(cs *consumerBase) { cs.consumeOpts = c }
}

// WithWorkerCount sets the number of concurrent goroutines processing
// deliveries for this consumer. Default: 1 (ordered processing — matches
// the default PrefetchCount=1).
//
// Raising this above 1 gives higher throughput at the cost of losing
// message order within the queue. Keep ≤ PrefetchCount (ConsumeOpts) or
// the extra workers starve.
//
// Any value < 1 is normalized to 1.
func WithWorkerCount(count int) ConsumerOption {
	return func(cs *consumerBase) {
		if count < 1 {
			count = 1
		}

		cs.workerCount = count
	}
}

// WithConsumerLogger attaches a *slog.Logger used for per-message error
// logging (marshal failures, nack/ack errors). Defaults to slog.Default()
// when omitted.
func WithConsumerLogger(l *slog.Logger) ConsumerOption {
	return func(cs *consumerBase) { cs.logger = l }
}

// Consumer is a per-queue consumer with declare-and-consume logic. Attach
// to a ConsumerHost via host.AddConsumer(consumer).
type Consumer[I any] struct {
	consumerBase

	processor      Processor[I]
	bodyMarshaller BodyMarshaller[I]
}

// NewConsumer creates a Consumer for the given queue and processor. By
// default the delivery body is JSON-unmarshalled into the payload type I;
// call SetMarshaller to override.
//
// All ConsumerOption values configure declare/consume behavior.
func NewConsumer[I any](
	queue string,
	processor Processor[I],
	opts ...ConsumerOption,
) *Consumer[I] {
	base := consumerBase{ //nolint:exhaustruct // exchangeConfig/queueConfig/bindQueueConfig are opt-in pointers
		queue: queue,
		consumeOpts: ConsumeOpts{
			PrefetchCount: defaultConsumePrefetchCount,
			PrefetchSize:  defaultConsumePrefetchSize,
			Global:        defaultConsumeGlobal,
		},
		workerCount: defaultWorkerCount,
		logger:      slog.Default(),
	}

	for _, apply := range opts {
		apply(&base)
	}

	return &Consumer[I]{ //nolint:exhaustruct // bodyMarshaller is opt-in via SetMarshaller
		consumerBase: base,
		processor:    processor,
	}
}

// SetMarshaller installs a custom body marshaller. Passing nil resets to
// the default JSON unmarshaller.
//
// Must be called before the consumer starts processing deliveries (i.e.,
// before the hosting ConsumerHost's Run).
func (c *Consumer[I]) SetMarshaller(m BodyMarshaller[I]) {
	c.bodyMarshaller = m
}

// Declare runs ExchangeDeclare / QueueDeclare / QueueBind on the channel as
// dictated by the configured options. Called by rabbitroutine on every
// successful (re)connect.
func (c *Consumer[I]) Declare(_ context.Context, ch *amqp.Channel) error {
	if c.exchangeConfig != nil {
		if err := ch.ExchangeDeclare(
			c.exchangeConfig.Name,
			string(c.exchangeConfig.Kind),
			c.exchangeConfig.Durable,
			c.exchangeConfig.AutoDelete,
			c.exchangeConfig.Internal,
			c.exchangeConfig.NoWait,
			c.exchangeConfig.Args,
		); err != nil {
			return fmt.Errorf("declare exchange %s: %w", c.exchangeConfig.Name, err)
		}
	}

	if c.queueConfig != nil {
		if _, err := ch.QueueDeclare(
			c.queue,
			c.queueConfig.Durable,
			c.queueConfig.AutoDelete,
			c.queueConfig.Exclusive,
			c.queueConfig.NoWait,
			c.queueConfig.Args,
		); err != nil {
			return fmt.Errorf("declare queue %s: %w", c.queue, err)
		}
	}

	if c.bindQueueConfig != nil {
		if err := ch.QueueBind(
			c.queue,
			c.bindQueueConfig.RoutingKey,
			c.bindQueueConfig.Exchange,
			c.bindQueueConfig.NoWait,
			c.bindQueueConfig.Args,
		); err != nil {
			return fmt.Errorf("bind queue %s: %w", c.bindQueueConfig.RoutingKey, err)
		}
	}

	return nil
}

// Consume sets QoS, opens the consume stream, and processes deliveries
// until ctx is cancelled. Called by rabbitroutine.
//
// Concurrency: a bounded worker pool sized by WithWorkerCount (default 1)
// pulls from the delivery channel. On ctx cancellation or channel close,
// Consume stops distributing new work and waits for all in-flight workers
// to finish their current Ack/Nack before returning. This prevents
// Ack-after-close panics and gives in-flight messages a chance to settle.
func (c *Consumer[I]) Consume(ctx context.Context, ch *amqp.Channel) error {
	if err := ch.Qos(
		c.consumeOpts.PrefetchCount,
		c.consumeOpts.PrefetchSize,
		c.consumeOpts.Global,
	); err != nil {
		return fmt.Errorf("set qos: %w", err)
	}

	msgs, err := ch.Consume(
		c.queue,
		c.processor.GetName(),
		c.consumerCfg.AutoAck,
		c.consumerCfg.Exclusive,
		c.consumerCfg.NoLocal,
		c.consumerCfg.NoWait,
		c.consumerCfg.Args,
	)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	workerCount := c.workerCount
	if workerCount < 1 {
		workerCount = defaultWorkerCount
	}

	var workers sync.WaitGroup

	workers.Add(workerCount)

	for range workerCount {
		go func() {
			defer workers.Done()

			c.workerLoop(ctx, msgs)
		}()
	}

	// workersDone closes when every worker has returned — happens when ctx
	// is cancelled or msgs is closed (broker disconnect, amqp channel close).
	workersDone := make(chan struct{})

	go func() {
		workers.Wait()
		close(workersDone)
	}()

	// Block until workers finish. If ctx is cancelled first, workers will
	// observe it and exit shortly; wait for them to complete their current
	// Ack/Nack before returning — otherwise rabbitroutine could close the
	// amqp channel while a worker is mid-ack and cause a panic.
	select {
	case <-ctx.Done():
		<-workersDone
	case <-workersDone:
	}

	// If workers exited because msgs was closed (broker disconnect), surface
	// amqp.ErrClosed so rabbitroutine triggers reconnect. A clean ctx
	// cancellation returns nil.
	if ctx.Err() == nil {
		return amqp.ErrClosed
	}

	return nil
}

// workerLoop is run by each worker goroutine. Pulls deliveries from msgs
// until ctx is cancelled or the channel closes.
func (c *Consumer[I]) workerLoop(ctx context.Context, msgs <-chan amqp.Delivery) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}

			c.handleDelivery(ctx, msg)
		}
	}
}

// handleDelivery processes a single delivery and acks/nacks accordingly.
// Never called after ctx cancellation returned from workerLoop, so Ack/Nack
// race with channel close is avoided.
func (c *Consumer[I]) handleDelivery(ctx context.Context, msg amqp.Delivery) {
	if err := c.process(ctx, msg); err != nil {
		c.logger.WarnContext(ctx, "rabbitmq message processing failed",
			slog.String("id", msg.MessageId),
			slog.String("body", string(msg.Body)),
			slog.String("content_type", msg.ContentType),
			slog.Any("headers", msg.Headers),
			slog.String("routing_key", msg.RoutingKey),
			slog.String("exchange", msg.Exchange),
			slog.Any("err", err),
		)

		if nErr := msg.Nack(false, false); nErr != nil {
			c.logger.ErrorContext(ctx, "rabbitmq nack failed", slog.Any("err", nErr))
		}

		return
	}

	if aErr := msg.Ack(false); aErr != nil {
		c.logger.ErrorContext(ctx, "rabbitmq ack failed", slog.Any("err", aErr))
	}
}

func (c *Consumer[I]) process(ctx context.Context, msg amqp.Delivery) error {
	payload, err := c.marshalMsgBody(msg)
	if err != nil {
		return fmt.Errorf("marshal message body: %w", err)
	}

	if err = c.processor.Process(ctx, Message[I]{Payload: payload}); err != nil {
		return fmt.Errorf("process message: %w", err)
	}

	return nil
}

func (c *Consumer[I]) marshalMsgBody(msg amqp.Delivery) (I, error) {
	var payload I

	if c.bodyMarshaller != nil {
		if err := c.bodyMarshaller(msg.Body, &payload); err != nil {
			return payload, fmt.Errorf("custom marshaller: %w", err)
		}

		return payload, nil
	}

	if msg.ContentType != "application/json" {
		return payload, ErrUnsupportedMessage
	}

	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		return payload, fmt.Errorf("unmarshal body: %w", err)
	}

	return payload, nil
}
