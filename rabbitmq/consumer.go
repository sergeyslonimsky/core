package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

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
)

var ErrUnsupportedMessage = errors.New("unsupported message type")

type BodyMarshaller[I any] func(body []byte, payload *I) error

type (
	ExchangeKind string

	ExchangeConfig struct {
		Name       string
		Kind       ExchangeKind
		Durable    bool
		AutoDelete bool
		Internal   bool
		NoWait     bool
		Args       amqp.Table
	}
	QueueConfig struct {
		Durable    bool
		AutoDelete bool
		Exclusive  bool
		NoWait     bool
		Args       amqp.Table
	}
	BindQueueConfig struct {
		Exchange   string
		RoutingKey string
		NoWait     bool
		Args       amqp.Table
	}
	ConsumerConfig struct {
		AutoAck   bool
		Exclusive bool
		NoLocal   bool
		NoWait    bool
		Args      amqp.Table
	}
	ConsumeOpts struct {
		PrefetchCount int
		PrefetchSize  int
		Global        bool
	}
)

type Opts[I any] func(consumer *Consumer[I])

func WithConsumerConfig[I any](config ConsumerConfig) Opts[I] {
	return func(consumer *Consumer[I]) {
		consumer.config = config
	}
}

func WithExchange[I any](config ExchangeConfig) Opts[I] {
	return func(consumer *Consumer[I]) {
		consumer.exchangeConfig = &config
	}
}

func WithQueue[I any](config QueueConfig) Opts[I] {
	return func(consumer *Consumer[I]) {
		consumer.queueConfig = &config
	}
}

func WithBindQueue[I any](config BindQueueConfig) Opts[I] {
	return func(consumer *Consumer[I]) {
		consumer.bindQueueConfig = &config
	}
}

func WithConsumeOpts[I any](config ConsumeOpts) Opts[I] {
	return func(consumer *Consumer[I]) {
		consumer.consumeOpts = config
	}
}

func WithCustomBodyMarshaller[I any](marshaller BodyMarshaller[I]) Opts[I] {
	return func(consumer *Consumer[I]) {
		consumer.bodyMarshaller = marshaller
	}
}

type Consumer[I any] struct {
	queue           string
	processor       Processor[I]
	config          ConsumerConfig
	exchangeConfig  *ExchangeConfig
	queueConfig     *QueueConfig
	bindQueueConfig *BindQueueConfig
	consumeOpts     ConsumeOpts
	bodyMarshaller  BodyMarshaller[I]
}

func NewConsumer[I any](queue string, processor Processor[I], opts ...Opts[I]) *Consumer[I] {
	consumer := &Consumer[I]{ //nolint:exhaustruct
		queue:     queue,
		processor: processor,
		consumeOpts: ConsumeOpts{
			PrefetchCount: defaultConsumePrefetchCount,
			PrefetchSize:  defaultConsumePrefetchSize,
			Global:        defaultConsumeGlobal,
		},
	}

	for _, opt := range opts {
		opt(consumer)
	}

	return consumer
}

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

func (c *Consumer[I]) Consume(ctx context.Context, ch *amqp.Channel) error { //nolint:funlen,cyclop
	err := ch.Qos(c.consumeOpts.PrefetchCount, c.consumeOpts.PrefetchSize, c.consumeOpts.Global)
	if err != nil {
		return fmt.Errorf("set qos: %w", err)
	}

	msgs, err := ch.Consume(
		c.queue,
		c.processor.GetName(),
		c.config.AutoAck,
		c.config.Exclusive,
		c.config.NoLocal,
		c.config.NoWait,
		c.config.Args,
	)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}

			processCtx, processCancel := context.WithCancel(ctx)

			go func(ctx context.Context, cancelFunc func(), msg amqp.Delivery) {
				if err := c.process(ctx, msg); err != nil {
					slog.Warn(
						"failed to process message",
						slog.String("id", msg.MessageId),
						slog.String("body", string(msg.Body)),
						slog.String("content_type", msg.ContentType),
						slog.Any("headers", msg.Headers),
						slog.String("routing_key", msg.RoutingKey),
						slog.String("exchange", msg.Exchange),
						slog.String("error", err.Error()),
					)

					if nErr := msg.Nack(false, false); nErr != nil {
						slog.Error(
							"failed to nack message",
							slog.String("error", nErr.Error()),
						)
					}
				} else {
					if aErr := msg.Ack(false); aErr != nil {
						slog.Error(
							"failed to ack message",
							slog.String("error", aErr.Error()),
						)
					}
				}

				cancelFunc()
			}(processCtx, processCancel, msg)
		case <-ctx.Done():
			if err = ctx.Err(); err != nil {
				return fmt.Errorf("cancel consume: %w", err)
			}

			return nil
		}
	}
}

func (c *Consumer[I]) process(ctx context.Context, msg amqp.Delivery) error {
	payload, err := c.marshallMsgBody(msg)
	if err != nil {
		return fmt.Errorf("marshall message body: %w", err)
	}

	if err = c.processor.Process(ctx, Message[I]{
		Payload: payload,
	}); err != nil {
		return fmt.Errorf("process message: %w", err)
	}

	return nil
}

func (c *Consumer[I]) marshallMsgBody(msg amqp.Delivery) (I, error) {
	var payload I

	if c.bodyMarshaller != nil {
		if err := c.bodyMarshaller(msg.Body, &payload); err != nil {
			return payload, fmt.Errorf("marshal body with custom marshaller: %w", err)
		}
	}

	switch msg.ContentType {
	case "application/json":
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return payload, fmt.Errorf("unmarshal body: %w", err)
		}
	default:
		return payload, ErrUnsupportedMessage
	}

	return payload, nil
}
