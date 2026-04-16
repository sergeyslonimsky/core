package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sergeyslonimsky/core/lifecycle"
)

const defaultProduceTimeout = 30 * time.Second

// Producer is a synchronous single-record kafka producer wrapping a
// franz-go client. Implements lifecycle.Resource — register with app.App.
//
// Construct via NewProducer with a ProducerConfig and Options.
type Producer struct {
	client       *kgo.Client
	timeout      time.Duration
	logger       *slog.Logger
	shutdownOnce sync.Once
}

// NewProducer creates a synchronous producer. The Brokers list must be
// non-empty. cfg.Topic is purely informational — Produce takes the topic
// per-call.
//
// Default timeout is 30s; override with WithProduceTimeout.
func NewProducer(cfg ProducerConfig, opts ...Option) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, ErrEmptyBrokersList
	}

	o := &options{logger: slog.Default()} //nolint:exhaustruct
	for _, apply := range opts {
		apply(o)
	}

	kgoOpts := make([]kgo.Opt, 0, 1+len(o.extraKgo))
	kgoOpts = append(kgoOpts, kgo.SeedBrokers(cfg.Brokers...))
	kgoOpts = append(kgoOpts, o.applyOtel()...)
	kgoOpts = append(kgoOpts, o.extraKgo...)

	c, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer client: %w", err)
	}

	timeout := o.produceTimeout
	if timeout == 0 {
		timeout = defaultProduceTimeout
	}

	//nolint:exhaustruct // shutdownOnce has valid zero value
	return &Producer{client: c, timeout: timeout, logger: o.logger}, nil
}

// Produce sends a single record synchronously. Blocks until the broker
// acks or the producer's per-call timeout expires.
func (p *Producer) Produce(topic, key string, message []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	record := &kgo.Record{ //nolint:exhaustruct
		Topic: topic,
		Key:   []byte(key),
		Value: message,
	}

	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce message: %w", err)
	}

	return nil
}

// Shutdown closes the underlying client, blocking until in-flight produces
// drain. Implements lifecycle.Resource.
//
// Idempotent and concurrent-safe.
func (p *Producer) Shutdown(_ context.Context) error {
	p.shutdownOnce.Do(func() {
		p.client.Close()
	})

	return nil
}

// WithProduceTimeout overrides the per-call produce timeout for Producer.
// Default: 30s. Ignored by NewClient and NewConsumer.
func WithProduceTimeout(d time.Duration) Option {
	return func(o *options) { o.produceTimeout = d }
}

// JSONProducer wraps a Publisher and JSON-encodes T messages on every call.
// Generic over T to give compile-time payload-type safety.
type JSONProducer[T any] struct {
	topic      string
	defaultKey string
	producer   Publisher
}

// NewJSONProducer wraps the given Publisher (typically *Producer) and
// always publishes to the configured topic.
func NewJSONProducer[T any](topic, defaultKey string, producer Publisher) *JSONProducer[T] {
	return &JSONProducer[T]{
		topic:      topic,
		defaultKey: defaultKey,
		producer:   producer,
	}
}

// Produce encodes message as JSON and sends it with the constructor's
// defaultKey.
func (p *JSONProducer[T]) Produce(ctx context.Context, message T) error {
	return p.ProduceWithKey(ctx, p.defaultKey, message)
}

// ProduceWithKey encodes message as JSON and sends it with the given key.
func (p *JSONProducer[T]) ProduceWithKey(_ context.Context, key string, message T) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(message); err != nil {
		return fmt.Errorf("encode message: %w", err)
	}

	if err := p.producer.Produce(p.topic, key, buf.Bytes()); err != nil {
		return fmt.Errorf("produce message: %w", err)
	}

	return nil
}

// Compile-time assertion.
var _ lifecycle.Resource = (*Producer)(nil)
