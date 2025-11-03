package v2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SyncProducer is a synchronous producer with franz-go.
type SyncProducer struct {
	client  *Client
	timeout time.Duration
}

// NewSyncProducer creates a new synchronous producer.
func NewSyncProducer(brokers string) (*SyncProducer, error) {
	const defaultTimeout = 30 * time.Second

	return NewSyncProducerWithTimeout(brokers, defaultTimeout)
}

// NewSyncProducerWithTimeout creates a producer with given timeout.
func NewSyncProducerWithTimeout(brokers string, timeout time.Duration) (*SyncProducer, error) {
	ctx := context.Background()

	client, err := NewClientFromBrokerString(ctx, brokers, "sync-producer")
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}

	return &SyncProducer{
		client:  client,
		timeout: timeout,
	}, nil
}

// NewSyncProducerFromClient creates a producer from existing client.
func NewSyncProducerFromClient(client *Client, timeout time.Duration) *SyncProducer {
	return &SyncProducer{
		client:  client,
		timeout: timeout,
	}
}

// Produce sends a message synchronously.
func (p *SyncProducer) Produce(topic, key string, message []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	record := &kgo.Record{ //nolint:exhaustruct
		Topic: topic,
		Key:   []byte(key),
		Value: message,
	}

	results := p.client.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce message: %w", err)
	}

	return nil
}

// GetTimeout returns the producer timeout.
func (p *SyncProducer) GetTimeout() time.Duration {
	return p.timeout
}

// Close closes the producer.
func (p *SyncProducer) Close() error {
	if p.client != nil {
		p.client.Close()
	}

	return nil
}

// JSONProducer is a producer for JSON messages.
type JSONProducer[T any] struct {
	topic      string
	defaultKey string
	producer   Producer
}

// NewJSONProducer creates a new JSON producer.
func NewJSONProducer[T any](topic, defaultKey string, producer Producer) *JSONProducer[T] {
	return &JSONProducer[T]{
		topic:      topic,
		defaultKey: defaultKey,
		producer:   producer,
	}
}

// Produce sends JSON message with default key.
func (p *JSONProducer[T]) Produce(ctx context.Context, message T) error {
	return p.ProduceWithKey(ctx, p.defaultKey, message)
}

// ProduceWithKey sends JSON message with specified key.
func (p *JSONProducer[T]) ProduceWithKey(_ context.Context, key string, message T) error {
	var buffer bytes.Buffer

	if err := json.NewEncoder(&buffer).Encode(message); err != nil {
		return fmt.Errorf("encode message: %w", err)
	}

	if err := p.producer.Produce(p.topic, key, buffer.Bytes()); err != nil {
		return fmt.Errorf("produce message: %w", err)
	}

	return nil
}
