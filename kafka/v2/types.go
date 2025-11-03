package v2

import (
	"context"
)

// Interfaces for compatibility with existing API

// Producer interface for synchronous producer.
type Producer interface {
	Produce(topic, key string, message []byte) error
}

// ContextProducer interface for producer with context support.
type ContextProducer[T any] interface {
	Produce(ctx context.Context, message T) error
}

// Consumer interface for consumer.
type Consumer interface {
	Run(ctx context.Context) error
}

// MessageProcessor interface for processing messages.
type MessageProcessor interface {
	Process(ctx context.Context, message Message) error
}
