package kafka

import (
	"context"
)

// Publisher is the minimal produce-side interface implemented by *Producer.
// JSONProducer accepts any Publisher so callers can swap in fakes for tests.
//
// Named Publisher (not Producer) to avoid collision with the *Producer struct.
type Publisher interface {
	Produce(topic, key string, message []byte) error
}

// ContextPublisher is a typed publisher (e.g., JSONProducer[T]) that takes
// a typed message and a context.
type ContextPublisher[T any] interface {
	Produce(ctx context.Context, message T) error
}

// MessageProcessor is what callers implement to handle messages delivered
// by a Consumer. Process returns nil on success; a non-nil error is logged
// but does NOT halt the consumer.
type MessageProcessor interface {
	Process(ctx context.Context, message Message) error
}
