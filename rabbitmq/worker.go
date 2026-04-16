package rabbitmq

import "context"

// Message is the typed payload delivered to a Processor.
type Message[I any] struct {
	Payload I
}

// Processor is what callers implement to handle messages off a queue. The
// returned error decides whether the underlying delivery is Acked
// (nil error) or Nacked (non-nil error).
//
// GetName is used as the AMQP consumer tag and shows up in logs / broker
// admin tools.
type Processor[I any] interface {
	Process(ctx context.Context, msg Message[I]) error
	GetName() string
}
