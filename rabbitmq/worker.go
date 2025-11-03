package rabbitmq

import "context"

type Message[I any] struct {
	Payload I
}

type Processor[I any] interface {
	Process(ctx context.Context, msg Message[I]) error
	GetName() string
}
