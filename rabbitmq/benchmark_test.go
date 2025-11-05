package rabbitmq_test

import (
	"context"
	"testing"

	"github.com/sergeyslonimsky/core/rabbitmq"
)

// benchmarkMessage is a test message for benchmarks.
type benchmarkMessage struct {
	ID   int    `json:"id"`
	Data string `json:"data"`
}

// BenchmarkPublisher_Publish benchmarks publisher performance.
func BenchmarkPublisher_Publish(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	cfg := rabbitmq.Config{
		Host:     "localhost",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
	}

	publisher := rabbitmq.NewRabbitPublisher(cfg)
	ctx := context.Background()

	// Skip actual connection for benchmark (would need real RabbitMQ)
	b.ResetTimer()

	msg := benchmarkMessage{ID: 1, Data: "benchmark message"}

	for b.Loop() {
		_ = publisher.Publish(ctx, "test-exchange", "test.key", msg, rabbitmq.PublishOpts{
			ContentType: "application/json",
		})
	}
}

// BenchmarkConsumer_NewConsumer benchmarks consumer creation.
func BenchmarkConsumer_NewConsumer(b *testing.B) {
	processor := &mockProcessor{name: "benchmark-processor"}

	b.ResetTimer()

	for b.Loop() {
		_ = rabbitmq.NewConsumer("test-queue", processor)
	}
}
