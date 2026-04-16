package rabbitmq_test

import (
	"testing"

	"github.com/sergeyslonimsky/core/rabbitmq"
)

// BenchmarkConsumer_NewConsumer benchmarks consumer construction (no I/O).
func BenchmarkConsumer_NewConsumer(b *testing.B) {
	processor := &mockProcessor{name: "benchmark-processor"}

	b.ResetTimer()

	for b.Loop() {
		_ = rabbitmq.NewConsumer("test-queue", processor)
	}
}

// Publisher benchmarks would require a live RabbitMQ broker because
// NewPublisher dials in the constructor. See e2e_integration_test.go for
// publish-path benchmarks under the integration build tag.
