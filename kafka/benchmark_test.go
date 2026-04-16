package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/sergeyslonimsky/core/kafka"
)

// BenchmarkProducer_Produce benchmarks franz-go producer performance.
func BenchmarkProducer_Produce(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark test")
	}

	producer, err := kafka.NewProducer(
		kafka.ProducerConfig{Brokers: []string{"localhost:9092"}},
		kafka.WithProduceTimeout(5*time.Second),
	)
	if err != nil {
		b.Skipf("Cannot create producer: %v", err)
	}
	defer producer.Shutdown(context.Background()) //nolint:errcheck

	const (
		testTopic = "benchmark-topic"
		testKey   = "benchmark-key"
	)

	message := []byte("benchmark message for producer")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := producer.Produce(testTopic, testKey, message); err != nil {
				b.Errorf("produce failed: %v", err)
			}
		}
	})
}

// BenchmarkJSONProducer_Produce benchmarks JSON producer performance.
func BenchmarkJSONProducer_Produce(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark test")
	}

	type BenchmarkMessage struct {
		ID        int    `json:"id"`
		Message   string `json:"message"`
		Timestamp int64  `json:"timestamp"`
	}

	producer, err := kafka.NewProducer(
		kafka.ProducerConfig{Brokers: []string{"localhost:9092"}},
		kafka.WithProduceTimeout(5*time.Second),
	)
	if err != nil {
		b.Skipf("Cannot create producer: %v", err)
	}
	defer producer.Shutdown(context.Background()) //nolint:errcheck

	const testTopic = "benchmark-json-topic"

	jsonProducer := kafka.NewJSONProducer[BenchmarkMessage](testTopic, "bench-key", producer)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := 0
		for pb.Next() {
			id++
			message := BenchmarkMessage{
				ID:        id,
				Message:   "Benchmark message",
				Timestamp: time.Now().Unix(),
			}

			if err := jsonProducer.Produce(ctx, message); err != nil {
				b.Errorf("JSON produce failed: %v", err)
			}
		}
	})
}

// BenchmarkProducer_Memory benchmarks producer memory usage.
func BenchmarkProducer_Memory(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark test")
	}

	b.ReportAllocs()

	ctx := context.Background()

	for b.Loop() {
		producer, err := kafka.NewProducer(
			kafka.ProducerConfig{Brokers: []string{"localhost:9092"}},
		)
		if err != nil {
			b.Skip("Kafka not available")
		}

		if producer != nil {
			_ = producer.Shutdown(ctx)
		}
	}
}

// simpleProcessor is a minimal processor for benchmarking.
type simpleProcessor struct {
	count int
}

func (p *simpleProcessor) Process(_ context.Context, _ kafka.Message) error {
	p.count++

	return nil
}

// BenchmarkConsumer_Consume benchmarks consumer performance.
func BenchmarkConsumer_Consume(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark test")
	}

	const testTopic = "benchmark-consumer-topic"

	brokers := []string{"localhost:9092"}

	producer, err := kafka.NewProducer(
		kafka.ProducerConfig{Brokers: brokers},
		kafka.WithProduceTimeout(5*time.Second),
	)
	if err != nil {
		b.Skipf("Cannot create producer: %v", err)
	}
	defer producer.Shutdown(context.Background()) //nolint:errcheck

	for range b.N {
		if err = producer.Produce(testTopic, "bench-key", []byte("benchmark message")); err != nil {
			b.Fatalf("produce failed: %v", err)
		}
	}

	processor := &simpleProcessor{}
	cfg := kafka.ConsumerConfig{
		Brokers: brokers,
		Group:   "benchmark-group",
		Offset:  "oldest",
		Topics:  []string{testTopic},
	}

	consumer, err := kafka.NewConsumer(cfg, processor)
	if err != nil {
		b.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Shutdown(context.Background()) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b.ResetTimer()

	go func() { _ = consumer.Run(ctx) }()

	for processor.count < b.N {
		time.Sleep(10 * time.Millisecond)
	}

	b.StopTimer()
	cancel()
}
