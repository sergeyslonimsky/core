package v2_test

import (
	"context"
	"testing"
	"time"

	v2 "github.com/sergeyslonimsky/core/kafka/v2"
)

// BenchmarkProducer_Produce benchmarks franz-go producer performance.
func BenchmarkProducer_Produce(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark test")
	}

	producer, err := v2.NewSyncProducerWithTimeout("localhost:9092", 5*time.Second)
	if err != nil {
		b.Skipf("Cannot create producer: %v", err)
	}
	defer producer.Close()

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

	producer, err := v2.NewSyncProducerWithTimeout("localhost:9092", 5*time.Second)
	if err != nil {
		b.Skipf("Cannot create producer: %v", err)
	}
	defer producer.Close()

	const testTopic = "benchmark-json-topic"

	jsonProducer := v2.NewJSONProducer[BenchmarkMessage](testTopic, "bench-key", producer)
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

	for b.Loop() {
		producer, err := v2.NewSyncProducer("localhost:9092")
		if err != nil {
			b.Skip("Kafka not available")
		}

		if producer != nil {
			producer.Close()
		}
	}
}

// BenchmarkProducer_ConcurrentProduce benchmarks concurrent production.
func BenchmarkProducer_ConcurrentProduce(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark test")
	}

	producer, err := v2.NewSyncProducerWithTimeout("localhost:9092", 5*time.Second)
	if err != nil {
		b.Skipf("Cannot create producer: %v", err)
	}
	defer producer.Close()

	const (
		testTopic = "benchmark-concurrent-topic"
		testKey   = "concurrent-key"
	)

	message := []byte("concurrent benchmark message")

	b.ResetTimer()
	b.SetParallelism(10) // 10 concurrent goroutines
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := producer.Produce(testTopic, testKey, message); err != nil {
				b.Errorf("concurrent produce failed: %v", err)
			}
		}
	})
}

// simpleProcessor is a minimal processor for benchmarking.
type simpleProcessor struct {
	count int
}

func (p *simpleProcessor) Process(_ context.Context, _ v2.Message) error {
	p.count++

	return nil
}

// BenchmarkConsumer_Consume benchmarks consumer performance.
func BenchmarkConsumer_Consume(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark test")
	}

	const (
		testTopic = "benchmark-consumer-topic"
		brokers   = "localhost:9092"
	)

	// Produce messages first
	producer, err := v2.NewSyncProducerWithTimeout(brokers, 5*time.Second)
	if err != nil {
		b.Skipf("Cannot create producer: %v", err)
	}
	defer producer.Close()

	for range b.N {
		err = producer.Produce(testTopic, "bench-key", []byte("benchmark message"))
		if err != nil {
			b.Fatalf("produce failed: %v", err)
		}
	}

	// Benchmark consumer
	processor := &simpleProcessor{}
	cfg := v2.ConsumerConfig{
		Brokers: brokers,
		Group:   "benchmark-group",
		Offset:  "oldest",
		Topics:  []string{testTopic},
	}

	consumer, err := v2.NewConsumer(cfg, processor)
	if err != nil {
		b.Fatalf("failed to create consumer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b.ResetTimer()

	go func() {
		_ = consumer.Run(ctx)
	}()

	// Wait for all messages to be consumed
	for processor.count < b.N {
		time.Sleep(10 * time.Millisecond)
	}

	b.StopTimer()
	cancel()
}

// BenchmarkConsumer_HighThroughput benchmarks consumer with high message volume.
func BenchmarkConsumer_HighThroughput(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark test")
	}

	const (
		testTopic    = "benchmark-throughput-topic"
		brokers      = "localhost:9092"
		messageCount = 1000
	)

	// Produce messages first
	producer, err := v2.NewSyncProducerWithTimeout(brokers, 5*time.Second)
	if err != nil {
		b.Skipf("Cannot create producer: %v", err)
	}
	defer producer.Close()

	for range messageCount {
		err = producer.Produce(testTopic, "bench-key", []byte("high throughput message"))
		if err != nil {
			b.Fatalf("produce failed: %v", err)
		}
	}

	b.ResetTimer()

	for b.Loop() {
		processor := &simpleProcessor{}
		cfg := v2.ConsumerConfig{
			Brokers: brokers,
			Group:   "benchmark-throughput-group",
			Offset:  "oldest",
			Topics:  []string{testTopic},
		}

		consumer, err := v2.NewConsumer(cfg, processor)
		if err != nil {
			b.Fatalf("failed to create consumer: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		go func() {
			_ = consumer.Run(ctx)
		}()

		// Wait for all messages
		for processor.count < messageCount {
			time.Sleep(10 * time.Millisecond)
		}

		cancel()
	}
}
