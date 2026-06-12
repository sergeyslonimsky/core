package nats_test

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"

	corenats "github.com/sergeyslonimsky/core/nats"
)

// benchmarkMessage is a small JSON payload used by the JSON benchmarks.
type benchmarkMessage struct {
	ID        int    `json:"id"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// startBenchNATS boots an in-process NATS server (optionally with JetStream)
// on a random free port and returns its client URL. Used by all benchmarks
// here so the suite has no external broker dependency.
func startBenchNATS(b *testing.B, withJetStream bool) string {
	b.Helper()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		StoreDir:  b.TempDir(),
		JetStream: withJetStream,
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		b.Fatalf("create in-process NATS server: %v", err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		b.Fatal("in-process NATS server failed to become ready")
	}

	b.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	return ns.ClientURL()
}

// BenchmarkPublisher_Publish measures raw Core NATS publish throughput
// (no JSON encoding, no headers, no OTel).
func BenchmarkPublisher_Publish(b *testing.B) {
	url := startBenchNATS(b, false)

	publisher, err := corenats.NewPublisher(b.Context(), corenats.Config{URL: url})
	if err != nil {
		b.Fatalf("create publisher: %v", err)
	}

	b.Cleanup(func() { _ = publisher.Shutdown(context.Background()) })

	const subject = "bench.core.publish"

	payload := []byte("benchmark payload")
	ctx := b.Context()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := publisher.Publish(ctx, subject, payload); err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
}

// BenchmarkPublisher_PublishJSON measures Core NATS publish with JSON
// encoding and the single-clone Content-Type injection path.
func BenchmarkPublisher_PublishJSON(b *testing.B) {
	url := startBenchNATS(b, false)

	publisher, err := corenats.NewPublisher(b.Context(), corenats.Config{URL: url})
	if err != nil {
		b.Fatalf("create publisher: %v", err)
	}

	b.Cleanup(func() { _ = publisher.Shutdown(context.Background()) })

	const subject = "bench.core.json"

	ctx := b.Context()

	b.ReportAllocs()
	b.ResetTimer()

	id := 0
	for b.Loop() {
		id++

		msg := benchmarkMessage{ID: id, Message: "x", Timestamp: 0}
		if err := publisher.PublishJSON(ctx, subject, msg); err != nil {
			b.Fatalf("publish JSON: %v", err)
		}
	}
}

// BenchmarkPublisher_PublishWithOtel measures the OTel-enabled publish
// path: header clone + trace-context injection + per-call span. With a
// no-op TracerProvider the propagator still runs its inject step.
func BenchmarkPublisher_PublishWithOtel(b *testing.B) {
	url := startBenchNATS(b, false)

	publisher, err := corenats.NewPublisher(
		b.Context(),
		corenats.Config{URL: url},
		corenats.WithOtel(),
	)
	if err != nil {
		b.Fatalf("create publisher: %v", err)
	}

	b.Cleanup(func() { _ = publisher.Shutdown(context.Background()) })

	const subject = "bench.core.otel"

	payload := []byte("benchmark payload")
	ctx := b.Context()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := publisher.Publish(ctx, subject, payload); err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
}

// BenchmarkPublisher_PublishJS measures JetStream publish throughput
// (synchronous PubAck round-trip).
func BenchmarkPublisher_PublishJS(b *testing.B) {
	url := startBenchNATS(b, true)

	const (
		stream  = "BENCH_JS"
		subject = "bench.js"
	)

	publisher, err := corenats.NewPublisher(
		b.Context(),
		corenats.Config{URL: url},
		corenats.WithPublisherStream(jetstream.StreamConfig{
			Name:     stream,
			Subjects: []string{subject},
			Storage:  jetstream.MemoryStorage,
		}),
	)
	if err != nil {
		b.Fatalf("create publisher: %v", err)
	}

	b.Cleanup(func() { _ = publisher.Shutdown(context.Background()) })

	payload := []byte("benchmark payload")
	ctx := b.Context()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if _, err := publisher.PublishJS(ctx, subject, payload); err != nil {
			b.Fatalf("publish JS: %v", err)
		}
	}
}

// BenchmarkJSONPublisher_Publish measures the typed JSONPublisher[T]
// wrapper — same hot path as PublishJSON plus generic dispatch.
func BenchmarkJSONPublisher_Publish(b *testing.B) {
	url := startBenchNATS(b, false)

	publisher, err := corenats.NewPublisher(b.Context(), corenats.Config{URL: url})
	if err != nil {
		b.Fatalf("create publisher: %v", err)
	}

	b.Cleanup(func() { _ = publisher.Shutdown(context.Background()) })

	const subject = "bench.jp.evt"

	jp := corenats.NewJSONPublisher[benchmarkMessage](publisher, subject)
	ctx := b.Context()

	b.ReportAllocs()
	b.ResetTimer()

	id := 0
	for b.Loop() {
		id++

		msg := benchmarkMessage{ID: id, Message: "x", Timestamp: 0}
		if err := jp.Publish(ctx, msg); err != nil {
			b.Fatalf("publish JSON typed: %v", err)
		}
	}
}
