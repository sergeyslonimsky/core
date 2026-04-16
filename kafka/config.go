// Package kafka wraps github.com/twmb/franz-go (kgo) with the lifecycle
// contract used across core: typed Configs, functional Options, Shutdown
// and Healthcheck methods, and an opt-in OpenTelemetry instrumentation
// pipeline via the kotel plugin.
//
// The package exposes three primary types:
//
//   - Client (Resource) — base kgo.Client wrapper for callers that build
//     their own producer/consumer logic.
//   - Producer (Resource) — synchronous single-record producer.
//   - Consumer (Runner) — consumer group with manual-commit semantics.
//
// All three implement lifecycle.Resource (and Consumer additionally
// implements lifecycle.Runner), so they can be registered with app.App.
package kafka

// Topic is a typed alias for a Kafka topic name. Used in Message and the
// processor surface to keep call sites readable.
type Topic string

// Message is the per-record payload delivered to MessageProcessor.Process.
// The Key and Message byte slices are reused by the underlying fetch loop
// after Process returns — copy them if you need to retain.
type Message struct {
	Topic   Topic
	Key     []byte
	Message []byte
}

// ConsumerConfig describes a consumer group's connection parameters. Plain
// fields, no struct tags — consumer apps map their viper keys to fields
// explicitly inside their own config.NewConfig().
type ConsumerConfig struct {
	// Brokers is the seed broker list.
	Brokers []string

	// Group is the consumer group ID. Required.
	Group string

	// Offset selects the initial offset on first read of a partition:
	// "newest" / "" (default) or "oldest".
	Offset string

	// Topics is the list of topics to subscribe to. Required.
	Topics []string
}

// ProducerConfig describes a synchronous producer. Plain fields, no struct
// tags.
type ProducerConfig struct {
	// Brokers is the seed broker list.
	Brokers []string

	// Topic is the producer's default topic. Used when callers pass an
	// empty topic to Produce; otherwise this is purely informational.
	Topic string
}
