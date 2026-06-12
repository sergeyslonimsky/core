// Package nats wraps github.com/nats-io/nats.go for Core NATS and NATS
// JetStream messaging with the lifecycle contract used across core: typed
// Config, functional Options, Shutdown(ctx), and Run(ctx).
//
// The package exposes three primary types:
//
//   - Publisher (Resource) — publishes to Core NATS subjects (at-most-once)
//     or JetStream streams (at-least-once).
//   - Subscriber[I] (Runner) — consumes Core NATS messages with a worker
//     pool and an optional queue group.
//   - JetStreamConsumer[I] (Runner) — pull consumer over a durable JetStream
//     consumer with ack/nak/term/stop error policy.
//
// Connections are established eagerly in the constructors — invalid URLs or
// unreachable brokers surface at boot, not on first publish.
//
// # Options
//
// Three option families are exported:
//
//   - PublisherOption — accepted by NewPublisher.
//   - SubscriberOption — accepted by NewSubscriber.
//   - JSConsumerOption — accepted by NewJetStreamConsumer.
//
// CommonOption (WithLogger, WithConnection, WithNATSOptions, WithOtel)
// implements all three interfaces, so any common option can be passed to any
// constructor without wrapping.
//
// # Error handling
//
// For the Core NATS Subscriber, processor errors are logged and routed
// through an optional SubscriberErrorHandler (set via SetErrorHandler) for
// metrics or DLQ side-effects. There is no ack model.
//
// For JetStreamConsumer, processor errors flow through an optional
// JSErrorHandler[I] (set via SetErrorHandler) returning one of:
//
//   - JSErrorActionNak (default) — redeliver after AckWait.
//   - JSErrorActionNakDelay — redeliver after an explicit delay.
//   - JSErrorActionTerm — discard (poison-pill).
//   - JSErrorActionAck — swallow (DLQ already handled by the caller).
//   - JSErrorActionStop — fatal; Run returns the wrapped error.
//
// # OpenTelemetry
//
// Pass WithOtel() to enable per-message spans. Uses the global TracerProvider
// — register a non-noop provider via otel.SetTracerProvider before
// constructing the publisher or consumer.
//
// # Connection sharing
//
// Pass WithConnection(nc) to reuse an existing *nats.Conn across multiple
// publishers/subscribers/consumers. Components constructed with a shared
// connection do NOT close it on Shutdown.
package nats
