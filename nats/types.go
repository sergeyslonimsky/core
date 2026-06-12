package nats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

// Message is the typed payload delivered to a Processor.
//
// Fields populated for both Core NATS and JetStream:
//   - Payload, Subject, Reply, Headers
//
// Fields populated for JetStream only (zero on Core NATS):
//   - Sequence, NumDelivered, Timestamp
type Message[I any] struct {
	// Payload is the decoded message body.
	Payload I

	// Subject is the NATS subject the message was published to.
	Subject string

	// Reply is the optional reply subject for request-reply patterns.
	// Empty when the publisher did not set one.
	Reply string

	// Headers carries NATS message headers (NATS 2.x+). Nil when no
	// headers were attached.
	Headers nats.Header

	// Sequence is the JetStream stream sequence number. Zero on Core NATS.
	Sequence uint64

	// NumDelivered is the JetStream delivery attempt counter (1-based).
	// Zero on Core NATS.
	NumDelivered uint64

	// Timestamp is the JetStream server-side publish time. Zero on Core NATS.
	Timestamp time.Time
}

// Processor is what callers implement to handle messages.
//
// For Core NATS Subscriber, the returned error is passed to the configured
// SubscriberErrorHandler (or logged at warn level if none is set). There is
// no ack model — the error is observational only.
//
// For JetStreamConsumer, the returned error is passed to the configured
// JSErrorHandler[I] which decides whether the message is Acked, Nacked,
// Term'd, or whether the consumer should stop. Without a custom handler the
// default is Nak (redeliver after AckWait).
type Processor[I any] interface {
	Process(ctx context.Context, msg Message[I]) error
}

// BodyMarshaller decodes a raw message body into a typed payload.
// Install via Subscriber.SetMarshaller or JetStreamConsumer.SetMarshaller.
// Default behavior (no custom marshaller) is json.Unmarshal.
type BodyMarshaller[I any] func(body []byte, payload *I) error

// SubscriberErrorHandler is invoked when Core NATS message processing fails.
// Use it for metrics, DLQ side-effects, or structured logging. The returned
// value is ignored — Core NATS has no ack model.
type SubscriberErrorHandler[I any] func(ctx context.Context, msg Message[I], err error)

// JSErrorAction selects what JetStreamConsumer should do after a Processor
// returns a non-nil error. Returned by JSErrorHandler.
type JSErrorAction int

const (
	// JSErrorActionNak negative-acks the message with no delay. JetStream
	// will redeliver after the consumer's AckWait. Default when no
	// JSErrorHandler is configured.
	JSErrorActionNak JSErrorAction = iota

	// JSErrorActionNakDelay negative-acks with an explicit redelivery delay.
	// The delay is the second return value of JSErrorHandler.
	JSErrorActionNakDelay

	// JSErrorActionTerm marks the message as a terminal failure: JetStream
	// will NOT redeliver and the delivery count is finalized. Use for
	// poison-pill records after exhausting in-process retries.
	JSErrorActionTerm

	// JSErrorActionAck acks the failing message (swallow). Use when the
	// processor has already shipped the payload to a DLQ topic and the
	// stream should advance.
	JSErrorActionAck

	// JSErrorActionStop terminates JetStreamConsumer: Run returns the
	// original processing error wrapped. The message is NOT acked. Use for
	// fatal conditions where the service should fail loudly rather than
	// silently advance.
	JSErrorActionStop
)

// JSErrorHandler is invoked when JetStreamConsumer's Processor returns a
// non-nil error. It decides what the consumer should do with the failing
// message.
//
// The returned time.Duration is used only when the action is
// JSErrorActionNakDelay; it is ignored otherwise.
//
// Implementations typically:
//   - Emit to a DLQ and return (JSErrorActionAck, 0).
//   - Increment a metric and return (JSErrorActionNak, 0) for transient errors.
//   - Return (JSErrorActionTerm, 0) for poison-pill records.
//   - Return (JSErrorActionStop, 0) for fatal dependency outages.
type JSErrorHandler[I any] func(ctx context.Context, msg Message[I], err error) (JSErrorAction, time.Duration)
