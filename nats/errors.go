package nats

import "errors"

// Sentinel errors returned by nats constructors and methods. Stable identity
// so callers can errors.Is them.
var (
	// ErrEmptyURL is returned by constructors when Config.URL is empty and
	// no shared connection is provided via WithConnection.
	ErrEmptyURL = errors.New("nats: URL cannot be empty")

	// ErrNilProcessor is returned by NewSubscriber and NewJetStreamConsumer
	// when the Processor argument is nil.
	ErrNilProcessor = errors.New("nats: processor cannot be nil")

	// ErrEmptyStream is returned by NewJetStreamConsumer when the stream
	// name is empty.
	ErrEmptyStream = errors.New("nats: stream name cannot be empty")

	// ErrEmptyDurable is returned by NewJetStreamConsumer when the durable
	// consumer name is empty.
	ErrEmptyDurable = errors.New("nats: durable consumer name cannot be empty")

	// ErrEmptySubjectFilter is returned by NewSubscriber when no subject
	// filter is configured via WithSubscriberSubject.
	ErrEmptySubjectFilter = errors.New("nats: subscriber subject filter cannot be empty")

	// ErrAlreadyRunning is returned by Run when the Runner is already active.
	ErrAlreadyRunning = errors.New("nats: runner already running")

	// ErrInvalidErrorAction is returned when a JSErrorHandler returns an
	// unknown JSErrorAction value.
	ErrInvalidErrorAction = errors.New("nats: invalid JSErrorAction")

	// ErrUnsupportedMessage is returned by the default JSON BodyMarshaller
	// when a Core NATS message's Content-Type header is set to something
	// other than "application/json" or "". JetStream messages without
	// headers are treated as JSON by default. Mirrors rabbitmq behavior.
	ErrUnsupportedMessage = errors.New("nats: unsupported message content type")
)
