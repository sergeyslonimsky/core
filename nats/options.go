package nats

import (
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// -----------------------------------------------------------------------------
// Common options
// -----------------------------------------------------------------------------

// commonOptions are configuration knobs shared between Publisher, Subscriber,
// and JetStreamConsumer.
type commonOptions struct {
	logger      *slog.Logger
	conn        *nats.Conn
	extraNATS   []nats.Option
	otelEnabled bool
}

func defaultCommonOptions() commonOptions {
	return commonOptions{
		logger:      slog.Default(),
		conn:        nil,
		extraNATS:   nil,
		otelEnabled: false,
	}
}

// CommonOption configures any nats constructor (NewPublisher, NewSubscriber,
// NewJetStreamConsumer). All CommonOption values are simultaneously
// PublisherOption, SubscriberOption, and JSConsumerOption.
type CommonOption interface {
	PublisherOption
	SubscriberOption
	JSConsumerOption
}

// commonOption is the concrete type backing every shared option helper. It
// stores a single mutator that runs against commonOptions and satisfies the
// three apply* interfaces.
type commonOption struct {
	apply func(*commonOptions)
}

func (c commonOption) applyToPublisher(o *publisherOptions)   { c.apply(&o.common) }
func (c commonOption) applyToSubscriber(o *subscriberOptions) { c.apply(&o.common) }
func (c commonOption) applyToJSConsumer(o *jsConsumerOptions) { c.apply(&o.common) }

// WithLogger attaches a *slog.Logger used for lifecycle and per-message
// error logging. Defaults to slog.Default() when omitted. Nil values are
// ignored.
func WithLogger(l *slog.Logger) CommonOption {
	return commonOption{apply: func(o *commonOptions) {
		if l != nil {
			o.logger = l
		}
	}}
}

// WithConnection injects a pre-existing *nats.Conn. When set, the constructor
// skips dialing — Config.URL/User/Password/Token/CredsFile and the WithNATSOptions
// extras are ignored — and Shutdown will NOT close the connection (the caller
// retains ownership).
//
// Use to share a single connection across a Publisher + Subscriber +
// JetStreamConsumer in the same process.
func WithConnection(nc *nats.Conn) CommonOption {
	return commonOption{apply: func(o *commonOptions) {
		o.conn = nc
	}}
}

// WithNATSOptions appends raw nats.Option values to the connect call. Escape
// hatch for TLS, NKey, JWT, custom inbox prefixes, NoEcho, custom error
// handlers, and anything else not exposed via Config.
//
// Applied after the options derived from Config so callers can override
// defaults (e.g., a different reconnect handler).
//
// Ignored when WithConnection is set.
func WithNATSOptions(opts ...nats.Option) CommonOption {
	return commonOption{apply: func(o *commonOptions) {
		o.extraNATS = append(o.extraNATS, opts...)
	}}
}

// WithOtel enables OpenTelemetry instrumentation: one span per
// publish/consume operation with messaging.* attributes. Uses the global
// tracer provider — register a non-noop provider before constructing the
// publisher/consumer.
func WithOtel() CommonOption {
	return commonOption{apply: func(o *commonOptions) {
		o.otelEnabled = true
	}}
}

// -----------------------------------------------------------------------------
// Publisher options
// -----------------------------------------------------------------------------

type publisherOptions struct {
	common       commonOptions
	streamConfig *jetstream.StreamConfig
}

func defaultPublisherOptions() publisherOptions {
	return publisherOptions{
		common:       defaultCommonOptions(),
		streamConfig: nil,
	}
}

// PublisherOption configures NewPublisher. Any CommonOption is also a
// PublisherOption.
type PublisherOption interface {
	applyToPublisher(o *publisherOptions)
}

type publisherOptionFunc func(*publisherOptions)

func (f publisherOptionFunc) applyToPublisher(o *publisherOptions) { f(o) }

// WithPublisherStream instructs NewPublisher to verify or create/update a
// JetStream stream with the given configuration at construction time. The
// constructor returns an error if the stream cannot be declared.
func WithPublisherStream(cfg jetstream.StreamConfig) PublisherOption {
	return publisherOptionFunc(func(o *publisherOptions) {
		o.streamConfig = new(cfg)
	})
}

// -----------------------------------------------------------------------------
// Subscriber options
// -----------------------------------------------------------------------------

type subscriberOptions struct {
	common        commonOptions
	queueGroup    string
	workerCount   int
	channelBuffer int
}

// defaultSubscriberChannelBuffer is the default capacity of the internal
// ChanSubscribe channel.
const defaultSubscriberChannelBuffer = 256

func defaultSubscriberOptions() subscriberOptions {
	return subscriberOptions{
		common:        defaultCommonOptions(),
		queueGroup:    "",
		workerCount:   1,
		channelBuffer: defaultSubscriberChannelBuffer,
	}
}

// SubscriberOption configures NewSubscriber. Any CommonOption is also a
// SubscriberOption.
//
// Payload-typed knobs (Marshaller, ErrorHandler) live on SubscriberHandlers,
// passed as a positional argument to NewSubscriber.
type SubscriberOption interface {
	applyToSubscriber(o *subscriberOptions)
}

type subscriberOptionFunc func(*subscriberOptions)

func (f subscriberOptionFunc) applyToSubscriber(o *subscriberOptions) { f(o) }

// WithSubscriberQueueGroup configures a queue group for Core NATS subscriber
// load balancing. Subscribers in the same queue group share message load.
func WithSubscriberQueueGroup(group string) SubscriberOption {
	return subscriberOptionFunc(func(o *subscriberOptions) {
		o.queueGroup = group
	})
}

// WithSubscriberWorkerCount sets the number of concurrent goroutines
// processing deliveries. Default 1 (ordered processing). Values < 1 are
// normalized to 1.
//
// Raising above 1 increases throughput at the cost of losing per-subject
// processing order.
func WithSubscriberWorkerCount(n int) SubscriberOption {
	return subscriberOptionFunc(func(o *subscriberOptions) {
		if n < 1 {
			n = 1
		}

		o.workerCount = n
	})
}

// WithSubscriberChannelBuffer sets the capacity of the internal
// ChanSubscribe channel used between the NATS client reader and the worker
// pool. Default 256.
//
// Tune up for high-throughput Subscribers if you see SlowConsumer warnings
// in the async error log (raises memory usage during bursts). Values < 1
// fall back to the default.
func WithSubscriberChannelBuffer(n int) SubscriberOption {
	return subscriberOptionFunc(func(o *subscriberOptions) {
		if n < 1 {
			n = defaultSubscriberChannelBuffer
		}

		o.channelBuffer = n
	})
}

// -----------------------------------------------------------------------------
// JetStream consumer options
// -----------------------------------------------------------------------------

type jsConsumerOptions struct {
	common             commonOptions
	streamConfig       *jetstream.StreamConfig
	subjectFilters     []string
	consumerMutate     func(*jetstream.ConsumerConfig)
	workerCount        int
	deliveryBuffer     int
	assumeExistingOnly bool
}

// defaultJSConsumerDeliveryBuffer is the default capacity of the channel
// between the JetStream Consume callback and the worker pool.
const defaultJSConsumerDeliveryBuffer = 256

func defaultJSConsumerOptions() jsConsumerOptions {
	return jsConsumerOptions{
		common:             defaultCommonOptions(),
		streamConfig:       nil,
		subjectFilters:     nil,
		consumerMutate:     nil,
		workerCount:        1,
		deliveryBuffer:     defaultJSConsumerDeliveryBuffer,
		assumeExistingOnly: false,
	}
}

// JSConsumerOption configures NewJetStreamConsumer. Any CommonOption is also
// a JSConsumerOption.
//
// Payload-typed knobs (Marshaller, ErrorHandler) live on JSConsumerHandlers,
// passed as a positional argument to NewJetStreamConsumer.
type JSConsumerOption interface {
	applyToJSConsumer(o *jsConsumerOptions)
}

type jsConsumerOptionFunc func(*jsConsumerOptions)

func (f jsConsumerOptionFunc) applyToJSConsumer(o *jsConsumerOptions) { f(o) }

// WithJSStream instructs NewJetStreamConsumer to verify or create/update a
// JetStream stream with the given configuration at construction time.
func WithJSStream(cfg jetstream.StreamConfig) JSConsumerOption {
	return jsConsumerOptionFunc(func(o *jsConsumerOptions) {
		o.streamConfig = new(cfg)
	})
}

// WithJSSubjects sets one or more filter subjects for the durable consumer.
// Single subject populates ConsumerConfig.FilterSubject; multiple subjects
// populate ConsumerConfig.FilterSubjects.
func WithJSSubjects(subjects ...string) JSConsumerOption {
	return jsConsumerOptionFunc(func(o *jsConsumerOptions) {
		o.subjectFilters = append([]string{}, subjects...)
	})
}

// WithJSConsumerConfig installs a mutator that runs against the
// pre-populated jetstream.ConsumerConfig before CreateOrUpdateConsumer.
//
// The mutator receives a config already populated with Durable, AckPolicy
// (AckExplicit), and the configured FilterSubject(s). Mutate only what you
// need:
//
//	corenats.WithJSConsumerConfig(func(cfg *jetstream.ConsumerConfig) {
//	    cfg.MaxDeliver = 5
//	    cfg.AckWait = 30 * time.Second
//	})
//
// This avoids accidentally clobbering defaults (e.g., setting AckPolicy back
// to AckNone by passing a zero-value struct).
func WithJSConsumerConfig(mutate func(*jetstream.ConsumerConfig)) JSConsumerOption {
	return jsConsumerOptionFunc(func(o *jsConsumerOptions) {
		o.consumerMutate = mutate
	})
}

// WithJSWorkerCount sets the number of concurrent goroutines processing
// JetStream deliveries. Default 1 (preserves per-subject order). Values < 1
// are normalized to 1.
func WithJSWorkerCount(n int) JSConsumerOption {
	return jsConsumerOptionFunc(func(o *jsConsumerOptions) {
		if n < 1 {
			n = 1
		}

		o.workerCount = n
	})
}

// WithJSDeliveryBuffer sets the capacity of the internal channel between
// the JetStream Consume callback and the worker pool. Default 256. Values
// < 1 fall back to the default.
func WithJSDeliveryBuffer(n int) JSConsumerOption {
	return jsConsumerOptionFunc(func(o *jsConsumerOptions) {
		if n < 1 {
			n = defaultJSConsumerDeliveryBuffer
		}

		o.deliveryBuffer = n
	})
}

// WithJSAssumeExistingConsumer instructs NewJetStreamConsumer to look up
// the existing durable consumer (via js.Consumer) instead of creating or
// updating one. The constructor fails if the consumer does not exist.
//
// Use to avoid silent configuration drift in environments where the
// JetStream consumer is managed externally (Terraform, CLI, ops tooling).
// When set, WithJSConsumerConfig and WithJSSubjects are ignored.
func WithJSAssumeExistingConsumer() JSConsumerOption {
	return jsConsumerOptionFunc(func(o *jsConsumerOptions) {
		o.assumeExistingOnly = true
	})
}
