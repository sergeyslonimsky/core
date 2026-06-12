package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"

	"github.com/sergeyslonimsky/core/lifecycle"
)

// PublishOpts captures per-publish Core NATS message metadata. Headers are
// optional; an empty Reply suppresses the reply subject.
type PublishOpts struct {
	// Headers are NATS message headers (NATS 2.x+).
	Headers nats.Header

	// Reply is an optional reply subject for request-reply patterns.
	Reply string

	// setJSONContentType is set by the internal withJSONContentType option
	// to signal that Publish should clone headers and add a JSON
	// Content-Type — handled in the publish path rather than at option-apply
	// time so we mutate headers exactly once.
	setJSONContentType bool
}

// PublishOption configures a single Publish/PublishJSON call.
type PublishOption func(*PublishOpts)

// WithHeaders attaches NATS headers to the published message.
func WithHeaders(h nats.Header) PublishOption {
	return func(o *PublishOpts) { o.Headers = h }
}

// WithReply sets the reply subject for request-reply patterns.
func WithReply(subject string) PublishOption {
	return func(o *PublishOpts) { o.Reply = subject }
}

// JSPublishOpts captures per-publish JetStream metadata. Use the helper
// option functions (WithJSHeaders, WithJSMsgID, etc.) to populate it.
type JSPublishOpts struct {
	Headers                nats.Header
	MsgID                  string
	ExpectedStream         string
	ExpectedLastSequence   *uint64
	ExpectedLastSubjectSeq *uint64
	ExpectedLastMsgID      string
	RetryAttempts          int

	// setJSONContentType — see PublishOpts.setJSONContentType.
	setJSONContentType bool
}

// JSPublishOption configures a single PublishJS/PublishJSONJS call.
type JSPublishOption func(*JSPublishOpts)

// WithJSHeaders attaches NATS headers to the published JetStream message.
func WithJSHeaders(h nats.Header) JSPublishOption {
	return func(o *JSPublishOpts) { o.Headers = h }
}

// WithJSMsgID sets the JetStream message ID used for server-side
// deduplication. Required for exactly-once-style publishing.
func WithJSMsgID(id string) JSPublishOption {
	return func(o *JSPublishOpts) { o.MsgID = id }
}

// WithJSExpectedStream rejects the publish if the target stream does not
// match the given name. Useful when multiple streams could route the same
// subject.
func WithJSExpectedStream(name string) JSPublishOption {
	return func(o *JSPublishOpts) { o.ExpectedStream = name }
}

// WithJSExpectedLastSequence enforces optimistic-concurrency control: the
// publish is rejected unless the stream's last sequence matches seq.
func WithJSExpectedLastSequence(seq uint64) JSPublishOption {
	return func(o *JSPublishOpts) { o.ExpectedLastSequence = &seq }
}

// WithJSExpectedLastSubjectSequence enforces per-subject optimistic
// concurrency control.
func WithJSExpectedLastSubjectSequence(seq uint64) JSPublishOption {
	return func(o *JSPublishOpts) { o.ExpectedLastSubjectSeq = &seq }
}

// WithJSExpectedLastMsgID enforces a precondition that the previous message
// in the stream had the given MsgID.
func WithJSExpectedLastMsgID(id string) JSPublishOption {
	return func(o *JSPublishOpts) { o.ExpectedLastMsgID = id }
}

// WithJSRetryAttempts overrides the underlying jetstream client's retry
// attempt count for the publish. Values < 1 are ignored.
func WithJSRetryAttempts(n int) JSPublishOption {
	return func(o *JSPublishOpts) {
		if n > 0 {
			o.RetryAttempts = n
		}
	}
}

// Publisher publishes messages to Core NATS subjects or JetStream streams.
// Implements lifecycle.Resource.
//
// Construct via NewPublisher. The publisher establishes its NATS connection
// (and optional JetStream context + stream declaration) eagerly in the
// constructor.
//
// When constructed with WithOtel(), every Publish*/Publish*JS call opens a
// SpanKindProducer span and injects the trace context into the outgoing
// nats.Header via the global TextMapPropagator. The consumer-side span
// (Subscriber / JetStreamConsumer) extracts the context so the producer
// and consumer spans chain.
type Publisher struct {
	nc             *nats.Conn
	js             jetstream.JetStream
	ownsConnection bool
	logger         *slog.Logger
	otelEnabled    bool
	shutdownOnce   sync.Once
	shutdownErr    error
}

// NewPublisher creates a Publisher. By default it dials NATS using cfg;
// pass WithConnection to share an existing connection.
//
// If WithPublisherStream is provided, the stream is verified or
// created/updated during the constructor — a non-nil error is returned on
// failure.
func NewPublisher(ctx context.Context, cfg Config, opts ...PublisherOption) (*Publisher, error) {
	o := defaultPublisherOptions()
	for _, apply := range opts {
		apply.applyToPublisher(&o)
	}

	nc, ownsConnection, err := acquireConn(cfg, o.common)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		closeOwned(nc, ownsConnection)

		return nil, fmt.Errorf("initialize JetStream: %w", err)
	}

	if o.streamConfig != nil {
		if _, err = js.CreateOrUpdateStream(ctx, *o.streamConfig); err != nil {
			closeOwned(nc, ownsConnection)

			return nil, fmt.Errorf("declare stream %s: %w", o.streamConfig.Name, err)
		}
	}

	return &Publisher{ //nolint:exhaustruct
		nc:             nc,
		js:             js,
		ownsConnection: ownsConnection,
		logger:         o.common.logger,
		otelEnabled:    o.common.otelEnabled,
	}, nil
}

// Publish sends raw bytes via Core NATS (at-most-once delivery). The
// message is buffered by the client and flushed asynchronously — call Flush
// before Shutdown if you must guarantee transmission.
//
// ctx is only used for OTel span context; the underlying nats.Conn.PublishMsg
// call is non-blocking and does not honor cancellation.
func (p *Publisher) Publish(ctx context.Context, subject string, data []byte, opts ...PublishOption) error {
	o := PublishOpts{} //nolint:exhaustruct
	for _, apply := range opts {
		apply(&o)
	}

	_, msg, span := p.prepareMsg(ctx, subject, o.Reply, data, o.Headers, o.setJSONContentType)

	err := p.nc.PublishMsg(msg)

	if span != nil {
		endPublishSpan(span, err)
	}

	if err != nil {
		return fmt.Errorf("publish to subject %s: %w", subject, err)
	}

	return nil
}

// PublishJSON marshals message to JSON and publishes it via Core NATS.
// Sets the Content-Type header to application/json so the consumer-side
// default decoder accepts it without needing a custom marshaller.
func (p *Publisher) PublishJSON(ctx context.Context, subject string, message any, opts ...PublishOption) error {
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	opts = append(opts, withJSONContentType())

	return p.Publish(ctx, subject, body, opts...)
}

// withJSONContentType is an internal helper that flags Publish to add the
// application/json Content-Type. The actual mutation runs in Publish so
// headers are cloned exactly once (along with any otel trace-context
// injection) rather than once per option helper.
func withJSONContentType() PublishOption {
	return func(o *PublishOpts) { o.setJSONContentType = true }
}

// PublishJS sends raw bytes via JetStream (at-least-once delivery). The
// returned PubAck contains the assigned stream sequence and duplicate flag.
func (p *Publisher) PublishJS(
	ctx context.Context,
	subject string,
	data []byte,
	opts ...JSPublishOption,
) (*jetstream.PubAck, error) {
	o := JSPublishOpts{} //nolint:exhaustruct
	for _, apply := range opts {
		apply(&o)
	}

	ctx, msg, span := p.prepareMsg(ctx, subject, "", data, o.Headers, o.setJSONContentType)
	publishOpts := buildJSPublishOpts(o)

	ack, err := p.js.PublishMsg(ctx, msg, publishOpts...)

	if span != nil {
		endPublishSpan(span, err)
	}

	if err != nil {
		return nil, fmt.Errorf("publish JS to subject %s: %w", subject, err)
	}

	return ack, nil
}

// PublishJSONJS marshals message to JSON and publishes it via JetStream.
// Sets Content-Type=application/json on the outgoing message.
func (p *Publisher) PublishJSONJS(
	ctx context.Context,
	subject string,
	message any,
	opts ...JSPublishOption,
) (*jetstream.PubAck, error) {
	body, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}

	opts = append(opts, withJSONContentTypeJS())

	return p.PublishJS(ctx, subject, body, opts...)
}

// withJSONContentTypeJS is the JetStream-side counterpart of
// withJSONContentType — flags PublishJS to add Content-Type during the
// single clone pass.
func withJSONContentTypeJS() JSPublishOption {
	return func(o *JSPublishOpts) { o.setJSONContentType = true }
}

// cloneHeader returns a deep copy of h with shared backing-slice safety.
// Nil input returns a fresh empty Header.
func cloneHeader(h nats.Header) nats.Header {
	out := make(nats.Header, len(h))
	for k, v := range h {
		out[k] = append([]string(nil), v...)
	}

	return out
}

// defaultFlushTimeout bounds Flush when ctx has no deadline.
const defaultFlushTimeout = 10 * time.Second

// Flush blocks until the server has processed all buffered messages or ctx
// expires. Call before Shutdown when using Core NATS publishing to avoid
// losing in-flight messages.
//
// If ctx has no deadline, Flush applies a default 10s timeout — the
// underlying NATS client requires a bounded wait.
func (p *Publisher) Flush(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		boundedCtx, cancel := context.WithTimeout(ctx, defaultFlushTimeout)
		defer cancel()

		ctx = boundedCtx
	}

	if err := p.nc.FlushWithContext(ctx); err != nil {
		return fmt.Errorf("flush NATS: %w", err)
	}

	return nil
}

// Conn returns the underlying *nats.Conn. Use to construct additional
// subscribers/consumers that share this connection via WithConnection.
func (p *Publisher) Conn() *nats.Conn {
	return p.nc
}

// JetStream returns the underlying jetstream.JetStream context. Use for
// advanced flows (KV, ObjectStore, custom stream management).
func (p *Publisher) JetStream() jetstream.JetStream {
	return p.js
}

// Shutdown flushes pending Core NATS messages and closes the underlying
// connection if it is owned by the Publisher. Honors ctx for the flush
// step; the close itself is non-blocking.
//
// Flush runs regardless of connection ownership so a shared-connection
// Publisher does not silently drop its in-flight Core NATS sends. The
// connection is closed only when this Publisher owns it.
//
// Idempotent — subsequent calls return the error captured by the first
// invocation (so callers can call Shutdown multiple times without losing
// the original flush failure).
func (p *Publisher) Shutdown(ctx context.Context) error {
	p.shutdownOnce.Do(func() {
		if err := p.Flush(ctx); err != nil {
			p.logger.WarnContext(ctx, "publisher flush failed during shutdown",
				slog.Any("err", err))

			p.shutdownErr = err
		}

		if p.ownsConnection {
			p.logger.InfoContext(ctx, "shutting down publisher NATS connection")
			p.nc.Close()
		}
	})

	return p.shutdownErr
}

// prepareMsg builds the outgoing *nats.Msg and (when OTel is enabled) opens
// a producer span and injects the trace context into the message headers.
//
// Headers are cloned exactly once when ANY mutation is needed
// (JSON Content-Type or OTel trace-context injection). When neither
// applies, the caller's nats.Header is reused as-is — raw Publish stays
// allocation-free apart from the *nats.Msg itself.
//
// ctx is returned because OTel span creation produces a new context
// that the caller must use for the publish call (so the JetStream client
// sees the active span on ctx). On the Core NATS path the caller does not
// use ctx after this point, but threading the new ctx through is harmless.
func (p *Publisher) prepareMsg(
	ctx context.Context,
	subject, reply string,
	data []byte,
	headers nats.Header,
	setJSONContentType bool,
) (context.Context, *nats.Msg, trace.Span) {
	if setJSONContentType || p.otelEnabled {
		headers = cloneHeader(headers)
		if setJSONContentType && headers.Get("Content-Type") == "" {
			headers.Set("Content-Type", contentTypeJSON)
		}
	}

	var span trace.Span

	if p.otelEnabled {
		ctx, span = startPublishSpan(ctx, subject)
		injectTraceContextInto(ctx, headers)
	}

	msg := &nats.Msg{ //nolint:exhaustruct
		Subject: subject,
		Reply:   reply,
		Header:  headers,
		Data:    data,
	}

	return ctx, msg, span
}

// buildJSPublishOpts translates JSPublishOpts into jetstream.PublishOpt
// values. Headers are forwarded through the *nats.Msg argument, not via
// PublishOpt.
func buildJSPublishOpts(o JSPublishOpts) []jetstream.PublishOpt {
	out := make([]jetstream.PublishOpt, 0, 6) //nolint:mnd

	if o.MsgID != "" {
		out = append(out, jetstream.WithMsgID(o.MsgID))
	}

	if o.ExpectedStream != "" {
		out = append(out, jetstream.WithExpectStream(o.ExpectedStream))
	}

	if o.ExpectedLastSequence != nil {
		out = append(out, jetstream.WithExpectLastSequence(*o.ExpectedLastSequence))
	}

	if o.ExpectedLastSubjectSeq != nil {
		out = append(out, jetstream.WithExpectLastSequencePerSubject(*o.ExpectedLastSubjectSeq))
	}

	if o.ExpectedLastMsgID != "" {
		out = append(out, jetstream.WithExpectLastMsgID(o.ExpectedLastMsgID))
	}

	if o.RetryAttempts > 0 {
		out = append(out, jetstream.WithRetryAttempts(o.RetryAttempts))
	}

	return out
}

// JSONPublisher wraps a Publisher and JSON-encodes T messages on every
// call. Generic over T for compile-time payload-type safety. The default
// publish subject is fixed at construction time.
type JSONPublisher[T any] struct {
	publisher *Publisher
	subject   string
}

// NewJSONPublisher wraps the given Publisher and JSON-publishes to subject.
func NewJSONPublisher[T any](publisher *Publisher, subject string) *JSONPublisher[T] {
	return &JSONPublisher[T]{publisher: publisher, subject: subject}
}

// Publish marshals message and sends it via Core NATS to the fixed subject.
func (p *JSONPublisher[T]) Publish(ctx context.Context, message T, opts ...PublishOption) error {
	return p.publisher.PublishJSON(ctx, p.subject, message, opts...)
}

// PublishJS marshals message and sends it via JetStream to the fixed subject.
func (p *JSONPublisher[T]) PublishJS(
	ctx context.Context,
	message T,
	opts ...JSPublishOption,
) (*jetstream.PubAck, error) {
	return p.publisher.PublishJSONJS(ctx, p.subject, message, opts...)
}

// Compile-time assertion.
var _ lifecycle.Resource = (*Publisher)(nil)
