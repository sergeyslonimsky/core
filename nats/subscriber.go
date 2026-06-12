package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/sergeyslonimsky/core/lifecycle"
)

// contentTypeJSON is the only Content-Type accepted by the default JSON
// body marshaller. Empty Content-Type is also accepted for compatibility
// with publishers that don't set headers.
const contentTypeJSON = "application/json"

// SubscriberHandlers carries payload-typed callbacks for Subscriber. Pass
// to NewSubscriber. Zero value is valid: Marshaller=nil means default JSON
// decoding, ErrorHandler=nil means processor errors are only logged.
type SubscriberHandlers[I any] struct {
	// Marshaller decodes raw message bytes into I. Nil = json.Unmarshal.
	Marshaller BodyMarshaller[I]

	// ErrorHandler is invoked when Processor.Process returns a non-nil
	// error. Nil = log only.
	ErrorHandler SubscriberErrorHandler[I]
}

// Subscriber is a generic Core NATS subscriber (at-most-once delivery) that
// consumes messages from a subject or queue group and dispatches them to a
// Processor via a worker pool.
//
// Implements lifecycle.Runner. Construct via NewSubscriber.
//
// Subscription registration happens in Run (not the constructor) — matching
// the lifecycle model used by kafka.Consumer and rabbitmq. Use Ready() to
// block until the subscription is live (tests, smoke checks).
//
// Core NATS is at-most-once: messages published to the subject before Run
// has registered the subscription are dropped by the broker. For
// at-least-once semantics use JetStreamConsumer.
type Subscriber[I any] struct {
	processor      Processor[I]
	bodyMarshaller BodyMarshaller[I]
	errorHandler   SubscriberErrorHandler[I]
	logger         *slog.Logger
	otelEnabled    bool

	nc             *nats.Conn
	ownsConnection bool

	subjectFilter string
	queueGroup    string
	workerCount   int
	channelBuffer int

	// Lifecycle state.
	mu       sync.Mutex
	cancelFn context.CancelFunc
	done     chan struct{}
	// ready is a close-only signal channel. Invariant: it is NEVER sent to;
	// only Run (via readyOnce) ever calls close on it. isClosed() depends on
	// this — a stray send would be misread as "closed" and the channel would
	// be silently rotated away under callers.
	ready     chan struct{}
	readyOnce sync.Once
}

// NewSubscriber creates a Subscriber and eagerly dials NATS (unless
// WithConnection is provided). Returns an error if cfg is invalid or the
// connection cannot be established.
//
// subject is required (the NATS subject filter to consume from). handlers
// carries optional payload-typed callbacks; pass a zero-value
// SubscriberHandlers[I]{} for defaults.
//
// By default the message body is JSON-unmarshalled and a Content-Type
// header of "application/json" (or empty) is enforced. Supply
// handlers.Marshaller for custom decoding and handlers.ErrorHandler to
// observe Processor failures.
func NewSubscriber[I any](
	cfg Config,
	subject string,
	processor Processor[I],
	handlers SubscriberHandlers[I],
	opts ...SubscriberOption,
) (*Subscriber[I], error) {
	if processor == nil {
		return nil, ErrNilProcessor
	}

	if subject == "" {
		return nil, ErrEmptySubjectFilter
	}

	o := defaultSubscriberOptions()
	for _, apply := range opts {
		apply.applyToSubscriber(&o)
	}

	nc, ownsConnection, err := acquireConn(cfg, o.common)
	if err != nil {
		return nil, err
	}

	return &Subscriber[I]{ //nolint:exhaustruct
		processor:      processor,
		bodyMarshaller: handlers.Marshaller,
		errorHandler:   handlers.ErrorHandler,
		logger:         o.common.logger,
		otelEnabled:    o.common.otelEnabled,
		nc:             nc,
		ownsConnection: ownsConnection,
		subjectFilter:  subject,
		queueGroup:     o.queueGroup,
		workerCount:    o.workerCount,
		channelBuffer:  o.channelBuffer,
		ready:          make(chan struct{}),
	}, nil
}

// Conn returns the underlying *nats.Conn. Useful for sharing the
// connection with a Publisher.
func (s *Subscriber[I]) Conn() *nats.Conn {
	return s.nc
}

// Ready returns a channel that is closed once Run has registered the NATS
// subscription. Block on it to ensure publishes are not lost in the
// at-most-once gap between Run start and Subscribe completion (typical
// pattern in tests).
//
// On a never-Run Subscriber, the channel never closes — pair with a
// timeout if you cannot guarantee Run is being called.
//
// Ready() is re-armed on every Run cycle: after a Run returns, the next
// Run rotates the underlying channel so callers that block on Ready()
// see the new cycle's subscribe completion, not the previous one.
func (s *Subscriber[I]) Ready() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ready
}

// Run subscribes to the configured subject (optionally in a queue group),
// dispatches messages to a worker pool, and blocks until ctx is cancelled.
//
// Returns ErrAlreadyRunning if called while an earlier Run is still active.
// Returns nil on graceful ctx cancellation.
//
// Implements lifecycle.Runner.
//
//nolint:funlen // single-method lifecycle coordinator: split would obscure ordering.
func (s *Subscriber[I]) Run(ctx context.Context) error {
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.mu.Lock()
	if s.cancelFn != nil {
		s.mu.Unlock()

		return ErrAlreadyRunning
	}

	s.cancelFn = cancel
	s.done = make(chan struct{})

	// Re-arm Ready for this Run cycle. The constructor seeded s.ready once;
	// every subsequent Run rotates it so Ready() reflects the new cycle's
	// subscribe completion, not the previous one.
	//
	// Assigning a fresh sync.Once is safe here: we hold mu, no goroutine
	// can be inside readyOnce.Do concurrently, and we are replacing — not
	// copying — a used Once with a zero-value struct.
	if isClosed(s.ready) {
		s.ready = make(chan struct{})
		s.readyOnce = sync.Once{}
	}
	s.mu.Unlock()

	// closeReady is invoked on every exit path so Ready() callers unblock
	// even when subscription registration fails.
	closeReady := func() { s.readyOnce.Do(func() { close(s.ready) }) }

	defer func() {
		closeReady()

		s.mu.Lock()
		close(s.done)
		s.cancelFn = nil
		s.mu.Unlock()
	}()

	deliveries := make(chan *nats.Msg, s.channelBuffer)

	var (
		sub *nats.Subscription
		err error
	)

	if s.queueGroup != "" {
		sub, err = s.nc.ChanQueueSubscribe(s.subjectFilter, s.queueGroup, deliveries)
	} else {
		sub, err = s.nc.ChanSubscribe(s.subjectFilter, deliveries)
	}

	if err != nil {
		return fmt.Errorf("subscribe to subject %s: %w", s.subjectFilter, err)
	}

	// Signal readiness AFTER Subscribe so consumers blocking on Ready() can
	// safely publish.
	closeReady()

	// Build the per-message Process function ONCE per Run cycle. With OTel
	// enabled the wrapper closes over a tracer + the user's Process method
	// value, so creating it per-message would allocate on every delivery.
	process := s.processor.Process
	if s.otelEnabled {
		process = instrumentProcessSubscriber(s.processor.Process)
	}

	var workers sync.WaitGroup

	workers.Add(s.workerCount)

	for range s.workerCount {
		go func() {
			defer workers.Done()

			s.workerLoop(innerCtx, deliveries, process)
		}()
	}

	<-innerCtx.Done()

	// Drain stops broker→client delivery and closes the channel so workers
	// can finish in-flight messages and exit.
	if drainErr := sub.Drain(); drainErr != nil {
		s.logger.WarnContext(ctx, "drain subscription failed", slog.Any("err", drainErr))
	}

	workers.Wait()

	return nil
}

// Shutdown signals Run to stop and waits for it to finish, bounded by ctx.
// On ctx expiry returns ctx.Err() — but the owned connection is still
// closed so the resource does not leak.
//
// Implements lifecycle.Resource. Idempotent.
func (s *Subscriber[I]) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	cancel := s.cancelFn
	done := s.done
	s.mu.Unlock()

	var ctxErr error

	if cancel != nil {
		cancel()

		select {
		case <-done:
		case <-ctx.Done():
			ctxErr = fmt.Errorf("subscriber shutdown: %w", ctx.Err())
		}
	}

	if s.ownsConnection {
		s.logger.InfoContext(ctx, "shutting down subscriber NATS connection")
		s.nc.Close()
	}

	return ctxErr
}

// workerLoop pulls deliveries until ctx is cancelled or the channel closes.
// process is the (possibly OTel-wrapped) Process function pre-built by Run.
func (s *Subscriber[I]) workerLoop(
	ctx context.Context,
	deliveries <-chan *nats.Msg,
	process processFn[I],
) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-deliveries:
			if !ok {
				return
			}

			s.handleMsg(ctx, msg, process)
		}
	}
}

// handleMsg decodes the body and invokes the processor. Decode and process
// errors are routed through the error handler if configured.
func (s *Subscriber[I]) handleMsg(ctx context.Context, raw *nats.Msg, process processFn[I]) {
	payload, err := s.decode(raw)
	if err != nil {
		s.onError(ctx, Message[I]{ //nolint:exhaustruct
			Subject: raw.Subject,
			Reply:   raw.Reply,
			Headers: raw.Header,
		}, fmt.Errorf("decode message: %w", err))

		return
	}

	msg := Message[I]{ //nolint:exhaustruct
		Payload: payload,
		Subject: raw.Subject,
		Reply:   raw.Reply,
		Headers: raw.Header,
	}

	if err = process(ctx, msg); err != nil {
		s.onError(ctx, msg, err)
	}
}

func (s *Subscriber[I]) decode(raw *nats.Msg) (I, error) {
	var payload I

	if s.bodyMarshaller != nil {
		if err := s.bodyMarshaller(raw.Data, &payload); err != nil {
			return payload, fmt.Errorf("custom marshaller: %w", err)
		}

		return payload, nil
	}

	// Default JSON path: accept empty Content-Type (publishers without headers)
	// or explicit application/json; reject anything else with ErrUnsupportedMessage.
	if ct := contentType(raw.Header); ct != "" && ct != contentTypeJSON {
		return payload, fmt.Errorf("%w: %s", ErrUnsupportedMessage, ct)
	}

	if err := json.Unmarshal(raw.Data, &payload); err != nil {
		return payload, fmt.Errorf("json unmarshal: %w", err)
	}

	return payload, nil
}

func (s *Subscriber[I]) onError(ctx context.Context, msg Message[I], err error) {
	s.logger.WarnContext(ctx, "subscriber message handling failed",
		slog.String("subject", msg.Subject),
		slog.Any("err", err),
	)

	if s.errorHandler != nil {
		s.errorHandler(ctx, msg, err)
	}
}

// contentType returns the Content-Type header (case-insensitive) or empty.
func contentType(h nats.Header) string {
	if h == nil {
		return ""
	}

	return h.Get("Content-Type")
}

// isClosed reports whether a struct{} signal channel has been closed.
// Safe to call concurrently — only reads. Must be called under the same
// lock that guards rotation of the channel.
func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Compile-time assertions.
var (
	_ lifecycle.Runner   = (*Subscriber[any])(nil)
	_ lifecycle.Resource = (*Subscriber[any])(nil)
)
