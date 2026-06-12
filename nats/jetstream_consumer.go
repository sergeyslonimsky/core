package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/sergeyslonimsky/core/lifecycle"
)

// JSConsumerHandlers carries payload-typed callbacks for JetStreamConsumer.
// Pass to NewJetStreamConsumer. Zero value is valid: Marshaller=nil means
// default JSON decoding, ErrorHandler=nil means the default action on a
// processor error is JSErrorActionNak.
type JSConsumerHandlers[I any] struct {
	// Marshaller decodes raw message bytes into I. Nil = json.Unmarshal.
	Marshaller BodyMarshaller[I]

	// ErrorHandler decides what to do when Processor.Process returns a
	// non-nil error. Nil = Nak (redeliver after AckWait).
	ErrorHandler JSErrorHandler[I]
}

// JetStreamConsumer is a generic NATS JetStream pull consumer that consumes
// messages and dispatches them to a Processor via a worker pool.
//
// Implements lifecycle.Runner. Construct via NewJetStreamConsumer.
//
// The consumer is created (or looked up — see WithJSAssumeExistingConsumer)
// in the constructor; the pull loop starts in Run. Use Ready() to block
// until Run has established the pull subscription.
type JetStreamConsumer[I any] struct {
	stream         string
	durable        string
	processor      Processor[I]
	bodyMarshaller BodyMarshaller[I]
	errorHandler   JSErrorHandler[I]
	logger         *slog.Logger
	otelEnabled    bool

	nc             *nats.Conn
	js             jetstream.JetStream
	consumer       jetstream.Consumer
	ownsConnection bool

	workerCount    int
	deliveryBuffer int

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

// NewJetStreamConsumer creates a JetStreamConsumer and eagerly:
//  1. Dials NATS (or uses WithConnection's shared connection).
//  2. Verifies/creates the stream if WithJSStream is provided.
//  3. Creates/updates the durable consumer with the configured subject
//     filter(s), AckPolicy=Explicit, and any user mutations from
//     WithJSConsumerConfig — OR looks up an existing consumer when
//     WithJSAssumeExistingConsumer is set.
//
// Any of these steps failing returns a wrapped error and closes the owned
// connection (if any).
//
// stream and durable are required (validated as non-empty).
//
// handlers carries optional payload-typed callbacks; pass
// JSConsumerHandlers[I]{} for defaults (JSON decode + Nak-on-error).
//
//nolint:funlen // linear constructor: dial, jetstream context, stream, consumer; splitting hides ordering.
func NewJetStreamConsumer[I any](
	ctx context.Context,
	cfg Config,
	stream string,
	durable string,
	processor Processor[I],
	handlers JSConsumerHandlers[I],
	opts ...JSConsumerOption,
) (*JetStreamConsumer[I], error) {
	if processor == nil {
		return nil, ErrNilProcessor
	}

	if stream == "" {
		return nil, ErrEmptyStream
	}

	if durable == "" {
		return nil, ErrEmptyDurable
	}

	o := defaultJSConsumerOptions()
	for _, apply := range opts {
		apply.applyToJSConsumer(&o)
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

	consumer, err := resolveJSConsumer(ctx, js, stream, durable, o)
	if err != nil {
		closeOwned(nc, ownsConnection)

		return nil, err
	}

	return &JetStreamConsumer[I]{ //nolint:exhaustruct
		stream:         stream,
		durable:        durable,
		processor:      processor,
		bodyMarshaller: handlers.Marshaller,
		errorHandler:   handlers.ErrorHandler,
		logger:         o.common.logger,
		otelEnabled:    o.common.otelEnabled,
		nc:             nc,
		js:             js,
		consumer:       consumer,
		ownsConnection: ownsConnection,
		workerCount:    o.workerCount,
		deliveryBuffer: o.deliveryBuffer,
		ready:          make(chan struct{}),
	}, nil
}

// resolveJSConsumer either looks up an existing durable consumer (when
// assumeExistingOnly is set) or creates/updates one.
func resolveJSConsumer(
	ctx context.Context,
	js jetstream.JetStream,
	stream, durable string,
	o jsConsumerOptions,
) (jetstream.Consumer, error) {
	if o.assumeExistingOnly {
		consumer, err := js.Consumer(ctx, stream, durable)
		if err != nil {
			return nil, fmt.Errorf("lookup existing consumer %s on stream %s: %w", durable, stream, err)
		}

		return consumer, nil
	}

	cfg := buildConsumerConfig(durable, o.subjectFilters, o.consumerMutate)

	consumer, err := js.CreateOrUpdateConsumer(ctx, stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("create/update consumer %s on stream %s: %w", durable, stream, err)
	}

	return consumer, nil
}

// Conn returns the underlying *nats.Conn.
func (c *JetStreamConsumer[I]) Conn() *nats.Conn {
	return c.nc
}

// JetStream returns the underlying jetstream.JetStream context.
func (c *JetStreamConsumer[I]) JetStream() jetstream.JetStream {
	return c.js
}

// Ready returns a channel that is closed once Run has established the
// JetStream Consume callback. Block on it to ensure tests/producers don't
// race the consumer's start-up.
//
// Ready() is re-armed on every Run cycle (the channel rotates), so callers
// blocking on Ready() after a restart see the new cycle's completion.
func (c *JetStreamConsumer[I]) Ready() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ready
}

// Run starts the JetStream Consume loop, dispatching messages to a worker
// pool, and blocks until ctx is cancelled or a fatal processor error
// triggers JSErrorActionStop.
//
// Returns nil on graceful shutdown; returns a wrapped error when the
// processor signals JSErrorActionStop.
//
// Implements lifecycle.Runner.
//
//nolint:funlen // single-method lifecycle coordinator: split would obscure ordering.
func (c *JetStreamConsumer[I]) Run(ctx context.Context) error {
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.mu.Lock()
	if c.cancelFn != nil {
		c.mu.Unlock()

		return ErrAlreadyRunning
	}

	c.cancelFn = cancel
	c.done = make(chan struct{})

	// Re-arm Ready for this Run cycle so Ready() blocks on the new
	// subscribe completion rather than returning an already-closed channel
	// from the previous Run.
	//
	// Assigning a fresh sync.Once is safe: we hold mu, no goroutine can be
	// inside readyOnce.Do concurrently, and we are replacing — not copying —
	// a used Once with a zero-value struct.
	if isClosed(c.ready) {
		c.ready = make(chan struct{})
		c.readyOnce = sync.Once{}
	}
	c.mu.Unlock()

	var (
		fatalErr     error
		fatalErrOnce sync.Once
		stop         = func(err error) {
			fatalErrOnce.Do(func() { fatalErr = err })
			cancel()
		}
	)

	// closeReady ensures Ready() callers unblock on every exit path,
	// including subscription-registration failures.
	closeReady := func() { c.readyOnce.Do(func() { close(c.ready) }) }

	defer func() {
		closeReady()

		c.mu.Lock()
		close(c.done)
		c.cancelFn = nil
		c.mu.Unlock()
	}()

	deliveries := make(chan jetstream.Msg, c.deliveryBuffer)

	consumeCtx, err := c.consumer.Consume(func(msg jetstream.Msg) {
		select {
		case deliveries <- msg:
		case <-innerCtx.Done():
			// Shutting down: NAK so the broker can redeliver to the next
			// subscriber rather than waiting for AckWait.
			_ = msg.Nak()
		}
	})
	if err != nil {
		return fmt.Errorf("start JetStream consume: %w", err)
	}

	closeReady()

	// Build the per-message Process function ONCE per Run cycle. With OTel
	// enabled the wrapper closes over a tracer + the user's Process method
	// value, so creating it per-message would allocate on every delivery.
	process := c.processor.Process
	if c.otelEnabled {
		process = instrumentProcessJS(c.processor.Process)
	}

	var workers sync.WaitGroup

	workers.Add(c.workerCount)

	for range c.workerCount {
		go func() {
			defer workers.Done()

			c.workerLoop(innerCtx, deliveries, process, stop)
		}()
	}

	<-innerCtx.Done()

	// Phase 1: stop new deliveries from the Consume callback. Stop() is
	// asynchronous in the jetstream library; the dispatcher continues
	// running until it observes the stop signal.
	consumeCtx.Stop()

	// Phase 2: block until the dispatcher has fully exited. After this
	// channel closes the jetstream library guarantees the callback will
	// never be invoked again — no more sends to `deliveries`. This is the
	// synchronization point that lets us safely drain without races.
	<-consumeCtx.Closed()

	// Phase 3: wait for in-flight workers. They exit on innerCtx.Done.
	workers.Wait()

	// Phase 4: NAK any leftover buffered messages so the broker can
	// redeliver immediately rather than waiting AckWait. Safe to do
	// without locking — no other goroutine can touch `deliveries` now.
	c.drainBuffered(deliveries)

	return fatalErr
}

// drainBuffered NAKs every message currently buffered in deliveries and
// returns. Called only during shutdown, after workers have exited and the
// Consume callback has been Stop'd.
func (c *JetStreamConsumer[I]) drainBuffered(deliveries <-chan jetstream.Msg) {
	for {
		select {
		case msg := <-deliveries:
			if err := msg.Nak(); err != nil {
				c.logger.Warn("nak buffered message during shutdown failed",
					slog.Any("err", err))
			}
		default:
			return
		}
	}
}

// Shutdown signals Run to stop and waits for it to finish, bounded by ctx.
// On ctx expiry returns ctx.Err() — but the owned connection is still
// closed so the resource does not leak.
//
// Implements lifecycle.Resource. Idempotent.
func (c *JetStreamConsumer[I]) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	cancel := c.cancelFn
	done := c.done
	c.mu.Unlock()

	var ctxErr error

	if cancel != nil {
		cancel()

		select {
		case <-done:
		case <-ctx.Done():
			ctxErr = fmt.Errorf("jetstream consumer shutdown: %w", ctx.Err())
		}
	}

	if c.ownsConnection {
		c.logger.InfoContext(ctx, "shutting down jetstream consumer NATS connection")
		c.nc.Close()
	}

	return ctxErr
}

// workerLoop pulls deliveries from the channel and processes them until
// ctx cancels or the channel closes. stop is invoked on JSErrorActionStop.
// process is the (possibly OTel-wrapped) Process function pre-built by Run.
func (c *JetStreamConsumer[I]) workerLoop(
	ctx context.Context,
	deliveries <-chan jetstream.Msg,
	process processFn[I],
	stop func(error),
) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-deliveries:
			if !ok {
				return
			}

			if err := c.handleMsg(ctx, msg, process); err != nil {
				stop(err)

				return
			}
		}
	}
}

// handleMsg decodes the body, runs the processor, and applies the
// configured error policy. Returns a non-nil error only when the policy is
// JSErrorActionStop (and only if the context is not already cancelled —
// shutdown-time errors do not propagate as fatal).
func (c *JetStreamConsumer[I]) handleMsg(
	ctx context.Context,
	raw jetstream.Msg,
	process processFn[I],
) error {
	msg, err := c.buildMessage(raw)
	if err != nil {
		c.logger.WarnContext(ctx, "failed to decode jetstream message",
			slog.String("subject", raw.Subject()),
			slog.Any("err", err),
		)

		return c.applyAction(ctx, raw, msg, fmt.Errorf("decode message: %w", err))
	}

	if err = process(ctx, msg); err != nil {
		c.logger.WarnContext(ctx, "jetstream message processing failed",
			slog.String("subject", raw.Subject()),
			slog.Uint64("sequence", msg.Sequence),
			slog.Any("err", err),
		)

		return c.applyAction(ctx, raw, msg, err)
	}

	if ackErr := raw.Ack(); ackErr != nil {
		c.logger.ErrorContext(ctx, "ack message failed", slog.Any("err", ackErr))
	}

	return nil
}

func (c *JetStreamConsumer[I]) buildMessage(raw jetstream.Msg) (Message[I], error) {
	var payload I

	data := raw.Data()

	if c.bodyMarshaller != nil {
		if err := c.bodyMarshaller(data, &payload); err != nil {
			return Message[I]{ //nolint:exhaustruct
				Subject: raw.Subject(),
				Headers: raw.Headers(),
			}, fmt.Errorf("custom marshaller: %w", err)
		}
	} else if err := json.Unmarshal(data, &payload); err != nil {
		return Message[I]{ //nolint:exhaustruct
			Subject: raw.Subject(),
			Headers: raw.Headers(),
		}, fmt.Errorf("json unmarshal: %w", err)
	}

	msg := Message[I]{ //nolint:exhaustruct
		Payload: payload,
		Subject: raw.Subject(),
		Headers: raw.Headers(),
	}

	if meta, mErr := raw.Metadata(); mErr == nil {
		msg.Sequence = meta.Sequence.Stream
		msg.NumDelivered = meta.NumDelivered
		msg.Timestamp = meta.Timestamp
	}

	return msg, nil
}

// applyAction consults the configured JSErrorHandler (or applies the
// default Nak) and executes the resulting action against the raw message.
// Returns a non-nil error only on JSErrorActionStop — and only when the
// context is not already cancelled, so a shutdown-time processor failure
// does not turn into a fatal Run error.
func (c *JetStreamConsumer[I]) applyAction(
	ctx context.Context,
	raw jetstream.Msg,
	msg Message[I],
	procErr error,
) error {
	action, delay := JSErrorActionNak, time.Duration(0)
	if c.errorHandler != nil {
		action, delay = c.errorHandler(ctx, msg, procErr)
	}

	switch action {
	case JSErrorActionNak:
		c.tryAck(ctx, "nak", raw.Nak)
	case JSErrorActionNakDelay:
		c.tryAck(ctx, "nak-with-delay", func() error { return raw.NakWithDelay(delay) })
	case JSErrorActionTerm:
		c.tryAck(ctx, "term", raw.Term)
	case JSErrorActionAck:
		c.tryAck(ctx, "ack-on-error", raw.Ack)
	case JSErrorActionStop:
		return c.handleStopAction(ctx, raw, procErr)
	default:
		c.logger.ErrorContext(ctx, "unknown JSErrorAction, defaulting to Nak",
			slog.Int("action", int(action)),
			slog.String("subject", msg.Subject),
		)
		c.tryAck(ctx, "fallback-nak", raw.Nak)

		return errors.Join(ErrInvalidErrorAction, procErr)
	}

	return nil
}

// tryAck runs a JetStream ack/nak/term operation and logs failures with the
// given label. Errors are non-fatal — at this point we've already decided
// the message's fate; a failed Ack just means the broker may redeliver.
func (c *JetStreamConsumer[I]) tryAck(ctx context.Context, label string, op func() error) {
	if err := op(); err != nil {
		c.logger.ErrorContext(ctx, label+" message failed", slog.Any("err", err))
	}
}

// handleStopAction implements JSErrorActionStop. During shutdown (ctx
// already cancelled) the processor error is treated as benign — the message
// is NAK'd so it survives to the next run; the outer handleMsg log has
// already recorded the diagnostic. Outside shutdown the error is propagated
// up so Run returns it.
func (c *JetStreamConsumer[I]) handleStopAction(
	ctx context.Context,
	raw jetstream.Msg,
	procErr error,
) error {
	if ctx.Err() != nil {
		c.tryAck(ctx, "shutdown-time nak", raw.Nak)

		return nil //nolint:nilerr // intentional: shutdown-time stop is benign
	}

	return fmt.Errorf("jetstream consumer stopped on processor error: %w", procErr)
}

// buildConsumerConfig populates the JetStream ConsumerConfig with defaults
// and runs the optional caller mutator on top. After the mutator runs,
// Durable and AckPolicy are re-asserted so a zero-value or wiped struct
// from the mutator cannot accidentally downgrade delivery semantics.
//
// Callers who legitimately want a different AckPolicy must set it inside
// the mutator AFTER the standard defaults are applied — at which point
// their explicit value sticks because re-assertion only fires for
// AckNonePolicy (the zero value).
func buildConsumerConfig(
	durable string,
	subjects []string,
	mutate func(*jetstream.ConsumerConfig),
) jetstream.ConsumerConfig {
	cfg := jetstream.ConsumerConfig{ //nolint:exhaustruct
		Durable:   durable,
		AckPolicy: jetstream.AckExplicitPolicy,
	}

	switch len(subjects) {
	case 0:
		// no filter
	case 1:
		cfg.FilterSubject = subjects[0]
	default:
		cfg.FilterSubjects = append([]string{}, subjects...)
	}

	if mutate != nil {
		mutate(&cfg)

		// Re-assert durable name in case the mutator cleared it.
		cfg.Durable = durable

		// Guard against accidental AckPolicy downgrade from a wiped struct.
		// Callers who explicitly want AckNonePolicy must build the consumer
		// outside this package — leaving it at zero here would silently
		// turn at-least-once into at-most-once.
		if cfg.AckPolicy == jetstream.AckNonePolicy {
			cfg.AckPolicy = jetstream.AckExplicitPolicy
		}
	}

	return cfg
}

// Compile-time assertions.
var (
	_ lifecycle.Runner   = (*JetStreamConsumer[any])(nil)
	_ lifecycle.Resource = (*JetStreamConsumer[any])(nil)
)
