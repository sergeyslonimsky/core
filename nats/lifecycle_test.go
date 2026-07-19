//nolint:testpackage // white-box test: exercises unexported helpers and internal state.
package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// processorFn adapts a function to the Processor[I] interface.
type processorFn[I any] func(context.Context, Message[I]) error

func (f processorFn[I]) Process(ctx context.Context, m Message[I]) error { return f(ctx, m) }

// runInProcessNATS starts a tiny in-process NATS server (no JetStream) and
// returns its client URL. Shut down via t.Cleanup.
func runInProcessNATS(t *testing.T) string {
	t.Helper()

	return startInProcessNATS(t, false)
}

// runInProcessJetStream starts an in-process NATS server with JetStream
// enabled and returns its client URL. Shut down via t.Cleanup.
func runInProcessJetStream(t *testing.T) string {
	t.Helper()

	return startInProcessNATS(t, true)
}

func startInProcessNATS(t *testing.T, jetStream bool) string {
	t.Helper()

	storeDir := t.TempDir()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		StoreDir:  storeDir,
		JetStream: jetStream,
	}

	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)

	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		t.Fatal("in-process NATS server failed to become ready")
	}

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	return ns.ClientURL()
}

// TestSubscriberRun_readyClosesOnSubscribeError verifies that Ready()
// unblocks even when ChanSubscribe fails — otherwise callers blocking on
// Ready() would deadlock forever.
func TestSubscriberRun_readyClosesOnSubscribeError(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	sub, err := NewSubscriber[string](
		Config{URL: url},
		"bad..subject", // illegal subject → ChanSubscribe fails
		fakeProcessor[string]{},
		SubscriberHandlers[string]{},
		WithConnection(nc),
	)
	require.NoError(t, err)

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- sub.Run(t.Context())
	}()

	select {
	case <-sub.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("Ready() did not close after Run error")
	}

	select {
	case err := <-runErrCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on subscribe error")
	}
}

// TestSubscriberRun_doubleRunNoPanic verifies that Subscriber supports
// restart: a second Run after a completed cycle re-arms Ready() (channel
// rotation) and runs to completion without double-closing s.ready. The
// readyOnce guard protects against panics; rotation lets the new cycle's
// Ready() reflect its own subscribe completion.
func TestSubscriberRun_doubleRunNoPanic(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	sub, err := NewSubscriber[string](
		Config{URL: url},
		"test.subj",
		fakeProcessor[string]{},
		SubscriberHandlers[string]{},
		WithConnection(nc),
	)
	require.NoError(t, err)

	ctx1, cancel1 := context.WithCancel(t.Context())

	doneCh := make(chan error, 1)
	go func() { doneCh <- sub.Run(ctx1) }()

	select {
	case <-sub.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("Ready() did not close")
	}

	cancel1()
	require.NoError(t, <-doneCh)

	// Second Run must not panic on close(s.ready); it should return without
	// double-closing thanks to the readyOnce guard.
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()

	doneCh2 := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				doneCh2 <- errors.New("panic on second Run")
			}
		}()

		doneCh2 <- sub.Run(ctx2)
	}()

	cancel2()

	select {
	case err := <-doneCh2:
		require.NoError(t, err, "second Run must not panic")
	case <-time.After(2 * time.Second):
		t.Fatal("second Run did not return")
	}
}

// TestSubscriberRun_alreadyRunningDoesNotCancelActive verifies that a
// second concurrent Run returns ErrAlreadyRunning WITHOUT cancelling the
// first Run's context. The earlier implementation invoked the captured
// cancel() before bailing — but since cancelFn was stored, the test would
// have to rely on deferred cancel(). Here we just verify the active Run
// keeps running.
func TestSubscriberRun_alreadyRunningDoesNotCancelActive(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	sub, err := NewSubscriber[string](
		Config{URL: url},
		"test.active",
		fakeProcessor[string]{},
		SubscriberHandlers[string]{},
		WithConnection(nc),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	doneCh := make(chan error, 1)
	go func() { doneCh <- sub.Run(ctx) }()

	select {
	case <-sub.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("Ready() did not close")
	}

	err = sub.Run(t.Context())
	require.ErrorIs(t, err, ErrAlreadyRunning)

	// The first Run is still alive: it has not returned yet.
	select {
	case <-doneCh:
		t.Fatal("first Run was cancelled by the second Run attempt")
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	require.NoError(t, <-doneCh)
}

// TestSubscriberReady_concurrentReadIsRaceFree exercises Ready() under -race.
// The earlier implementation reset s.ready under mu while Ready() read it
// lock-free — a data race. After the fix, ready is closed via sync.Once and
// never re-created.
func TestSubscriberReady_concurrentReadIsRaceFree(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	sub, err := NewSubscriber[string](
		Config{URL: url},
		"race.subj",
		fakeProcessor[string]{},
		SubscriberHandlers[string]{},
		WithConnection(nc),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	doneCh := make(chan error, 1)
	go func() { doneCh <- sub.Run(ctx) }()

	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			ch := sub.Ready()
			select {
			case <-ch:
			case <-time.After(2 * time.Second):
			}
		})
	}

	wg.Wait()
	cancel()
	require.NoError(t, <-doneCh)
}

// TestPublisherShutdown_flushesEvenWithSharedConnection verifies that a
// Publisher constructed with WithConnection still attempts Flush on
// Shutdown. The earlier implementation skipped Flush for shared connections,
// silently dropping in-flight Core NATS sends.
func TestPublisherShutdown_flushesEvenWithSharedConnection(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	subNC, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(subNC.Close)

	const subject = "flush.shared"

	received := make(chan []byte, 1)
	_, err = subNC.Subscribe(subject, func(m *nats.Msg) { received <- m.Data })
	require.NoError(t, err)
	require.NoError(t, subNC.Flush())

	pub, err := NewPublisher(t.Context(), Config{URL: url}, WithConnection(nc))
	require.NoError(t, err)

	require.NoError(t, pub.Publish(t.Context(), subject, []byte("hello")))

	// Shutdown must flush the shared connection even though it does not own
	// it; otherwise the byte slice may not have reached the server yet.
	require.NoError(t, pub.Shutdown(t.Context()))

	select {
	case got := <-received:
		assert.Equal(t, []byte("hello"), got)
	case <-time.After(2 * time.Second):
		t.Fatal("Publisher.Shutdown did not flush — message lost")
	}

	// Shutdown is idempotent and shared connection must remain usable.
	require.NoError(t, pub.Shutdown(t.Context()))
	assert.False(t, nc.IsClosed(), "shared connection must not be closed by Shutdown")
}

// TestSubscriberHandlers_marshallerAndErrorHandler verifies that the
// Marshaller and ErrorHandler passed via SubscriberHandlers are actually
// invoked on processed messages.
func TestSubscriberHandlers_marshallerAndErrorHandler(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	const subject = "handlers.test"

	var (
		marshallerCalled atomic.Int32
		errHandlerCalled atomic.Int32
		processed        = make(chan struct{}, 1)
	)

	proc := processorFn[string](func(_ context.Context, _ Message[string]) error {
		processed <- struct{}{}

		return errors.New("boom")
	})

	sub, err := NewSubscriber[string](
		Config{URL: url},
		subject,
		proc,
		SubscriberHandlers[string]{
			Marshaller: func(b []byte, p *string) error {
				marshallerCalled.Add(1)

				*p = string(b)

				return nil
			},
			ErrorHandler: func(_ context.Context, _ Message[string], _ error) {
				errHandlerCalled.Add(1)
			},
		},
		WithConnection(nc),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	doneCh := make(chan error, 1)
	go func() { doneCh <- sub.Run(ctx) }()

	<-sub.Ready()

	pubNC, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(pubNC.Close)
	require.NoError(t, pubNC.Publish(subject, []byte("payload")))
	require.NoError(t, pubNC.Flush())

	select {
	case <-processed:
	case <-time.After(2 * time.Second):
		t.Fatal("message was not processed")
	}

	cancel()
	require.NoError(t, <-doneCh)

	assert.Equal(t, int32(1), marshallerCalled.Load())
	assert.Equal(t, int32(1), errHandlerCalled.Load())
}

// TestConfig_MaxReconnectsNilUsesNatsDefault checks that leaving MaxReconnects
// nil does NOT inject nats.MaxReconnects(...) at all — the nats.go default
// (60) applies.
func TestConfig_MaxReconnectsNilUsesNatsDefault(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	// nats.go default is 60.
	assert.Equal(t, 60, nc.Opts.MaxReconnect)

	// And our connectNATS path with nil MaxReconnects matches.
	cfg := Config{URL: url}
	nc2, err := connectNATS(cfg, nil, nil)
	require.NoError(t, err)

	defer nc2.Close()

	assert.Equal(t, 60, nc2.Opts.MaxReconnect)
}

// TestConfig_MaxReconnectsExplicitZero verifies that an explicit *int=0
// produces "no reconnect attempts" (matching nats.go semantics).
func TestConfig_MaxReconnectsExplicitZero(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	cfg := Config{URL: url, MaxReconnects: new(0)}
	nc, err := connectNATS(cfg, nil, nil)
	require.NoError(t, err)

	defer nc.Close()

	assert.Equal(t, 0, nc.Opts.MaxReconnect)
}

// TestConfig_MaxReconnectsForever verifies the -1 sentinel reaches nats.go.
func TestConfig_MaxReconnectsForever(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	cfg := Config{URL: url, MaxReconnects: new(MaxReconnectsForever)}
	nc, err := connectNATS(cfg, nil, nil)
	require.NoError(t, err)

	defer nc.Close()

	assert.Equal(t, -1, nc.Opts.MaxReconnect)
}

// TestPublisherShutdown_closesOwnedConnection verifies the owned-connection
// path still closes nc.
func TestPublisherShutdown_closesOwnedConnection(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	pub, err := NewPublisher(t.Context(), Config{URL: url})
	require.NoError(t, err)

	owned := pub.Conn()
	require.NotNil(t, owned)
	require.False(t, owned.IsClosed())

	require.NoError(t, pub.Shutdown(t.Context()))
	assert.True(t, owned.IsClosed(), "owned connection must be closed by Shutdown")
}

// jsTestStream + jsTestSubject + jsTestDurable are the canned identifiers
// used by the JetStream lifecycle tests.
const (
	jsTestStream  = "TEST_EVENTS"
	jsTestSubject = "test.events"
	jsTestDurable = "test-worker"
)

func newJSStreamConfig() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:     jsTestStream,
		Subjects: []string{jsTestSubject},
		Storage:  jetstream.MemoryStorage,
	}
}

// TestJetStreamConsumer_runShutdownRoundtrip exercises the full JS lifecycle:
// publish through the constructor's stream, Run, process one message, Ack,
// then Shutdown. Verifies no panic on consumeCtx.Stop + drainBuffered path
// and that the connection close drives Run to a clean nil return.
func TestJetStreamConsumer_runShutdownRoundtrip(t *testing.T) {
	t.Parallel()

	url := runInProcessJetStream(t)

	processed := make(chan string, 1)
	proc := processorFn[string](func(_ context.Context, msg Message[string]) error {
		processed <- msg.Payload

		return nil
	})

	cons, err := NewJetStreamConsumer[string](
		t.Context(),
		Config{URL: url},
		jsTestStream,
		jsTestDurable,
		proc,
		JSConsumerHandlers[string]{},
		WithJSStream(newJSStreamConfig()),
		WithJSSubjects(jsTestSubject),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	doneCh := make(chan error, 1)
	go func() { doneCh <- cons.Run(ctx) }()

	select {
	case <-cons.Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("JetStreamConsumer Ready() did not close")
	}

	pub, err := NewPublisher(t.Context(), Config{URL: url})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Shutdown(t.Context()) })

	_, err = pub.PublishJSONJS(t.Context(), jsTestSubject, "hello-js")
	require.NoError(t, err)

	select {
	case got := <-processed:
		assert.Equal(t, "hello-js", got)
	case <-time.After(3 * time.Second):
		t.Fatal("message was not processed")
	}

	// Graceful shutdown drives Run to nil.
	require.NoError(t, cons.Shutdown(t.Context()))
	require.NoError(t, <-doneCh)
}

// TestJetStreamConsumer_shutdownNaksBufferedDeliveries proves that buffered
// messages are NAK'd (not silently dropped) when the consumer shuts down
// mid-stream. We set a LONG AckWait so that ANY redelivery within a short
// observation window can only be the result of an explicit NAK — the broker
// would not redeliver via timeout in this window.
func TestJetStreamConsumer_shutdownNaksBufferedDeliveries(t *testing.T) {
	t.Parallel()

	url := runInProcessJetStream(t)

	const totalMessages = 5

	// First consumer: one worker per message so EVERY message goes through
	// the processor → applyAction → Nak path. This removes the prior
	// pull-buffer-vs-deliveries timing dependence — we wait until ALL
	// messages are inside a worker before triggering shutdown.
	var firstProcessed atomic.Int32

	gate := make(chan struct{})
	firstProc := processorFn[string](func(ctx context.Context, _ Message[string]) error {
		firstProcessed.Add(1)

		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	streamCfg := newJSStreamConfig()
	cons1, err := NewJetStreamConsumer[string](
		t.Context(),
		Config{URL: url},
		jsTestStream,
		jsTestDurable,
		firstProc,
		JSConsumerHandlers[string]{},
		WithJSStream(streamCfg),
		WithJSSubjects(jsTestSubject),
		WithJSWorkerCount(totalMessages),
		WithJSDeliveryBuffer(totalMessages),
		WithJSConsumerConfig(func(c *jetstream.ConsumerConfig) {
			c.AckWait = 30 * time.Second // long enough that timeout cannot drive redelivery
		}),
	)
	require.NoError(t, err)

	runCtx, cancelRun := context.WithCancel(t.Context())

	d1 := make(chan error, 1)
	go func() { d1 <- cons1.Run(runCtx) }()

	<-cons1.Ready()

	pub, err := NewPublisher(t.Context(), Config{URL: url})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Shutdown(t.Context()) })

	for i := range totalMessages {
		_, err = pub.PublishJSONJS(t.Context(), jsTestSubject, fmt.Sprintf("msg-%d", i))
		require.NoError(t, err)
	}

	// Wait until EVERY message reached a processor — guarantees nothing
	// is still sitting in nats.go's pull buffer (Stop discards that buffer
	// silently). At this point all 5 messages are in worker hands.
	require.Eventually(t, func() bool { return firstProcessed.Load() == totalMessages },
		3*time.Second, 10*time.Millisecond,
		"not all messages reached a processor — pull-buffer timing assumption violated")

	// Shut down WITHOUT releasing the gate. Each processor sees ctx.Done,
	// returns ctx.Err() → applyAction issues Nak via default handler.
	cancelRun()
	require.NoError(t, cons1.Shutdown(t.Context()))
	require.NoError(t, <-d1)

	// Second consumer on the same durable: every NAK'd message must be
	// redelivered within a short window. Without NAK, we would wait
	// AckWait (30s) — the test would time out.
	redelivered := make(chan struct{}, totalMessages)
	secondProc := processorFn[string](func(_ context.Context, _ Message[string]) error {
		redelivered <- struct{}{}

		return nil
	})

	cons2, err := NewJetStreamConsumer[string](
		t.Context(),
		Config{URL: url},
		jsTestStream,
		jsTestDurable,
		secondProc,
		JSConsumerHandlers[string]{},
	)
	require.NoError(t, err)

	runCtx2, cancelRun2 := context.WithCancel(t.Context())
	defer cancelRun2()

	d2 := make(chan error, 1)
	go func() { d2 <- cons2.Run(runCtx2) }()

	<-cons2.Ready()

	deadline := time.After(5 * time.Second)

	for i := range totalMessages {
		select {
		case <-redelivered:
		case <-deadline:
			t.Fatalf("only %d/%d messages redelivered within 5s — NAK on shutdown not working",
				i, totalMessages)
		}
	}

	cancelRun2()
	require.NoError(t, cons2.Shutdown(t.Context()))
	require.NoError(t, <-d2)
}

// TestJetStreamConsumer_readyReArmsOnRerun proves that Ready() rotates
// between Run cycles. The test captures the channel identity from the
// first cycle (post-close), and asserts that the channel returned during
// the second cycle is DIFFERENT (new chan was allocated) — meaning the
// rotation actually happened and Ready() does not hand back a stale
// already-closed channel from the prior cycle.
func TestJetStreamConsumer_readyReArmsOnRerun(t *testing.T) {
	t.Parallel()

	url := runInProcessJetStream(t)

	proc := processorFn[string](func(_ context.Context, _ Message[string]) error { return nil })

	cons, err := NewJetStreamConsumer[string](
		t.Context(),
		Config{URL: url},
		jsTestStream,
		jsTestDurable,
		proc,
		JSConsumerHandlers[string]{},
		WithJSStream(newJSStreamConfig()),
		WithJSSubjects(jsTestSubject),
	)
	require.NoError(t, err)

	// Cycle 1: capture the channel identity after it closes.
	ctx1, cancel1 := context.WithCancel(t.Context())

	d1 := make(chan error, 1)
	go func() { d1 <- cons.Run(ctx1) }()

	firstReady := cons.Ready()
	<-firstReady
	cancel1()
	require.NoError(t, <-d1)
	require.True(t, isChanClosed(firstReady), "first cycle's Ready should be closed after Run returns")

	// Cycle 2: start, then poll Ready() until a NEW channel identity is
	// returned. This is the rotation assertion.
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()

	d2 := make(chan error, 1)
	go func() { d2 <- cons.Run(ctx2) }()

	require.Eventually(t, func() bool {
		return cons.Ready() != firstReady
	}, 3*time.Second, 10*time.Millisecond,
		"Ready() must return a different channel on cycle 2 (rotation)")

	// And the new channel must close once subscribe completes.
	select {
	case <-cons.Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("rotated Ready() did not close")
	}

	cancel2()
	require.NoError(t, <-d2)
}

// isChanClosed reports whether a close-only signal channel has been closed.
// Test helper — same semantics as the production isClosed but kept separate
// so the prod helper can change without invalidating the assertion.
func isChanClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// fakeJSMsg satisfies the jetstream.Msg interface for unit tests of
// applyAction / handleStopAction without a real broker. Counters let the
// test assert which ack/nak/term path executed.
type fakeJSMsg struct {
	nakCount  atomic.Int32
	ackCount  atomic.Int32
	termCount atomic.Int32
	delay     time.Duration
}

func (f *fakeJSMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return nil, errors.New("no metadata in fake")
}
func (f *fakeJSMsg) Data() []byte         { return nil }
func (f *fakeJSMsg) Headers() nats.Header { return nil }
func (f *fakeJSMsg) Subject() string      { return "test.fake" }
func (f *fakeJSMsg) Reply() string        { return "" }
func (f *fakeJSMsg) Ack() error {
	f.ackCount.Add(1)

	return nil
}

func (f *fakeJSMsg) DoubleAck(_ context.Context) error {
	f.ackCount.Add(1)

	return nil
}

func (f *fakeJSMsg) Nak() error {
	f.nakCount.Add(1)

	return nil
}

func (f *fakeJSMsg) NakWithDelay(d time.Duration) error {
	f.nakCount.Add(1)
	f.delay = d

	return nil
}
func (f *fakeJSMsg) InProgress() error { return nil }
func (f *fakeJSMsg) Term() error {
	f.termCount.Add(1)

	return nil
}

func (f *fakeJSMsg) TermWithReason(_ string) error {
	f.termCount.Add(1)

	return nil
}

func newFakeJSConsumer() *JetStreamConsumer[string] {
	return &JetStreamConsumer[string]{
		logger: slog.New(slog.DiscardHandler),
	}
}

// TestJetStreamConsumer_handleStopAction_cancelledCtx pins down the
// shutdown-time-benign branch: when ctx is already cancelled,
// JSErrorActionStop is suppressed (no fatal error from Run) and the
// message is NAK'd so the next consumer sees it.
func TestJetStreamConsumer_handleStopAction_cancelledCtx(t *testing.T) {
	t.Parallel()

	c := newFakeJSConsumer()
	fake := &fakeJSMsg{}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := c.handleStopAction(ctx, fake, errors.New("processor failure"))
	require.NoError(t, err, "shutdown-time stop must be benign (return nil)")
	assert.Equal(t, int32(1), fake.nakCount.Load(), "message must be NAK'd on shutdown-time stop")
	assert.Zero(t, fake.ackCount.Load())
	assert.Zero(t, fake.termCount.Load())
}

// TestJetStreamConsumer_handleStopAction_liveCtx confirms the inverse: when
// ctx is still live, JSErrorActionStop returns the wrapped processor error
// and does NOT NAK (preserves message for AckWait redelivery).
func TestJetStreamConsumer_handleStopAction_liveCtx(t *testing.T) {
	t.Parallel()

	c := newFakeJSConsumer()
	fake := &fakeJSMsg{}
	procErr := errors.New("processor failure")

	err := c.handleStopAction(t.Context(), fake, procErr)
	require.Error(t, err)
	require.ErrorIs(t, err, procErr)
	assert.Contains(t, err.Error(), "jetstream consumer stopped")
	assert.Zero(t, fake.nakCount.Load(), "live-ctx stop must not NAK — the broker redelivers via AckWait")
}

// TestJetStreamConsumer_applyAction_unknownAction verifies the fallback
// path when a caller-supplied JSErrorHandler returns an unknown action
// value: applyAction logs, NAKs, and returns errors.Join(ErrInvalidErrorAction, procErr).
func TestJetStreamConsumer_applyAction_unknownAction(t *testing.T) {
	t.Parallel()

	c := newFakeJSConsumer()
	c.errorHandler = func(_ context.Context, _ Message[string], _ error) (JSErrorAction, time.Duration) {
		return JSErrorAction(99), 0
	}

	fake := &fakeJSMsg{}
	procErr := errors.New("boom")
	msg := Message[string]{Subject: "test"}

	err := c.applyAction(t.Context(), fake, msg, procErr)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidErrorAction)
	require.ErrorIs(t, err, procErr)
	assert.Equal(t, int32(1), fake.nakCount.Load(), "unknown action must fall back to NAK")
}

// TestJetStreamConsumer_applyAction_nakWithDelay exercises the NakWithDelay
// branch and verifies the delay value reaches the broker.
func TestJetStreamConsumer_applyAction_nakWithDelay(t *testing.T) {
	t.Parallel()

	const wantDelay = 750 * time.Millisecond

	c := newFakeJSConsumer()
	c.errorHandler = func(_ context.Context, _ Message[string], _ error) (JSErrorAction, time.Duration) {
		return JSErrorActionNakDelay, wantDelay
	}

	fake := &fakeJSMsg{}
	msg := Message[string]{Subject: "test"}

	require.NoError(t, c.applyAction(t.Context(), fake, msg, errors.New("transient")))
	assert.Equal(t, int32(1), fake.nakCount.Load())
	assert.Equal(t, wantDelay, fake.delay)
}

// TestPublisherFlush_noDeadlineUsesDefault verifies the no-deadline branch
// of Publisher.Flush: when ctx has no Deadline the method wraps it in a
// 10s timeout and completes promptly against a live in-process server.
func TestPublisherFlush_noDeadlineUsesDefault(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	pub, err := NewPublisher(t.Context(), Config{URL: url})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Shutdown(t.Context()) })

	// context.Background has no deadline. Without the default-timeout branch,
	// FlushWithContext would block on a never-cancellable context. Here the
	// server is healthy, so the bounded flush completes immediately.
	start := time.Now()

	require.NoError(t, pub.Flush(context.Background()))
	assert.Less(t, time.Since(start), 2*time.Second,
		"flush against a live server must complete fast even with the default timeout")
}

// TestPublishJSON_doesNotMutateCallerHeaders pins down the
// shared-header-mutation bug: a single nats.Header reused across multiple
// PublishJSON calls must not accumulate state from the JSON helper.
func TestPublishJSON_doesNotMutateCallerHeaders(t *testing.T) {
	t.Parallel()

	url := runInProcessNATS(t)

	pub, err := NewPublisher(t.Context(), Config{URL: url})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Shutdown(t.Context()) })

	shared := nats.Header{"X-Trace": []string{"abc"}}
	require.NoError(t, pub.PublishJSON(t.Context(), "shared.hdr", "v1", WithHeaders(shared)))

	// Caller's map must be untouched: only the original key remains.
	assert.Equal(t, nats.Header{"X-Trace": []string{"abc"}}, shared,
		"PublishJSON must not mutate caller's nats.Header")
}

// TestPublish_otelDoesNotMutateCallerHeaders covers the OTel-only branch of
// the single-clone gate in Publisher.Publish: when otel is enabled but the
// caller uses plain Publish (not PublishJSON), trace-context injection
// must still write into a clone — never the caller's shared map.
func TestPublish_otelDoesNotMutateCallerHeaders(t *testing.T) {
	t.Parallel()

	// Register a no-op propagator that actually injects something so we
	// can detect any mutation. The global TextMapPropagator defaults to a
	// no-op which writes nothing — without setting one, the injection step
	// is silent and this test would tautologically pass.
	prev := otel.GetTextMapPropagator()

	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	url := runInProcessNATS(t)

	pub, err := NewPublisher(t.Context(), Config{URL: url}, WithOtel())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Shutdown(t.Context()) })

	// Run inside an active span so the propagator has something to inject.
	tracer := otel.Tracer("test")

	ctx, span := tracer.Start(t.Context(), "test-publish")
	defer span.End()

	shared := nats.Header{"X-Trace": []string{"abc"}}
	require.NoError(t, pub.Publish(ctx, "otel.shared", []byte("data"), WithHeaders(shared)))

	// Caller's map must remain exactly what it was — no traceparent.
	assert.Equal(t, nats.Header{"X-Trace": []string{"abc"}}, shared,
		"Publish with otel must not mutate caller's nats.Header")
}
