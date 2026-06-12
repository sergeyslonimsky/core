package integration_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/internal/integration/testhelpers"
	corenats "github.com/sergeyslonimsky/core/nats"
)

const (
	natsEventually = 5 * time.Second
	natsTick       = 25 * time.Millisecond
)

type natsEvent struct {
	ID   int    `json:"id"`
	Text string `json:"text"`
}

type natsRecorder struct {
	mu       sync.Mutex
	received []natsEvent
	headers  []nats.Header
	seq      []uint64
	delivers []uint64
	failN    atomic.Int32
}

func (p *natsRecorder) Process(_ context.Context, msg corenats.Message[natsEvent]) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.received = append(p.received, msg.Payload)
	p.headers = append(p.headers, msg.Headers)
	p.seq = append(p.seq, msg.Sequence)
	p.delivers = append(p.delivers, msg.NumDelivered)

	if p.failN.Load() > 0 {
		p.failN.Add(-1)

		return assert.AnError
	}

	return nil
}

func (p *natsRecorder) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.received)
}

func (p *natsRecorder) Snapshot() []natsEvent {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]natsEvent, len(p.received))
	copy(out, p.received)

	return out
}

func (p *natsRecorder) DeliverCounts() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]uint64, len(p.delivers))
	copy(out, p.delivers)

	return out
}

type natsRunner interface {
	Run(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type readyRunner interface {
	natsRunner
	Ready() <-chan struct{}
}

// runNATSRunner starts a Runner in a goroutine, blocks until its
// subscription is ready (when the runner exposes Ready()), and registers
// cleanup that cancels and waits for Run to return, then calls Shutdown.
func runNATSRunner(t *testing.T, runner natsRunner) {
	t.Helper()

	runCtx, cancel := context.WithCancel(t.Context())
	runErr := make(chan error, 1)

	go func() { runErr <- runner.Run(runCtx) }()

	if r, ok := runner.(readyRunner); ok {
		select {
		case <-r.Ready():
		case <-time.After(natsEventually):
			cancel()
			<-runErr
			t.Fatal("runner Ready() never closed")
		}
	}

	t.Cleanup(func() {
		cancel()

		select {
		case <-runErr:
		case <-time.After(natsEventually):
			t.Error("runner did not stop within timeout")
		}

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()

		_ = runner.Shutdown(shutdownCtx)
	})
}

// -----------------------------------------------------------------------------
// Core NATS
// -----------------------------------------------------------------------------

func TestNATS_CorePublishSubscribe(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct
	subject := "test.core.basic"

	publisher, err := corenats.NewPublisher(t.Context(), cfg)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	rec := &natsRecorder{} //nolint:exhaustruct
	subscriber, err := corenats.NewSubscriber[natsEvent](
		cfg, subject, rec, corenats.SubscriberHandlers[natsEvent]{},
	)
	require.NoError(t, err)

	runNATSRunner(t, subscriber)

	require.NoError(t, publisher.Flush(t.Context()))
	require.NoError(t, publisher.PublishJSON(t.Context(), subject, natsEvent{ID: 1, Text: "hi"}))

	require.Eventually(t, func() bool { return rec.Count() == 1 }, natsEventually, natsTick)
	assert.Equal(t, natsEvent{ID: 1, Text: "hi"}, rec.Snapshot()[0])
}

func TestNATS_CoreHeaders(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct
	subject := "test.core.headers"

	publisher, err := corenats.NewPublisher(t.Context(), cfg)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	rec := &natsRecorder{} //nolint:exhaustruct
	subscriber, err := corenats.NewSubscriber[natsEvent](
		cfg, subject, rec, corenats.SubscriberHandlers[natsEvent]{},
	)
	require.NoError(t, err)

	runNATSRunner(t, subscriber)
	require.NoError(t, publisher.Flush(t.Context()))

	headers := nats.Header{"X-Tenant": []string{"acme"}, "Trace-ID": []string{"abc-123"}}
	require.NoError(t, publisher.PublishJSON(t.Context(), subject, natsEvent{ID: 7, Text: "h"},
		corenats.WithHeaders(headers)))

	require.Eventually(t, func() bool { return rec.Count() == 1 }, natsEventually, natsTick)

	rec.mu.Lock()
	defer rec.mu.Unlock()

	assert.Equal(t, "acme", rec.headers[0].Get("X-Tenant"))
	assert.Equal(t, "abc-123", rec.headers[0].Get("Trace-ID"))
}

func TestNATS_CoreQueueGroupFanout(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct
	subject := "test.core.qg"

	publisher, err := corenats.NewPublisher(t.Context(), cfg)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	recA := &natsRecorder{} //nolint:exhaustruct
	recB := &natsRecorder{} //nolint:exhaustruct

	subA, err := corenats.NewSubscriber[natsEvent](
		cfg, subject, recA, corenats.SubscriberHandlers[natsEvent]{},
		corenats.WithSubscriberQueueGroup("workers"),
	)
	require.NoError(t, err)

	subB, err := corenats.NewSubscriber[natsEvent](
		cfg, subject, recB, corenats.SubscriberHandlers[natsEvent]{},
		corenats.WithSubscriberQueueGroup("workers"),
	)
	require.NoError(t, err)

	runNATSRunner(t, subA)
	runNATSRunner(t, subB)
	require.NoError(t, publisher.Flush(t.Context()))

	const total = 40
	for i := range total {
		require.NoError(t, publisher.PublishJSON(t.Context(), subject, natsEvent{ID: i, Text: "x"}))
	}

	require.Eventually(t,
		func() bool { return recA.Count()+recB.Count() == total },
		natsEventually, natsTick)

	assert.Positive(t, recA.Count(), "subscriber A received nothing")
	assert.Positive(t, recB.Count(), "subscriber B received nothing")
	assert.Equal(t, total, recA.Count()+recB.Count())
}

func TestNATS_CoreErrorHandler(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct
	subject := "test.core.err"

	publisher, err := corenats.NewPublisher(t.Context(), cfg)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	rec := &natsRecorder{} //nolint:exhaustruct
	rec.failN.Store(1)

	var (
		mu      sync.Mutex
		handled int
	)

	subscriber, err := corenats.NewSubscriber[natsEvent](
		cfg, subject, rec,
		corenats.SubscriberHandlers[natsEvent]{
			ErrorHandler: func(_ context.Context, _ corenats.Message[natsEvent], _ error) {
				mu.Lock()
				defer mu.Unlock()

				handled++
			},
		},
	)
	require.NoError(t, err)

	runNATSRunner(t, subscriber)
	require.NoError(t, publisher.Flush(t.Context()))

	require.NoError(t, publisher.PublishJSON(t.Context(), subject, natsEvent{ID: 1, Text: "x"}))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return handled == 1
	}, natsEventually, natsTick)
}

func TestNATS_SharedConnection(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct
	subject := "test.core.shared"

	publisher, err := corenats.NewPublisher(t.Context(), cfg)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	rec := &natsRecorder{} //nolint:exhaustruct
	subscriber, err := corenats.NewSubscriber[natsEvent](
		cfg, subject, rec, corenats.SubscriberHandlers[natsEvent]{},
		corenats.WithConnection(publisher.Conn()),
	)
	require.NoError(t, err)

	runNATSRunner(t, subscriber)
	require.NoError(t, publisher.Flush(t.Context()))
	require.NoError(t, publisher.PublishJSON(t.Context(), subject, natsEvent{ID: 99, Text: "shared"}))

	require.Eventually(t, func() bool { return rec.Count() == 1 }, natsEventually, natsTick)

	// Subscriber must NOT close the shared connection on its Shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, subscriber.Shutdown(shutdownCtx))
	assert.False(t, publisher.Conn().IsClosed())
}

func TestNATS_DoubleRun(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct

	rec := &natsRecorder{} //nolint:exhaustruct
	subscriber, err := corenats.NewSubscriber[natsEvent](
		cfg, "test.double", rec, corenats.SubscriberHandlers[natsEvent]{},
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- subscriber.Run(runCtx) }()

	<-subscriber.Ready()

	err = subscriber.Run(t.Context())
	require.ErrorIs(t, err, corenats.ErrAlreadyRunning)

	cancel()
	<-done
}

// -----------------------------------------------------------------------------
// JetStream
// -----------------------------------------------------------------------------

func setupJetStream(
	t *testing.T,
	streamName string,
	subjects ...string,
) (corenats.Config, *corenats.Publisher) {
	t.Helper()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct

	publisher, err := corenats.NewPublisher(t.Context(), cfg,
		corenats.WithPublisherStream(jetstream.StreamConfig{ //nolint:exhaustruct
			Name:     streamName,
			Subjects: subjects,
		}),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	return cfg, publisher
}

func TestNATS_JetStreamPublishConsume(t *testing.T) {
	t.Parallel()

	subject := "test.js.basic"
	cfg, publisher := setupJetStream(t, "JS_BASIC", subject)

	rec := &natsRecorder{} //nolint:exhaustruct
	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_BASIC", "worker", rec,
		corenats.JSConsumerHandlers[natsEvent]{},
		corenats.WithJSSubjects(subject),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer)

	ack, err := publisher.PublishJSONJS(t.Context(), subject, natsEvent{ID: 11, Text: "js"})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ack.Sequence)

	require.Eventually(t, func() bool { return rec.Count() == 1 }, natsEventually, natsTick)
	assert.Equal(t, natsEvent{ID: 11, Text: "js"}, rec.Snapshot()[0])

	rec.mu.Lock()
	defer rec.mu.Unlock()

	assert.Equal(t, uint64(1), rec.seq[0])
	assert.Equal(t, uint64(1), rec.delivers[0])
}

func TestNATS_JetStreamMsgIDDedup(t *testing.T) {
	t.Parallel()

	subject := "test.js.dedup"
	_, publisher := setupJetStream(t, "JS_DEDUP", subject)

	ack1, err := publisher.PublishJSONJS(t.Context(), subject, natsEvent{ID: 1, Text: "a"},
		corenats.WithJSMsgID("msg-1"))
	require.NoError(t, err)
	assert.False(t, ack1.Duplicate)

	ack2, err := publisher.PublishJSONJS(t.Context(), subject, natsEvent{ID: 1, Text: "a"},
		corenats.WithJSMsgID("msg-1"))
	require.NoError(t, err)
	assert.True(t, ack2.Duplicate, "second publish with same MsgID must be flagged duplicate")
}

func TestNATS_JetStreamNakRedelivery(t *testing.T) {
	t.Parallel()

	subject := "test.js.nak"
	cfg, publisher := setupJetStream(t, "JS_NAK", subject)

	rec := &natsRecorder{} //nolint:exhaustruct
	rec.failN.Store(2)     // fail twice, succeed on 3rd delivery

	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_NAK", "worker", rec,
		corenats.JSConsumerHandlers[natsEvent]{},
		corenats.WithJSSubjects(subject),
		corenats.WithJSConsumerConfig(func(c *jetstream.ConsumerConfig) {
			c.AckWait = 100 * time.Millisecond
			c.MaxDeliver = 5
		}),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer)

	_, err = publisher.PublishJSONJS(t.Context(), subject, natsEvent{ID: 1, Text: "redeliver"})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return rec.Count() >= 3 }, natsEventually, natsTick)

	delivers := rec.DeliverCounts()
	assert.GreaterOrEqual(t, delivers[len(delivers)-1], uint64(3))
}

func TestNATS_JetStreamErrorActionTerm(t *testing.T) {
	t.Parallel()

	subject := "test.js.term"
	cfg, publisher := setupJetStream(t, "JS_TERM", subject)

	rec := &natsRecorder{} //nolint:exhaustruct
	rec.failN.Store(100)   // always fail

	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_TERM", "worker", rec,
		corenats.JSConsumerHandlers[natsEvent]{
			ErrorHandler: func(
				_ context.Context, _ corenats.Message[natsEvent], _ error,
			) (corenats.JSErrorAction, time.Duration) {
				return corenats.JSErrorActionTerm, 0
			},
		},
		corenats.WithJSSubjects(subject),
		corenats.WithJSConsumerConfig(func(c *jetstream.ConsumerConfig) {
			c.AckWait = 100 * time.Millisecond
			c.MaxDeliver = 10
		}),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer)

	_, err = publisher.PublishJSONJS(t.Context(), subject, natsEvent{ID: 1, Text: "poison"})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return rec.Count() >= 1 }, natsEventually, natsTick)

	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 1, rec.Count(), "Term must prevent redelivery")
}

func TestNATS_JetStreamErrorActionAck(t *testing.T) {
	t.Parallel()

	subject := "test.js.ackerr"
	cfg, publisher := setupJetStream(t, "JS_ACKERR", subject)

	rec := &natsRecorder{} //nolint:exhaustruct
	rec.failN.Store(100)

	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_ACKERR", "worker", rec,
		corenats.JSConsumerHandlers[natsEvent]{
			ErrorHandler: func(
				_ context.Context, _ corenats.Message[natsEvent], _ error,
			) (corenats.JSErrorAction, time.Duration) {
				return corenats.JSErrorActionAck, 0
			},
		},
		corenats.WithJSSubjects(subject),
		corenats.WithJSConsumerConfig(func(c *jetstream.ConsumerConfig) {
			c.AckWait = 100 * time.Millisecond
		}),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer)

	_, err = publisher.PublishJSONJS(t.Context(), subject, natsEvent{ID: 1, Text: "swallow"})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return rec.Count() == 1 }, natsEventually, natsTick)

	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 1, rec.Count())
}

func TestNATS_JetStreamErrorActionStop(t *testing.T) {
	t.Parallel()

	subject := "test.js.stop"
	cfg, publisher := setupJetStream(t, "JS_STOP", subject)

	rec := &natsRecorder{} //nolint:exhaustruct
	rec.failN.Store(1)

	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_STOP", "worker", rec,
		corenats.JSConsumerHandlers[natsEvent]{
			ErrorHandler: func(
				_ context.Context, _ corenats.Message[natsEvent], _ error,
			) (corenats.JSErrorAction, time.Duration) {
				return corenats.JSErrorActionStop, 0
			},
		},
		corenats.WithJSSubjects(subject),
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(runCtx) }()

	t.Cleanup(func() {
		cancel()

		_ = consumer.Shutdown(t.Context())
	})

	_, err = publisher.PublishJSONJS(t.Context(), subject, natsEvent{ID: 1, Text: "fatal"})
	require.NoError(t, err)

	select {
	case err := <-runErr:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "jetstream consumer stopped")
	case <-time.After(natsEventually):
		t.Fatal("Run did not return after JSErrorActionStop")
	}
}

func TestNATS_JetStreamMultiSubjectFilter(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct

	publisher, err := corenats.NewPublisher(t.Context(), cfg,
		corenats.WithPublisherStream(jetstream.StreamConfig{ //nolint:exhaustruct
			Name:     "JS_MULTI",
			Subjects: []string{"a.>", "b.>", "c.>"},
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	rec := &natsRecorder{} //nolint:exhaustruct
	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_MULTI", "worker", rec,
		corenats.JSConsumerHandlers[natsEvent]{},
		corenats.WithJSSubjects("a.>", "b.>"),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer)

	for _, subj := range []string{"a.evt", "b.evt", "c.evt"} {
		_, err = publisher.PublishJSONJS(t.Context(), subj, natsEvent{ID: 1, Text: subj})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return rec.Count() == 2 }, natsEventually, natsTick)

	got := map[string]bool{}
	for _, e := range rec.Snapshot() {
		got[e.Text] = true
	}

	assert.True(t, got["a.evt"])
	assert.True(t, got["b.evt"])
	assert.False(t, got["c.evt"], "c.evt must be filtered out")
}

type processorFunc[I any] func(ctx context.Context, msg corenats.Message[I]) error

func (f processorFunc[I]) Process(ctx context.Context, msg corenats.Message[I]) error {
	return f(ctx, msg)
}

func TestNATS_JetStreamWorkerCountParallel(t *testing.T) {
	t.Parallel()

	subject := "test.js.parallel"
	cfg, publisher := setupJetStream(t, "JS_PAR", subject)

	var (
		mu       sync.Mutex
		count    int
		inflight atomic.Int32
		peak     atomic.Int32
		gate     = make(chan struct{})
	)

	proc := processorFunc[natsEvent](func(_ context.Context, _ corenats.Message[natsEvent]) error {
		inflight.Add(1)

		for {
			cur := peak.Load()

			cur2 := inflight.Load()
			if cur2 <= cur || peak.CompareAndSwap(cur, cur2) {
				break
			}
		}

		mu.Lock()
		count++
		mu.Unlock()

		<-gate
		inflight.Add(-1)

		return nil
	})

	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_PAR", "worker", proc,
		corenats.JSConsumerHandlers[natsEvent]{},
		corenats.WithJSSubjects(subject),
		corenats.WithJSWorkerCount(3),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer)

	for i := range 3 {
		_, err = publisher.PublishJSONJS(t.Context(), subject, natsEvent{ID: i, Text: "p"})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return peak.Load() == 3 }, natsEventually, natsTick)
	close(gate)
}

func TestNATS_JetStreamCustomMarshaller(t *testing.T) {
	t.Parallel()

	subject := "test.js.marshal"
	cfg, publisher := setupJetStream(t, "JS_MARSHAL", subject)

	rec := &natsRecorder{} //nolint:exhaustruct

	var called atomic.Int32

	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_MARSHAL", "worker", rec,
		corenats.JSConsumerHandlers[natsEvent]{
			Marshaller: func(body []byte, p *natsEvent) error {
				called.Add(1)

				p.ID = 999
				p.Text = string(body)

				return nil
			},
		},
		corenats.WithJSSubjects(subject),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer)

	_, err = publisher.PublishJS(t.Context(), subject, []byte("raw-payload"))
	require.NoError(t, err)

	require.Eventually(t, func() bool { return rec.Count() == 1 }, natsEventually, natsTick)
	assert.Equal(t, int32(1), called.Load())

	snap := rec.Snapshot()
	assert.Equal(t, 999, snap[0].ID)
	assert.Equal(t, "raw-payload", snap[0].Text)
}

func TestNATS_JetStreamMarshallerError(t *testing.T) {
	t.Parallel()

	subject := "test.js.badjson"
	cfg, publisher := setupJetStream(t, "JS_BADJSON", subject)

	rec := &natsRecorder{} //nolint:exhaustruct

	var errSeen atomic.Int32

	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_BADJSON", "worker", rec,
		corenats.JSConsumerHandlers[natsEvent]{
			ErrorHandler: func(
				_ context.Context, _ corenats.Message[natsEvent], _ error,
			) (corenats.JSErrorAction, time.Duration) {
				errSeen.Add(1)

				return corenats.JSErrorActionTerm, 0
			},
		},
		corenats.WithJSSubjects(subject),
		corenats.WithJSConsumerConfig(func(c *jetstream.ConsumerConfig) {
			c.AckWait = 100 * time.Millisecond
			c.MaxDeliver = 3
		}),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer)

	_, err = publisher.PublishJS(t.Context(), subject, []byte("not-json"))
	require.NoError(t, err)

	require.Eventually(t, func() bool { return errSeen.Load() >= 1 }, natsEventually, natsTick)
	assert.Zero(t, rec.Count(), "processor must NOT see undecodable payload")
}

func TestNATS_JSONPublisher(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct
	subject := "test.jp.evt"

	publisher, err := corenats.NewPublisher(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	jp := corenats.NewJSONPublisher[natsEvent](publisher, subject)

	rec := &natsRecorder{} //nolint:exhaustruct
	subscriber, err := corenats.NewSubscriber[natsEvent](
		cfg, subject, rec, corenats.SubscriberHandlers[natsEvent]{},
	)
	require.NoError(t, err)

	runNATSRunner(t, subscriber)
	require.NoError(t, publisher.Flush(t.Context()))
	require.NoError(t, jp.Publish(t.Context(), natsEvent{ID: 1, Text: "typed"}))

	require.Eventually(t, func() bool { return rec.Count() == 1 }, natsEventually, natsTick)
}
