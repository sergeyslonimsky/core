package integration_test

import (
	"context"
	"fmt"
	"strings"
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

// TestNATS_PublisherShutdownFlushOnSharedConn ensures Publisher.Shutdown
// flushes pending Core NATS sends even when the connection is shared
// (WithConnection) — i.e. the publisher does NOT own it. Regression test
// for the silent-drop bug where Shutdown skipped Flush entirely for shared
// connections.
func TestNATS_PublisherShutdownFlushOnSharedConn(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)

	owner, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(owner.Close)

	const subject = "test.shared.flush"

	received := make(chan []byte, 1)
	_, err = owner.Subscribe(subject, func(m *nats.Msg) { received <- m.Data })
	require.NoError(t, err)
	require.NoError(t, owner.Flush())

	cfg := corenats.Config{URL: url} //nolint:exhaustruct
	publisher, err := corenats.NewPublisher(t.Context(), cfg, corenats.WithConnection(owner))
	require.NoError(t, err)

	require.NoError(t, publisher.Publish(t.Context(), subject, []byte("payload")))

	// Shutdown must flush even though it does not own the conn — otherwise
	// the payload remains buffered client-side and the assertion below
	// times out.
	require.NoError(t, publisher.Shutdown(t.Context()))

	select {
	case got := <-received:
		assert.Equal(t, []byte("payload"), got)
	case <-time.After(natsEventually):
		t.Fatal("Shutdown did not flush — message lost")
	}

	// Shared connection must remain alive.
	assert.False(t, owner.IsClosed(), "shared connection must NOT be closed by Publisher.Shutdown")
}

// TestNATS_MaxReconnectsPointerSemantics validates the three branches of
// Config.MaxReconnects *int: nil → nats.go default (60), explicit 0 →
// no retries, explicit MaxReconnectsForever (-1) → forever.
func TestNATS_MaxReconnectsPointerSemantics(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)

	cases := []struct {
		name string
		val  *int
		want int
	}{
		{"nil uses nats.go default", nil, nats.DefaultMaxReconnect},
		{"explicit zero = no retries", corenats.IntPtr(0), 0},
		{"explicit forever", corenats.IntPtr(corenats.MaxReconnectsForever), -1},
		{"explicit cap", corenats.IntPtr(7), 7},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := corenats.Config{URL: url, MaxReconnects: tc.val} //nolint:exhaustruct
			pub, err := corenats.NewPublisher(t.Context(), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = pub.Shutdown(t.Context()) })

			assert.Equal(t, tc.want, pub.Conn().Opts.MaxReconnect)
		})
	}
}

// TestNATS_SubscriberReadyClosesOnSubscribeError verifies that Ready()
// unblocks when subscribe fails — otherwise downstream code blocking on
// Ready() would deadlock forever.
func TestNATS_SubscriberReadyClosesOnSubscribeError(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct

	rec := &natsRecorder{} //nolint:exhaustruct
	// "bad..subject" with consecutive dots is illegal — ChanSubscribe errors.
	sub, err := corenats.NewSubscriber[natsEvent](
		cfg, "bad..subject", rec, corenats.SubscriberHandlers[natsEvent]{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Shutdown(t.Context()) })

	runErr := make(chan error, 1)
	go func() { runErr <- sub.Run(t.Context()) }()

	select {
	case <-sub.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("Ready() did not close after subscribe error")
	}

	select {
	case err := <-runErr:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "subscribe to subject")
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after subscribe error")
	}
}

// TestNATS_SubscriberReadyReArms validates that the Ready() channel rotates
// across Run cycles — a second Run hands out a fresh, not-yet-closed
// channel rather than re-using the closed one from the prior cycle.
func TestNATS_SubscriberReadyReArms(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct

	rec := &natsRecorder{} //nolint:exhaustruct
	sub, err := corenats.NewSubscriber[natsEvent](
		cfg, "test.rearm.sub", rec, corenats.SubscriberHandlers[natsEvent]{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Shutdown(t.Context()) })

	// Cycle 1.
	ctx1, cancel1 := context.WithCancel(t.Context())

	d1 := make(chan error, 1)
	go func() { d1 <- sub.Run(ctx1) }()

	first := sub.Ready()
	<-first
	cancel1()
	require.NoError(t, <-d1)

	// Cycle 2 — the channel returned by Ready() must be a different one.
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()

	d2 := make(chan error, 1)
	go func() { d2 <- sub.Run(ctx2) }()

	require.Eventually(t, func() bool { return sub.Ready() != first },
		natsEventually, natsTick,
		"Ready() did not rotate on second Run")

	<-sub.Ready()

	cancel2()
	require.NoError(t, <-d2)
}

// TestNATS_PublishJSONDoesNotMutateSharedHeaders proves at the
// integration boundary that PublishJSON does not accumulate Content-Type
// in a caller's shared nats.Header across multiple publishes.
func TestNATS_PublishJSONDoesNotMutateSharedHeaders(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct

	pub, err := corenats.NewPublisher(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Shutdown(t.Context()) })

	shared := nats.Header{"X-Trace": []string{"abc"}}

	for range 3 {
		require.NoError(t, pub.PublishJSON(t.Context(), "test.hdr.shared",
			natsEvent{ID: 1, Text: "x"}, corenats.WithHeaders(shared)))
	}

	assert.Equal(t, nats.Header{"X-Trace": []string{"abc"}}, shared,
		"PublishJSON must not mutate caller's nats.Header across calls")
}

// TestNATS_ShutdownCtxExpiry verifies that Subscriber.Shutdown returns the
// ctx error when its bounded context expires before Run completes, yet
// still closes the owned NATS connection so the resource does not leak.
func TestNATS_ShutdownCtxExpiry(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct

	gate := make(chan struct{})
	proc := processorFunc[natsEvent](func(_ context.Context, _ corenats.Message[natsEvent]) error {
		<-gate

		return nil
	})

	sub, err := corenats.NewSubscriber[natsEvent](
		cfg, "test.shutdown.expiry", proc, corenats.SubscriberHandlers[natsEvent]{},
	)
	require.NoError(t, err)

	runDone := make(chan error, 1)
	go func() { runDone <- sub.Run(t.Context()) }()

	<-sub.Ready()

	pub, err := corenats.NewPublisher(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Shutdown(t.Context()) })

	require.NoError(t, pub.PublishJSON(t.Context(), "test.shutdown.expiry",
		natsEvent{ID: 1, Text: "block"}))

	// Wait until the processor is in flight.
	time.Sleep(200 * time.Millisecond)

	conn := sub.Conn()
	require.NotNil(t, conn)

	// Shutdown with a tight deadline — processor is stuck, so Shutdown
	// will time out. Must surface ctx.Err() but still close the owned conn.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = sub.Shutdown(shutdownCtx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
	assert.True(t, conn.IsClosed(),
		"Shutdown must close the owned connection even when ctx expired")

	// Release the processor so Run can finish; cleanup the goroutine.
	close(gate)

	select {
	case <-runDone:
	case <-time.After(natsEventually):
		// Connection is closed; Run should have returned by now.
	}
}

// TestNATS_JetStreamHandlersMarshallerAndErrorHandler exercises both
// marshaller AND error-handler passed via JSConsumerHandlers at construction
// — the new typed-handlers API supersedes the old SetMarshaller/SetErrorHandler
// setters and must still wire both callbacks through correctly.
func TestNATS_JetStreamHandlersMarshallerAndErrorHandler(t *testing.T) {
	t.Parallel()

	subject := "test.js.handlers"
	cfg, publisher := setupJetStream(t, "JS_HANDLERS", subject)

	var (
		marshallerCalled atomic.Int32
		errCalled        atomic.Int32
	)

	rec := &natsRecorder{} //nolint:exhaustruct

	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_HANDLERS", "worker", rec,
		corenats.JSConsumerHandlers[natsEvent]{
			Marshaller: func(body []byte, p *natsEvent) error {
				marshallerCalled.Add(1)

				if strings.HasPrefix(string(body), "BAD") {
					return fmt.Errorf("bad payload: %s", body)
				}

				p.Text = string(body)

				return nil
			},
			ErrorHandler: func(
				_ context.Context, _ corenats.Message[natsEvent], _ error,
			) (corenats.JSErrorAction, time.Duration) {
				errCalled.Add(1)

				return corenats.JSErrorActionTerm, 0
			},
		},
		corenats.WithJSSubjects(subject),
		corenats.WithJSConsumerConfig(func(c *jetstream.ConsumerConfig) {
			c.AckWait = 200 * time.Millisecond
			c.MaxDeliver = 3
		}),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer)

	// Good payload → marshaller called, processor runs, message acked.
	_, err = publisher.PublishJS(t.Context(), subject, []byte("hello"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return rec.Count() == 1 }, natsEventually, natsTick)

	// Bad payload → marshaller fails → applyAction → Term (no redelivery).
	_, err = publisher.PublishJS(t.Context(), subject, []byte("BAD-payload"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return errCalled.Load() >= 1 }, natsEventually, natsTick)

	// Confirm Term semantics: no redelivery for the bad message.
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 1, rec.Count(), "Term must prevent redelivery of bad payload")
	assert.GreaterOrEqual(t, marshallerCalled.Load(), int32(2))
}

// TestNATS_ConcurrentPublish makes sure many goroutines can call Publish
// against the same Publisher concurrently without data corruption — pin
// for the recent header-clone refactor where the publish path mutates
// internal state under the hood.
func TestNATS_ConcurrentPublish(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct

	pub, err := corenats.NewPublisher(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Shutdown(t.Context()) })

	rec := &natsRecorder{} //nolint:exhaustruct
	sub, err := corenats.NewSubscriber[natsEvent](
		cfg, "test.concurrent.pub", rec, corenats.SubscriberHandlers[natsEvent]{},
		corenats.WithSubscriberWorkerCount(8),
		corenats.WithSubscriberChannelBuffer(512),
	)
	require.NoError(t, err)
	runNATSRunner(t, sub)
	require.NoError(t, pub.Flush(t.Context()))

	const (
		goroutines = 16
		perGo      = 25
		total      = goroutines * perGo
	)

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for g := range goroutines {
		go func(g int) {
			defer wg.Done()

			for i := range perGo {
				_ = pub.PublishJSON(t.Context(), "test.concurrent.pub",
					natsEvent{ID: g*perGo + i, Text: "c"})
			}
		}(g)
	}

	wg.Wait()

	require.Eventually(t, func() bool { return rec.Count() == total },
		natsEventually*2, natsTick,
		"only %d/%d messages delivered", rec.Count(), total)
}
