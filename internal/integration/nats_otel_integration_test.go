package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/sergeyslonimsky/core/internal/integration/testhelpers"
	corenats "github.com/sergeyslonimsky/core/nats"
)

// withRecordingTracer installs a tracetest.SpanRecorder-backed
// TracerProvider + TraceContext propagator for the duration of the test.
// Returns the recorder so the test can inspect emitted spans.
func withRecordingTracer(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())

		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	return rec
}

// Cannot run in parallel — mutates global TracerProvider.
//
//nolint:paralleltest // mutates global TracerProvider
func TestNATS_OTel_PublishConsumeSpansChain_Core(t *testing.T) {
	rec := withRecordingTracer(t)

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct
	subject := "test.otel.core"

	publisher, err := corenats.NewPublisher(t.Context(), cfg, corenats.WithOtel())
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	recorder := &natsRecorder{} //nolint:exhaustruct
	subscriber, err := corenats.NewSubscriber[natsEvent](
		cfg, subject, recorder, corenats.SubscriberHandlers[natsEvent]{},
		corenats.WithOtel(),
	)
	require.NoError(t, err)

	runNATSRunner(t, subscriber)
	require.NoError(t, publisher.Flush(t.Context()))

	require.NoError(t, publisher.PublishJSON(t.Context(), subject, natsEvent{ID: 1, Text: "otel"}))

	require.Eventually(t, func() bool { return recorder.Count() == 1 }, natsEventually, natsTick)

	// Force span export.
	require.Eventually(t, func() bool { return len(rec.Ended()) >= 2 }, natsEventually, natsTick)

	spans := rec.Ended()

	var pub, con sdktrace.ReadOnlySpan

	for _, s := range spans {
		switch s.SpanKind() { //nolint:exhaustive // only producer/consumer kinds matter for this assertion
		case trace.SpanKindProducer:
			pub = s
		case trace.SpanKindConsumer:
			con = s
		}
	}

	require.NotNil(t, pub, "no producer span emitted")
	require.NotNil(t, con, "no consumer span emitted")

	assert.Contains(t, pub.Name(), subject)
	assert.Contains(t, con.Name(), subject)
	assert.Equal(t, pub.SpanContext().TraceID(), con.SpanContext().TraceID(),
		"consumer span must inherit trace ID from producer (TraceContext propagation)")
	assert.Equal(t, pub.SpanContext().SpanID(), con.Parent().SpanID(),
		"consumer span's parent must be the producer span")
}

// Cannot run in parallel — mutates global TracerProvider.
//
//nolint:paralleltest // mutates global TracerProvider
func TestNATS_OTel_PublishConsumeSpansChain_JetStream(t *testing.T) {
	rec := withRecordingTracer(t)

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct
	subject := "test.otel.js"

	publisher, err := corenats.NewPublisher(t.Context(), cfg,
		corenats.WithOtel(),
		corenats.WithPublisherStream(jetstream.StreamConfig{ //nolint:exhaustruct
			Name:     "OTEL_JS",
			Subjects: []string{subject},
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	recorder := &natsRecorder{} //nolint:exhaustruct
	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "OTEL_JS", "otel-worker", recorder,
		corenats.JSConsumerHandlers[natsEvent]{},
		corenats.WithJSSubjects(subject),
		corenats.WithOtel(),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer)

	_, err = publisher.PublishJSONJS(t.Context(), subject, natsEvent{ID: 2, Text: "otel-js"})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return recorder.Count() == 1 }, natsEventually, natsTick)
	require.Eventually(t, func() bool { return len(rec.Ended()) >= 2 }, natsEventually, natsTick)

	var pub, con sdktrace.ReadOnlySpan

	for _, s := range rec.Ended() {
		switch s.SpanKind() { //nolint:exhaustive // only producer/consumer kinds matter for this assertion
		case trace.SpanKindProducer:
			pub = s
		case trace.SpanKindConsumer:
			con = s
		}
	}

	require.NotNil(t, pub)
	require.NotNil(t, con)
	assert.Equal(t, pub.SpanContext().TraceID(), con.SpanContext().TraceID())
	assert.Equal(t, pub.SpanContext().SpanID(), con.Parent().SpanID())

	// JS consumer span has sequence + delivery_attempt attributes.
	attrs := con.Attributes()

	var seqSeen, deliverySeen bool

	for _, a := range attrs {
		switch string(a.Key) {
		case "messaging.message.sequence":
			seqSeen = true
		case "messaging.nats.delivery_attempt":
			deliverySeen = true
		}
	}

	assert.True(t, seqSeen, "JS consumer span missing messaging.message.sequence")
	assert.True(t, deliverySeen, "JS consumer span missing messaging.nats.delivery_attempt")
}

func TestNATS_JetStreamErrorActionNakDelay(t *testing.T) {
	t.Parallel()

	subject := "test.js.nakdelay"
	cfg, publisher := setupJetStream(t, "JS_NAKDELAY", subject)

	rec := &natsRecorder{} //nolint:exhaustruct
	rec.failN.Store(1)     // fail once, succeed on second delivery

	consumer, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_NAKDELAY", "worker", rec,
		corenats.JSConsumerHandlers[natsEvent]{
			ErrorHandler: func(
				_ context.Context, _ corenats.Message[natsEvent], _ error,
			) (corenats.JSErrorAction, time.Duration) {
				return corenats.JSErrorActionNakDelay, 200 * time.Millisecond
			},
		},
		corenats.WithJSSubjects(subject),
		corenats.WithJSConsumerConfig(func(c *jetstream.ConsumerConfig) {
			c.AckWait = 5 * time.Second // long, so without delay we'd wait forever
			c.MaxDeliver = 5
		}),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer)

	start := time.Now()

	_, err = publisher.PublishJSONJS(t.Context(), subject, natsEvent{ID: 1, Text: "delay"})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return rec.Count() >= 2 }, natsEventually, natsTick)

	elapsed := time.Since(start)
	// Second delivery must be at least the configured delay; AckWait is 5s so
	// without the explicit delay we'd be waiting that long. Allow some slack.
	assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond)
	assert.Less(t, elapsed, 4*time.Second, "NakDelay must redeliver well before AckWait expiry")
}

func TestNATS_AssumeExistingConsumer(t *testing.T) {
	t.Parallel()

	subject := "test.js.existing"
	cfg, publisher := setupJetStream(t, "JS_EXISTING", subject)

	// Step 1: create the durable consumer via the normal path.
	rec1 := &natsRecorder{} //nolint:exhaustruct
	consumer1, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_EXISTING", "the-worker", rec1,
		corenats.JSConsumerHandlers[natsEvent]{},
		corenats.WithJSSubjects(subject),
		corenats.WithJSConsumerConfig(func(c *jetstream.ConsumerConfig) {
			c.MaxDeliver = 7
		}),
	)
	require.NoError(t, err)
	require.NoError(t, consumer1.Shutdown(t.Context()))

	// Step 2: look up the existing consumer — must NOT clobber config.
	rec2 := &natsRecorder{} //nolint:exhaustruct
	consumer2, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_EXISTING", "the-worker", rec2,
		corenats.JSConsumerHandlers[natsEvent]{},
		corenats.WithJSAssumeExistingConsumer(),
	)
	require.NoError(t, err)

	runNATSRunner(t, consumer2)

	_, err = publisher.PublishJSONJS(t.Context(), subject, natsEvent{ID: 3, Text: "existing"})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return rec2.Count() == 1 }, natsEventually, natsTick)
}

func TestNATS_AssumeExistingConsumer_NotFound(t *testing.T) {
	t.Parallel()

	subject := "test.js.notfound"
	cfg, _ := setupJetStream(t, "JS_NOTFOUND", subject)

	rec := &natsRecorder{} //nolint:exhaustruct
	_, err := corenats.NewJetStreamConsumer[natsEvent](
		t.Context(), cfg, "JS_NOTFOUND", "never-created", rec,
		corenats.JSConsumerHandlers[natsEvent]{},
		corenats.WithJSAssumeExistingConsumer(),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lookup existing consumer")
}

func TestNATS_UnsupportedContentType(t *testing.T) {
	t.Parallel()

	url := testhelpers.RunInProcessNATS(t)
	cfg := corenats.Config{URL: url} //nolint:exhaustruct
	subject := "test.contenttype"

	publisher, err := corenats.NewPublisher(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	rec := &natsRecorder{} //nolint:exhaustruct
	errSeen := make(chan error, 1)

	subscriber, err := corenats.NewSubscriber[natsEvent](
		cfg, subject, rec,
		corenats.SubscriberHandlers[natsEvent]{
			ErrorHandler: func(_ context.Context, _ corenats.Message[natsEvent], err error) {
				select {
				case errSeen <- err:
				default:
				}
			},
		},
	)
	require.NoError(t, err)

	runNATSRunner(t, subscriber)
	require.NoError(t, publisher.Flush(t.Context()))

	// Publish with non-JSON Content-Type — default decoder must reject.
	require.NoError(t, publisher.Publish(t.Context(), subject, []byte("hello"),
		corenats.WithHeaders(nats.Header{"Content-Type": []string{"text/plain"}})))

	select {
	case err := <-errSeen:
		require.ErrorIs(t, err, corenats.ErrUnsupportedMessage)
	case <-time.After(natsEventually):
		t.Fatal("error handler never fired")
	}

	assert.Zero(t, rec.Count(), "processor must not see decoded payload")
}
