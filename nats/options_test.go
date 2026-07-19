//nolint:testpackage // white-box test: validates unexported option-application helpers.
package nats

import (
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommonOptions_applyToPublisher(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	conn := &nats.Conn{}
	extraOpt := nats.Name("override")

	o := defaultPublisherOptions()
	for _, opt := range []PublisherOption{
		WithLogger(logger),
		WithConnection(conn),
		WithNATSOptions(extraOpt),
		WithOtel(),
		WithPublisherStream(jetstream.StreamConfig{Name: "S"}),
	} {
		opt.applyToPublisher(&o)
	}

	assert.Same(t, logger, o.common.logger)
	assert.Same(t, conn, o.common.conn)
	assert.Len(t, o.common.extraNATS, 1)
	assert.True(t, o.common.otelEnabled)
	require.NotNil(t, o.streamConfig)
	assert.Equal(t, "S", o.streamConfig.Name)
}

func TestCommonOptions_applyToSubscriber(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	conn := &nats.Conn{}

	o := defaultSubscriberOptions()
	for _, opt := range []SubscriberOption{
		WithLogger(logger),
		WithConnection(conn),
		WithOtel(),
		WithSubscriberQueueGroup("workers"),
		WithSubscriberWorkerCount(4),
	} {
		opt.applyToSubscriber(&o)
	}

	assert.Same(t, logger, o.common.logger)
	assert.Same(t, conn, o.common.conn)
	assert.True(t, o.common.otelEnabled)
	assert.Equal(t, "workers", o.queueGroup)
	assert.Equal(t, 4, o.workerCount)
}

func TestCommonOptions_applyToJSConsumer(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)

	o := defaultJSConsumerOptions()

	called := false
	mutate := func(cfg *jetstream.ConsumerConfig) {
		called = true
		cfg.MaxDeliver = 7
	}

	for _, opt := range []JSConsumerOption{
		WithLogger(logger),
		WithJSStream(jetstream.StreamConfig{Name: "STREAM"}),
		WithJSSubjects("a.>", "b.>"),
		WithJSConsumerConfig(mutate),
		WithJSWorkerCount(3),
	} {
		opt.applyToJSConsumer(&o)
	}

	assert.Same(t, logger, o.common.logger)
	require.NotNil(t, o.streamConfig)
	assert.Equal(t, "STREAM", o.streamConfig.Name)
	assert.Equal(t, []string{"a.>", "b.>"}, o.subjectFilters)
	require.NotNil(t, o.consumerMutate)
	assert.Equal(t, 3, o.workerCount)

	cfg := jetstream.ConsumerConfig{}
	o.consumerMutate(&cfg)
	assert.True(t, called)
	assert.Equal(t, 7, cfg.MaxDeliver)
}

func TestWithLogger_nilIgnored(t *testing.T) {
	t.Parallel()

	o := defaultPublisherOptions()
	original := o.common.logger

	WithLogger(nil).applyToPublisher(&o)

	assert.Same(t, original, o.common.logger, "nil logger must be ignored")
}

func TestWorkerCount_normalizedToOne(t *testing.T) {
	t.Parallel()

	t.Run("subscriber", func(t *testing.T) {
		t.Parallel()

		o := defaultSubscriberOptions()
		WithSubscriberWorkerCount(0).applyToSubscriber(&o)
		assert.Equal(t, 1, o.workerCount)

		WithSubscriberWorkerCount(-5).applyToSubscriber(&o)
		assert.Equal(t, 1, o.workerCount)
	})

	t.Run("jsconsumer", func(t *testing.T) {
		t.Parallel()

		o := defaultJSConsumerOptions()
		WithJSWorkerCount(0).applyToJSConsumer(&o)
		assert.Equal(t, 1, o.workerCount)

		WithJSWorkerCount(-3).applyToJSConsumer(&o)
		assert.Equal(t, 1, o.workerCount)
	})
}

func TestBuildConsumerConfig_defaults(t *testing.T) {
	t.Parallel()

	cfg := buildConsumerConfig("my-durable", nil, nil)
	assert.Equal(t, "my-durable", cfg.Durable)
	assert.Equal(t, jetstream.AckExplicitPolicy, cfg.AckPolicy)
	assert.Empty(t, cfg.FilterSubject)
	assert.Empty(t, cfg.FilterSubjects)
}

func TestBuildConsumerConfig_singleSubject(t *testing.T) {
	t.Parallel()

	cfg := buildConsumerConfig("d", []string{"events.>"}, nil)
	assert.Equal(t, "events.>", cfg.FilterSubject)
	assert.Empty(t, cfg.FilterSubjects)
}

func TestBuildConsumerConfig_multiSubject(t *testing.T) {
	t.Parallel()

	cfg := buildConsumerConfig("d", []string{"a.>", "b.>"}, nil)
	assert.Empty(t, cfg.FilterSubject)
	assert.Equal(t, []string{"a.>", "b.>"}, cfg.FilterSubjects)
}

func TestBuildConsumerConfig_mutatorPreservesDurable(t *testing.T) {
	t.Parallel()

	mutate := func(cfg *jetstream.ConsumerConfig) {
		// Wipe everything — durable must be re-asserted by buildConsumerConfig.
		*cfg = jetstream.ConsumerConfig{}
	}

	cfg := buildConsumerConfig("must-stick", []string{"x"}, mutate)
	assert.Equal(t, "must-stick", cfg.Durable)
}

func TestBuildConsumerConfig_mutatorCanOverrideAckPolicy(t *testing.T) {
	t.Parallel()

	mutate := func(cfg *jetstream.ConsumerConfig) {
		cfg.AckPolicy = jetstream.AckAllPolicy
	}

	cfg := buildConsumerConfig("d", nil, mutate)
	assert.Equal(t, jetstream.AckAllPolicy, cfg.AckPolicy)
}

func TestBuildConsumerConfig_mutatorAckPolicyNoneResetToExplicit(t *testing.T) {
	t.Parallel()

	// A mutator that wipes the struct (or just forgets AckPolicy) used to
	// leave AckPolicy at AckNonePolicy (zero) — silently downgrading
	// at-least-once to at-most-once. buildConsumerConfig now re-asserts
	// AckExplicit when the mutator leaves it at None.
	mutate := func(cfg *jetstream.ConsumerConfig) {
		*cfg = jetstream.ConsumerConfig{
			MaxDeliver: 5,
		}
	}

	cfg := buildConsumerConfig("d", nil, mutate)
	assert.Equal(t, jetstream.AckExplicitPolicy, cfg.AckPolicy)
	assert.Equal(t, 5, cfg.MaxDeliver)
}

func TestSubscriberChannelBuffer(t *testing.T) {
	t.Parallel()

	o := defaultSubscriberOptions()
	assert.Equal(t, defaultSubscriberChannelBuffer, o.channelBuffer)

	WithSubscriberChannelBuffer(1024).applyToSubscriber(&o)
	assert.Equal(t, 1024, o.channelBuffer)

	WithSubscriberChannelBuffer(0).applyToSubscriber(&o)
	assert.Equal(t, defaultSubscriberChannelBuffer, o.channelBuffer, "non-positive reverts to default")
}

func TestJSDeliveryBuffer(t *testing.T) {
	t.Parallel()

	o := defaultJSConsumerOptions()
	assert.Equal(t, defaultJSConsumerDeliveryBuffer, o.deliveryBuffer)

	WithJSDeliveryBuffer(512).applyToJSConsumer(&o)
	assert.Equal(t, 512, o.deliveryBuffer)

	WithJSDeliveryBuffer(-1).applyToJSConsumer(&o)
	assert.Equal(t, defaultJSConsumerDeliveryBuffer, o.deliveryBuffer)
}

func TestJSAssumeExistingConsumer(t *testing.T) {
	t.Parallel()

	o := defaultJSConsumerOptions()
	assert.False(t, o.assumeExistingOnly)

	WithJSAssumeExistingConsumer().applyToJSConsumer(&o)
	assert.True(t, o.assumeExistingOnly)
}

func TestBuildJSPublishOpts_empty(t *testing.T) {
	t.Parallel()

	out := buildJSPublishOpts(JSPublishOpts{})
	assert.Empty(t, out)
}

func TestBuildJSPublishOpts_populated(t *testing.T) {
	t.Parallel()

	opts := JSPublishOpts{
		Headers:                nats.Header{"X-Tenant": []string{"acme"}},
		MsgID:                  "msg-1",
		ExpectedStream:         "EVENTS",
		ExpectedLastSequence:   new(uint64(42)),
		ExpectedLastSubjectSeq: new(uint64(7)),
		ExpectedLastMsgID:      "prev",
		RetryAttempts:          5,
	}

	out := buildJSPublishOpts(opts)
	assert.Len(t, out, 6)
}

func TestPublishOption_helpers(t *testing.T) {
	t.Parallel()

	o := PublishOpts{}
	WithHeaders(nats.Header{"K": []string{"V"}})(&o)
	WithReply("inbox.1")(&o)

	assert.Equal(t, "V", o.Headers.Get("K"))
	assert.Equal(t, "inbox.1", o.Reply)
}

func TestJSPublishOption_helpers(t *testing.T) {
	t.Parallel()

	o := JSPublishOpts{}
	WithJSHeaders(nats.Header{"K": []string{"V"}})(&o)
	WithJSMsgID("id")(&o)
	WithJSExpectedStream("S")(&o)
	WithJSExpectedLastSequence(1)(&o)
	WithJSExpectedLastSubjectSequence(2)(&o)
	WithJSExpectedLastMsgID("prev")(&o)
	WithJSRetryAttempts(0)(&o) // ignored
	WithJSRetryAttempts(3)(&o)

	assert.Equal(t, "V", o.Headers.Get("K"))
	assert.Equal(t, "id", o.MsgID)
	assert.Equal(t, "S", o.ExpectedStream)
	require.NotNil(t, o.ExpectedLastSequence)
	assert.Equal(t, uint64(1), *o.ExpectedLastSequence)
	require.NotNil(t, o.ExpectedLastSubjectSeq)
	assert.Equal(t, uint64(2), *o.ExpectedLastSubjectSeq)
	assert.Equal(t, "prev", o.ExpectedLastMsgID)
	assert.Equal(t, 3, o.RetryAttempts)
}

func TestDefaults_subscriber(t *testing.T) {
	t.Parallel()

	o := defaultSubscriberOptions()
	assert.NotNil(t, o.common.logger)
	assert.Nil(t, o.common.conn)
	assert.False(t, o.common.otelEnabled)
	assert.Equal(t, 1, o.workerCount)
}

func TestDefaults_jsconsumer(t *testing.T) {
	t.Parallel()

	o := defaultJSConsumerOptions()
	assert.NotNil(t, o.common.logger)
	assert.Equal(t, 1, o.workerCount)
	assert.Nil(t, o.consumerMutate)
	assert.Empty(t, o.subjectFilters)
}

// Compile-time check: defaults are stable shape.
var _ = time.Second
