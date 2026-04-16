package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/internal/integration/testhelpers"
	"github.com/sergeyslonimsky/core/rabbitmq"
)

// e2eTestMessage is the typed payload used across rabbitmq E2E tests.
type e2eTestMessage struct {
	ID   int    `json:"id"`
	Text string `json:"text"`
}

// rabbitmqConfigFromURL decomposes an amqp://user:pass@host:port URL
// (returned by testhelpers.SetupRabbitMQContainer) into the rabbitmq.Config
// struct expected by the core API.
func rabbitmqConfigFromURL(t *testing.T, amqpURL string) rabbitmq.Config {
	t.Helper()

	u, err := url.Parse(amqpURL)
	require.NoError(t, err, "parse amqp url %q", amqpURL)

	password, _ := u.User.Password()

	return rabbitmq.Config{
		Host:     u.Hostname(),
		Port:     u.Port(),
		User:     u.User.Username(),
		Password: password,
	}
}

// recordingRabbitProcessor collects received payloads for assertions.
type recordingRabbitProcessor struct {
	name string

	mu       sync.Mutex
	received []e2eTestMessage
}

func (p *recordingRabbitProcessor) Process(
	_ context.Context,
	msg rabbitmq.Message[e2eTestMessage],
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.received = append(p.received, msg.Payload)

	return nil
}

func (p *recordingRabbitProcessor) GetName() string {
	if p.name == "" {
		return "test-e2e-processor"
	}

	return p.name
}

func (p *recordingRabbitProcessor) Snapshot() []e2eTestMessage {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]e2eTestMessage, len(p.received))
	copy(out, p.received)

	return out
}

func (p *recordingRabbitProcessor) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.received)
}

// startConsumerHost builds a ConsumerHost, registers the given consumers,
// runs it in a goroutine, and ensures it shuts down at test end.
func startConsumerHost(
	t *testing.T,
	cfg rabbitmq.Config,
	consumers ...func(*rabbitmq.ConsumerHost),
) *rabbitmq.ConsumerHost {
	t.Helper()

	host := rabbitmq.NewConsumerHost(cfg)
	for _, add := range consumers {
		add(host)
	}

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)

	go func() { done <- host.Run(ctx) }()

	t.Cleanup(func() {
		cancel()

		select {
		case <-done:
		case <-time.After(testhelpers.DefaultTestTimeout):
			t.Fatal("rabbitmq consumer host did not stop within timeout")
		}

		shutdownCtx, shutdownCancel := context.WithTimeout(
			context.Background(),
			testhelpers.ShortTestTimeout,
		)
		defer shutdownCancel()

		_ = host.Shutdown(shutdownCtx)
	})

	return host
}

// TestRabbitMQE2E_PublishToQueueAndConsume covers the simplest path:
// publish via the default (nameless) exchange to a named queue, consume
// via rabbitmq.Consumer. Exercises both rabbitmq.Publisher and the
// Consumer worker-pool lifecycle.
func TestRabbitMQE2E_PublishToQueueAndConsume(t *testing.T) {
	t.Parallel()

	amqpURL, cleanup := testhelpers.SetupRabbitMQContainer(t)
	defer cleanup()

	cfg := rabbitmqConfigFromURL(t, amqpURL)
	queueName := "test-e2e-queue-core"

	processor := &recordingRabbitProcessor{name: "queue-core"}
	consumer := rabbitmq.NewConsumer[e2eTestMessage](
		queueName, processor,
		rabbitmq.WithQueue(rabbitmq.QueueConfig{}), // declare the queue
	)

	startConsumerHost(t, cfg, func(h *rabbitmq.ConsumerHost) {
		h.AddConsumer(consumer)
	})

	pubCtx, pubCancel := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)
	defer pubCancel()

	publisher, err := rabbitmq.NewPublisher(pubCtx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, c := context.WithTimeout(context.Background(), testhelpers.ShortTestTimeout)
		defer c()

		_ = publisher.Shutdown(shutCtx)
	})

	messages := []e2eTestMessage{
		{ID: 1, Text: "first"},
		{ID: 2, Text: "second"},
		{ID: 3, Text: "third"},
	}

	for _, msg := range messages {
		require.NoError(t,
			publisher.Publish(t.Context(), "", queueName, msg, rabbitmq.PublishOpts{
				ContentType: "application/json",
				MessageID:   fmt.Sprintf("msg-%d", msg.ID),
			}),
		)
	}

	require.Eventually(t, func() bool {
		return processor.Count() == len(messages)
	}, testhelpers.DefaultTestTimeout, 100*time.Millisecond)

	got := processor.Snapshot()
	assert.ElementsMatch(t, messages, got)
}

// TestRabbitMQE2E_DirectExchange verifies that a Consumer configured with
// an exchange + queue + binding correctly routes a direct-exchange message
// to its queue.
func TestRabbitMQE2E_DirectExchange(t *testing.T) {
	t.Parallel()

	amqpURL, cleanup := testhelpers.SetupRabbitMQContainer(t)
	defer cleanup()

	cfg := rabbitmqConfigFromURL(t, amqpURL)

	const (
		exchange   = "test-direct-exchange-core"
		queueName  = "test-direct-queue-core"
		routingKey = "orders.created"
	)

	processor := &recordingRabbitProcessor{name: "direct-core"}
	consumer := rabbitmq.NewConsumer[e2eTestMessage](
		queueName, processor,
		rabbitmq.WithExchange(rabbitmq.ExchangeConfig{
			Name: exchange,
			Kind: rabbitmq.ExchangeKindDirect,
		}),
		rabbitmq.WithQueue(rabbitmq.QueueConfig{}),
		rabbitmq.WithBindQueue(rabbitmq.BindQueueConfig{
			Exchange:   exchange,
			RoutingKey: routingKey,
		}),
	)

	startConsumerHost(t, cfg, func(h *rabbitmq.ConsumerHost) {
		h.AddConsumer(consumer)
	})

	pubCtx, pubCancel := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)
	defer pubCancel()

	publisher, err := rabbitmq.NewPublisher(pubCtx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, c := context.WithTimeout(context.Background(), testhelpers.ShortTestTimeout)
		defer c()

		_ = publisher.Shutdown(shutCtx)
	})

	want := e2eTestMessage{ID: 42, Text: "routed"}
	require.NoError(
		t,
		publisher.Publish(
			t.Context(),
			exchange,
			routingKey,
			want,
			rabbitmq.PublishOpts{ContentType: "application/json"},
		),
	)

	require.Eventually(t, func() bool { return processor.Count() >= 1 },
		testhelpers.DefaultTestTimeout, 100*time.Millisecond)

	got := processor.Snapshot()
	require.Len(t, got, 1)
	assert.Equal(t, want, got[0])
}

// TestRabbitMQE2E_FanoutExchange verifies broadcast: a single publish to
// a fanout exchange reaches every bound queue. Uses two independent
// Consumer instances inside the same ConsumerHost.
func TestRabbitMQE2E_FanoutExchange(t *testing.T) {
	t.Parallel()

	amqpURL, cleanup := testhelpers.SetupRabbitMQContainer(t)
	defer cleanup()

	cfg := rabbitmqConfigFromURL(t, amqpURL)

	const (
		exchange = "test-fanout-exchange-core"
		queue1   = "test-fanout-queue-1-core"
		queue2   = "test-fanout-queue-2-core"
	)

	proc1 := &recordingRabbitProcessor{name: "fanout-1"}
	proc2 := &recordingRabbitProcessor{name: "fanout-2"}

	mkConsumer := func(queue string, p rabbitmq.Processor[e2eTestMessage]) *rabbitmq.Consumer[e2eTestMessage] {
		return rabbitmq.NewConsumer[e2eTestMessage](
			queue, p,
			rabbitmq.WithExchange(rabbitmq.ExchangeConfig{
				Name: exchange,
				Kind: rabbitmq.ExchangeKindFanout,
			}),
			rabbitmq.WithQueue(rabbitmq.QueueConfig{}),
			rabbitmq.WithBindQueue(rabbitmq.BindQueueConfig{Exchange: exchange}),
		)
	}

	startConsumerHost(t, cfg, func(h *rabbitmq.ConsumerHost) {
		h.AddConsumer(mkConsumer(queue1, proc1))
		h.AddConsumer(mkConsumer(queue2, proc2))
	})

	pubCtx, pubCancel := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)
	defer pubCancel()

	publisher, err := rabbitmq.NewPublisher(pubCtx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, c := context.WithTimeout(context.Background(), testhelpers.ShortTestTimeout)
		defer c()

		_ = publisher.Shutdown(shutCtx)
	})

	want := e2eTestMessage{ID: 7, Text: "broadcast"}

	// Publish in a loop until both queues have received the message.
	// A fanout exchange drops messages silently if no queue is bound at
	// publish time — and the two Consumer QueueBind calls race with the
	// Publisher's dial/publish. Retrying lets the test be robust without
	// reaching into rabbitroutine internals to check bind state.
	require.Eventually(t, func() bool {
		_ = publisher.Publish(t.Context(), exchange, "", want,
			rabbitmq.PublishOpts{ContentType: "application/json"})

		return proc1.Count() >= 1 && proc2.Count() >= 1
	}, testhelpers.DefaultTestTimeout, 200*time.Millisecond,
		"fanout: both queues must receive the message once bindings are in place")

	assert.Equal(t, want, proc1.Snapshot()[0])
	assert.Equal(t, want, proc2.Snapshot()[0])
}

// nackOnceProcessor fails the FIRST delivery it sees (causing Nack, which
// the broker may redeliver) and accepts every subsequent one. Used by the
// "worker pool drains Ack/Nack on shutdown" test.
type nackOnceProcessor struct {
	mu           sync.Mutex
	seen         atomic.Int32
	failFirstKey string
}

func (p *nackOnceProcessor) Process(_ context.Context, msg rabbitmq.Message[e2eTestMessage]) error {
	n := p.seen.Add(1)
	if n == 1 {
		p.mu.Lock()
		p.failFirstKey = msg.Payload.Text
		p.mu.Unlock()

		return fmt.Errorf("simulated failure on first delivery of %q", msg.Payload.Text)
	}

	return nil
}

func (p *nackOnceProcessor) GetName() string { return "nack-once" }

// TestRabbitMQE2E_WorkerPool verifies that a Consumer configured with
// multiple workers processes messages without panicking on shutdown —
// covers the Ack-after-close regression the worker-pool refactor fixed.
func TestRabbitMQE2E_WorkerPool(t *testing.T) {
	t.Parallel()

	amqpURL, cleanup := testhelpers.SetupRabbitMQContainer(t)
	defer cleanup()

	cfg := rabbitmqConfigFromURL(t, amqpURL)

	const (
		queueName   = "test-worker-pool-queue"
		workerCount = 4
		messages    = 20
	)

	processor := &recordingRabbitProcessor{name: "worker-pool"}
	consumer := rabbitmq.NewConsumer[e2eTestMessage](
		queueName, processor,
		rabbitmq.WithQueue(rabbitmq.QueueConfig{}),
		rabbitmq.WithWorkerCount(workerCount),
		rabbitmq.WithConsumeOpts(rabbitmq.ConsumeOpts{PrefetchCount: workerCount}),
	)

	startConsumerHost(t, cfg, func(h *rabbitmq.ConsumerHost) {
		h.AddConsumer(consumer)
	})

	pubCtx, pubCancel := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)
	defer pubCancel()

	publisher, err := rabbitmq.NewPublisher(pubCtx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, c := context.WithTimeout(context.Background(), testhelpers.ShortTestTimeout)
		defer c()

		_ = publisher.Shutdown(shutCtx)
	})

	for i := range messages {
		require.NoError(t,
			publisher.Publish(t.Context(), "", queueName,
				e2eTestMessage{ID: i, Text: fmt.Sprintf("wp-%d", i)},
				rabbitmq.PublishOpts{ContentType: "application/json"},
			),
		)
	}

	require.Eventually(t, func() bool { return processor.Count() == messages },
		testhelpers.DefaultTestTimeout, 100*time.Millisecond,
		"all messages should be processed across the worker pool")
}

// TestRabbitMQE2E_ProcessorErrorIsNacked verifies the Processor-error path:
// when Process returns non-nil, the Consumer Nacks the delivery. Since the
// test is single-shot (no requeue configured), the message is discarded
// by the broker — but the consumer stays healthy and keeps processing
// subsequent messages.
func TestRabbitMQE2E_ProcessorErrorIsNacked(t *testing.T) {
	t.Parallel()

	amqpURL, cleanup := testhelpers.SetupRabbitMQContainer(t)
	defer cleanup()

	cfg := rabbitmqConfigFromURL(t, amqpURL)

	const queueName = "test-nack-queue"

	processor := &nackOnceProcessor{}
	consumer := rabbitmq.NewConsumer[e2eTestMessage](
		queueName, processor,
		rabbitmq.WithQueue(rabbitmq.QueueConfig{}),
	)

	startConsumerHost(t, cfg, func(h *rabbitmq.ConsumerHost) {
		h.AddConsumer(consumer)
	})

	pubCtx, pubCancel := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)
	defer pubCancel()

	publisher, err := rabbitmq.NewPublisher(pubCtx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, c := context.WithTimeout(context.Background(), testhelpers.ShortTestTimeout)
		defer c()

		_ = publisher.Shutdown(shutCtx)
	})

	// Publish two messages: the first gets nacked (dropped by the broker,
	// since no DLX + no requeue); the second succeeds.
	require.NoError(t,
		publisher.Publish(t.Context(), "", queueName,
			e2eTestMessage{ID: 1, Text: "will-nack"},
			rabbitmq.PublishOpts{ContentType: "application/json"},
		),
	)
	require.NoError(t,
		publisher.Publish(t.Context(), "", queueName,
			e2eTestMessage{ID: 2, Text: "will-ack"},
			rabbitmq.PublishOpts{ContentType: "application/json"},
		),
	)

	// At least two Process calls must have happened: one error + one success.
	require.Eventually(t, func() bool { return processor.seen.Load() >= 2 },
		testhelpers.DefaultTestTimeout, 100*time.Millisecond,
		"consumer should remain healthy after a processor error and continue consuming")
}
