package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/internal/integration/testhelpers"
	"github.com/sergeyslonimsky/core/kafka"
)

// recordingProcessor collects processed messages and optionally returns an
// error for every Process call.
type recordingProcessor struct {
	mu       sync.Mutex
	messages []kafka.Message
	fail     bool
}

func (p *recordingProcessor) Process(_ context.Context, msg kafka.Message) error {
	if p.fail {
		return errors.New("processor error simulation")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.messages = append(p.messages, msg)

	return nil
}

func (p *recordingProcessor) Snapshot() []kafka.Message {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]kafka.Message, len(p.messages))
	copy(out, p.messages)

	return out
}

func (p *recordingProcessor) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.messages)
}

// runConsumerAsync starts consumer.Run in a goroutine and returns the
// cancel func and a channel that delivers Run's result.
func runConsumerAsync(
	t *testing.T,
	consumer *kafka.Consumer,
	timeout time.Duration,
) (context.CancelFunc, chan error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	errCh := make(chan error, 1)

	go func() { errCh <- consumer.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		<-errCh
		_ = consumer.Shutdown(context.Background())
	})

	return cancel, errCh
}

// produceMessages is a small helper that publishes a batch of
// key-i/message-i pairs through a fresh producer.
func produceMessages(t *testing.T, brokerURL, topic string, count int) {
	t.Helper()

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers: []string{brokerURL},
		Topic:   topic,
	})
	require.NoError(t, err)

	defer func() { _ = producer.Shutdown(context.Background()) }()

	for i := range count {
		require.NoError(t,
			producer.Produce(topic, fmt.Sprintf("key-%d", i), fmt.Appendf(nil, "message-%d", i)),
		)
	}
}

// TestConsumer_NewConsumer_Success covers the happy construction path.
func TestConsumer_NewConsumer_Success(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-creation"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{brokerURL},
		Group:   "test-group",
		Offset:  "newest",
		Topics:  []string{topic},
	}, &recordingProcessor{})
	require.NoError(t, err)
	require.NotNil(t, consumer)
	t.Cleanup(func() { _ = consumer.Shutdown(context.Background()) })
}

// TestConsumer_ConsumeMessages verifies a consumer with Offset=oldest
// reads pre-produced messages in order.
func TestConsumer_ConsumeMessages(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-consume"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	const messageCount = 5

	produceMessages(t, brokerURL, topic, messageCount)

	processor := &recordingProcessor{}

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{brokerURL},
		Group:   "test-consume-group",
		Offset:  "oldest",
		Topics:  []string{topic},
	}, processor)
	require.NoError(t, err)

	runConsumerAsync(t, consumer, testhelpers.DefaultTestTimeout)

	require.Eventually(t, func() bool {
		return processor.Count() == messageCount
	}, testhelpers.DefaultTestTimeout, 100*time.Millisecond, "should consume all messages")

	messages := processor.Snapshot()
	require.Len(t, messages, messageCount)

	for i, msg := range messages {
		assert.Equal(t, kafka.Topic(topic), msg.Topic)
		assert.Equal(t, fmt.Sprintf("key-%d", i), string(msg.Key))
		assert.Equal(t, fmt.Sprintf("message-%d", i), string(msg.Message))
	}
}

// TestConsumer_MultipleTopics verifies that a single consumer can consume
// from multiple topics simultaneously.
func TestConsumer_MultipleTopics(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic1 := "test-consumer-topic-1"
	topic2 := "test-consumer-topic-2"

	testhelpers.CreateTestTopic(t, brokerURL, topic1, 1)
	testhelpers.CreateTestTopic(t, brokerURL, topic2, 1)

	const perTopic = 3

	produceMessages(t, brokerURL, topic1, perTopic)
	produceMessages(t, brokerURL, topic2, perTopic)

	processor := &recordingProcessor{}

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{brokerURL},
		Group:   "test-multi-topic-group",
		Offset:  "oldest",
		Topics:  []string{topic1, topic2},
	}, processor)
	require.NoError(t, err)

	runConsumerAsync(t, consumer, testhelpers.DefaultTestTimeout)

	totalMessages := perTopic * 2

	require.Eventually(t, func() bool {
		return processor.Count() == totalMessages
	}, testhelpers.DefaultTestTimeout, 100*time.Millisecond, "should consume all messages from both topics")

	var topic1Count, topic2Count int

	for _, msg := range processor.Snapshot() {
		switch msg.Topic {
		case kafka.Topic(topic1):
			topic1Count++
		case kafka.Topic(topic2):
			topic2Count++
		}
	}

	assert.Equal(t, perTopic, topic1Count)
	assert.Equal(t, perTopic, topic2Count)
}

// TestConsumer_ProcessorError_DefaultSkipPolicy verifies the default
// no-handler behavior: a Process error is logged and the record is
// committed (offset advances). After a failing batch the consumer keeps
// running until ctx expires.
func TestConsumer_ProcessorError_DefaultSkipPolicy(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-processor-error-skip"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	produceMessages(t, brokerURL, topic, 3)

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{brokerURL},
		Group:   "test-error-skip-group",
		Offset:  "oldest",
		Topics:  []string{topic},
	}, &recordingProcessor{fail: true})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	// The consumer must not crash on Process errors. With no ErrorHandler,
	// records are logged + skipped, and Run exits cleanly on ctx cancel.
	err = consumer.Run(ctx)
	assert.NoError(t, err, "Run should return nil on ctx cancel even when every Process call fails")

	_ = consumer.Shutdown(context.Background())
}

// TestConsumer_ErrorHandler_Stop verifies that a handler returning
// ErrorActionStop terminates the consumer with a wrapped error.
func TestConsumer_ErrorHandler_Stop(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-handler-stop"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	produceMessages(t, brokerURL, topic, 1)

	var invocations atomic.Int32

	processor := &recordingProcessor{fail: true}
	consumer, err := kafka.NewConsumer(
		kafka.ConsumerConfig{
			Brokers: []string{brokerURL},
			Group:   "test-handler-stop-group",
			Offset:  "oldest",
			Topics:  []string{topic},
		},
		processor,
		kafka.WithErrorHandler(func(_ context.Context, _ kafka.Message, _ error) kafka.ErrorAction {
			invocations.Add(1)

			return kafka.ErrorActionStop
		}),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)
	defer cancel()

	err = consumer.Run(ctx)
	require.Error(t, err, "ErrorActionStop should propagate an error out of Run")
	assert.Contains(t, err.Error(), "kafka consumer stopped on processor error")
	assert.Equal(
		t,
		int32(1),
		invocations.Load(),
		"handler should be invoked exactly once before stop",
	)

	_ = consumer.Shutdown(context.Background())
}

// TestConsumer_ConsumerGroup_Rebalance verifies that two consumers sharing
// a group together consume every message across all partitions.
func TestConsumer_ConsumerGroup_Rebalance(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-group-rebalance"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 3)

	const messageCount = 10

	produceMessages(t, brokerURL, topic, messageCount)

	processor1 := &recordingProcessor{}
	processor2 := &recordingProcessor{}

	group := "test-consumer-group-shared"

	consumer1, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{brokerURL},
		Group:   group,
		Offset:  "oldest",
		Topics:  []string{topic},
	}, processor1)
	require.NoError(t, err)

	consumer2, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{brokerURL},
		Group:   group,
		Offset:  "oldest",
		Topics:  []string{topic},
	}, processor2)
	require.NoError(t, err)

	runConsumerAsync(t, consumer1, testhelpers.DefaultTestTimeout)
	runConsumerAsync(t, consumer2, testhelpers.DefaultTestTimeout)

	require.Eventually(t, func() bool {
		return processor1.Count()+processor2.Count() == messageCount
	}, testhelpers.DefaultTestTimeout, 100*time.Millisecond,
		"both consumers together should consume every message exactly once")
}

// TestConsumer_InvalidConfig checks every sentinel error path in
// NewConsumer.
func TestConsumer_InvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cfg    kafka.ConsumerConfig
		target error
	}{
		{
			name: "empty brokers",
			cfg: kafka.ConsumerConfig{
				Group:  "test-group",
				Topics: []string{"test-topic"},
			},
			target: kafka.ErrEmptyBrokersList,
		},
		{
			name: "empty group",
			cfg: kafka.ConsumerConfig{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
			},
			target: kafka.ErrNilConsumerGroup,
		},
		{
			name: "empty topics",
			cfg: kafka.ConsumerConfig{
				Brokers: []string{"localhost:9092"},
				Group:   "test-group",
			},
			target: kafka.ErrEmptyTopic,
		},
		{
			name: "invalid offset",
			cfg: kafka.ConsumerConfig{
				Brokers: []string{"localhost:9092"},
				Group:   "test-group",
				Offset:  "invalid",
				Topics:  []string{"test-topic"},
			},
			target: kafka.ErrInvalidOffset,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := kafka.NewConsumer(tt.cfg, &recordingProcessor{})
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.target)
		})
	}
}

// TestConsumer_NilProcessor verifies the nil-processor guard.
func TestConsumer_NilProcessor(t *testing.T) {
	t.Parallel()

	_, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{"localhost:9092"},
		Group:   "test-group",
		Topics:  []string{"test-topic"},
	}, nil)
	require.ErrorIs(t, err, kafka.ErrNilProcessor)
}

// TestConsumer_ContextCancellation verifies graceful termination on ctx
// cancellation. Run returns nil (context.Canceled is treated as normal
// exit).
func TestConsumer_ContextCancellation(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-cancel"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{brokerURL},
		Group:   "test-cancel-group",
		Offset:  "newest",
		Topics:  []string{topic},
	}, &recordingProcessor{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Shutdown(context.Background()) })

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- consumer.Run(ctx) }()

	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err, "Run should return nil on ctx cancellation")
	case <-time.After(testhelpers.ShortTestTimeout):
		t.Fatal("consumer did not stop within timeout after cancel")
	}
}

// countingProcessor atomically counts processed messages.
type countingProcessor struct {
	count atomic.Int64
}

func (p *countingProcessor) Process(_ context.Context, _ kafka.Message) error {
	p.count.Add(1)

	return nil
}

// TestConsumer_HighThroughput pushes 100 messages across 3 partitions and
// verifies they are all consumed.
func TestConsumer_HighThroughput(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-throughput"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 3)

	const messageCount = 100

	produceMessages(t, brokerURL, topic, messageCount)

	processor := &countingProcessor{}

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{brokerURL},
		Group:   "test-throughput-group",
		Offset:  "oldest",
		Topics:  []string{topic},
	}, processor)
	require.NoError(t, err)

	runConsumerAsync(t, consumer, testhelpers.DefaultTestTimeout)

	require.Eventually(t, func() bool {
		return processor.count.Load() == int64(messageCount)
	}, testhelpers.DefaultTestTimeout, 100*time.Millisecond, "should consume all messages")
}
