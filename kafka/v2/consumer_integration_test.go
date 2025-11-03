//go:build integration

package v2_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sergeyslonimsky/core/internal/testhelpers"
	v2 "github.com/sergeyslonimsky/core/kafka/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testProcessor is a simple test processor that collects messages.
type testProcessor struct {
	messages []v2.Message
	mu       sync.Mutex
	errors   bool // if true, processor returns errors
}

func (p *testProcessor) Process(_ context.Context, msg v2.Message) error {
	if p.errors {
		return errors.New("processor error simulation")
	}

	p.mu.Lock()
	p.messages = append(p.messages, msg)
	p.mu.Unlock()

	return nil
}

func (p *testProcessor) GetMessages() []v2.Message {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]v2.Message, len(p.messages))
	copy(result, p.messages)

	return result
}

func (p *testProcessor) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.messages)
}

// TestConsumer_NewConsumer_Success tests creating a consumer successfully.
func TestConsumer_NewConsumer_Success(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-creation"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	processor := &testProcessor{}
	cfg := v2.ConsumerConfig{
		Brokers: brokerURL,
		Group:   "test-group",
		Offset:  "newest",
		Topics:  []string{topic},
	}

	consumer, err := v2.NewConsumer(cfg, processor)
	require.NoError(t, err, "NewConsumer should succeed")
	require.NotNil(t, consumer, "consumer should not be nil")
}

// TestConsumer_ConsumeMessages tests consuming messages from a topic.
func TestConsumer_ConsumeMessages(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-consume"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	// Produce messages first
	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	messageCount := 5
	for i := range messageCount {
		err = producer.Produce(topic, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("message-%d", i)))
		require.NoError(t, err)
	}

	// Start consumer
	processor := &testProcessor{}
	cfg := v2.ConsumerConfig{
		Brokers: brokerURL,
		Group:   "test-consume-group",
		Offset:  "oldest", // Read from beginning
		Topics:  []string{topic},
	}

	consumer, err := v2.NewConsumer(cfg, processor)
	require.NoError(t, err)

	// Run consumer in background
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	consumerDone := make(chan error, 1)

	go func() {
		consumerDone <- consumer.Run(ctx)
	}()

	// Wait for messages to be processed
	require.Eventually(t, func() bool {
		return processor.Count() == messageCount
	}, 8*time.Second, 100*time.Millisecond, "should consume all messages")

	// Stop consumer
	cancel()
	<-consumerDone

	// Verify messages
	messages := processor.GetMessages()
	require.Len(t, messages, messageCount)

	for i, msg := range messages {
		assert.Equal(t, v2.Topic(topic), msg.Topic)
		assert.Equal(t, fmt.Sprintf("key-%d", i), string(msg.Key))
		assert.Equal(t, fmt.Sprintf("message-%d", i), string(msg.Message))
	}
}

// TestConsumer_OffsetOldest tests that consumer with offset=oldest reads all messages.
func TestConsumer_OffsetOldest(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-offset-oldest"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	// Produce messages BEFORE starting consumer
	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	messageCount := 5
	for i := range messageCount {
		err = producer.Produce(topic, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("message-%d", i)))
		require.NoError(t, err)
	}

	// Start consumer with offset=oldest
	processor := &testProcessor{}
	cfg := v2.ConsumerConfig{
		Brokers: brokerURL,
		Group:   "test-oldest-group",
		Offset:  "oldest",
		Topics:  []string{topic},
	}

	consumer, err := v2.NewConsumer(cfg, processor)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	consumerDone := make(chan error, 1)

	go func() {
		consumerDone <- consumer.Run(ctx)
	}()

	// Wait for all messages
	require.Eventually(t, func() bool {
		return processor.Count() == messageCount
	}, 8*time.Second, 100*time.Millisecond, "should consume all messages from beginning")

	cancel()
	<-consumerDone

	// Verify all messages were consumed
	messages := processor.GetMessages()
	require.Len(t, messages, messageCount)
}

// TestConsumer_MultipleTopics tests consuming from multiple topics.
func TestConsumer_MultipleTopics(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic1 := "test-consumer-topic-1"
	topic2 := "test-consumer-topic-2"

	testhelpers.CreateTestTopic(t, brokerURL, topic1, 1)
	testhelpers.CreateTestTopic(t, brokerURL, topic2, 1)

	// Produce messages to both topics
	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	messagesPerTopic := 3
	for i := range messagesPerTopic {
		err = producer.Produce(topic1, "key", []byte(fmt.Sprintf("topic1-message-%d", i)))
		require.NoError(t, err)
		err = producer.Produce(topic2, "key", []byte(fmt.Sprintf("topic2-message-%d", i)))
		require.NoError(t, err)
	}

	// Start consumer for both topics
	processor := &testProcessor{}
	cfg := v2.ConsumerConfig{
		Brokers: brokerURL,
		Group:   "test-multi-topic-group",
		Offset:  "oldest",
		Topics:  []string{topic1, topic2},
	}

	consumer, err := v2.NewConsumer(cfg, processor)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	consumerDone := make(chan error, 1)

	go func() {
		consumerDone <- consumer.Run(ctx)
	}()

	// Wait for all messages from both topics
	totalMessages := messagesPerTopic * 2

	require.Eventually(t, func() bool {
		return processor.Count() == totalMessages
	}, 8*time.Second, 100*time.Millisecond, "should consume all messages from both topics")

	cancel()
	<-consumerDone

	messages := processor.GetMessages()
	require.Len(t, messages, totalMessages)

	// Count messages per topic
	topic1Count := 0
	topic2Count := 0

	for _, msg := range messages {
		if msg.Topic == v2.Topic(topic1) {
			topic1Count++
		} else if msg.Topic == v2.Topic(topic2) {
			topic2Count++
		}
	}

	assert.Equal(t, messagesPerTopic, topic1Count, "should consume all messages from topic1")
	assert.Equal(t, messagesPerTopic, topic2Count, "should consume all messages from topic2")
}

// TestConsumer_ProcessorError tests that processor errors don't crash the consumer.
func TestConsumer_ProcessorError(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-processor-error"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	// Produce messages
	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	err = producer.Produce(topic, "key", []byte("message"))
	require.NoError(t, err)

	// Start consumer with processor that returns errors
	processor := &testProcessor{errors: true}
	cfg := v2.ConsumerConfig{
		Brokers: brokerURL,
		Group:   "test-error-group",
		Offset:  "oldest",
		Topics:  []string{topic},
	}

	consumer, err := v2.NewConsumer(cfg, processor)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Consumer should not crash even with processor errors
	err = consumer.Run(ctx)
	// Should exit with context.DeadlineExceeded, not processor error
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestConsumer_ConsumerGroup tests consumer group behavior with rebalancing.
func TestConsumer_ConsumerGroup(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-group-rebalance"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 3) // 3 partitions

	// Produce messages
	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	messageCount := 10
	for i := range messageCount {
		err = producer.Produce(topic, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("message-%d", i)))
		require.NoError(t, err)
	}

	// Start two consumers in the same group
	processor1 := &testProcessor{}
	processor2 := &testProcessor{}

	cfg1 := v2.ConsumerConfig{
		Brokers: brokerURL,
		Group:   "test-consumer-group-shared",
		Offset:  "oldest",
		Topics:  []string{topic},
	}

	cfg2 := v2.ConsumerConfig{
		Brokers: brokerURL,
		Group:   "test-consumer-group-shared", // Same group
		Offset:  "oldest",
		Topics:  []string{topic},
	}

	consumer1, err := v2.NewConsumer(cfg1, processor1)
	require.NoError(t, err)

	consumer2, err := v2.NewConsumer(cfg2, processor2)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	// Run both consumers
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		_ = consumer1.Run(ctx)
	}()

	go func() {
		defer wg.Done()

		_ = consumer2.Run(ctx)
	}()

	// Wait for all messages to be consumed by either consumer
	require.Eventually(t, func() bool {
		total := processor1.Count() + processor2.Count()

		return total == messageCount
	}, 12*time.Second, 100*time.Millisecond, "both consumers should consume all messages together")

	cancel()
	wg.Wait()

	// Verify total message count
	totalConsumed := processor1.Count() + processor2.Count()
	assert.Equal(t, messageCount, totalConsumed, "total messages consumed should match produced")
}

// TestConsumer_InvalidConfig tests error handling for invalid configurations.
func TestConsumer_InvalidConfig(t *testing.T) {
	t.Parallel()

	processor := &testProcessor{}

	tests := []struct {
		name        string
		cfg         v2.ConsumerConfig
		expectedErr string
	}{
		{
			name: "empty brokers",
			cfg: v2.ConsumerConfig{
				Brokers: "",
				Group:   "test-group",
				Topics:  []string{"test-topic"},
			},
			expectedErr: "brokers string cannot be empty",
		},
		{
			name: "empty group",
			cfg: v2.ConsumerConfig{
				Brokers: "localhost:9092",
				Group:   "",
				Topics:  []string{"test-topic"},
			},
			expectedErr: "consumer group cannot be empty",
		},
		{
			name: "empty topics",
			cfg: v2.ConsumerConfig{
				Brokers: "localhost:9092",
				Group:   "test-group",
				Topics:  []string{},
			},
			expectedErr: "topics list cannot be empty",
		},
		{
			name: "invalid offset",
			cfg: v2.ConsumerConfig{
				Brokers: "localhost:9092",
				Group:   "test-group",
				Offset:  "invalid",
				Topics:  []string{"test-topic"},
			},
			expectedErr: "invalid offset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := v2.NewConsumer(tt.cfg, processor)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

// TestConsumer_NilProcessor tests that nil processor returns error.
func TestConsumer_NilProcessor(t *testing.T) {
	t.Parallel()

	cfg := v2.ConsumerConfig{
		Brokers: "localhost:9092",
		Group:   "test-group",
		Topics:  []string{"test-topic"},
	}

	_, err := v2.NewConsumer(cfg, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "processor cannot be nil")
}

// TestConsumer_ContextCancellation tests graceful shutdown on context cancellation.
func TestConsumer_ContextCancellation(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-cancel"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	processor := &testProcessor{}
	cfg := v2.ConsumerConfig{
		Brokers: brokerURL,
		Group:   "test-cancel-group",
		Offset:  "newest",
		Topics:  []string{topic},
	}

	consumer, err := v2.NewConsumer(cfg, processor)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	consumerDone := make(chan error, 1)

	go func() {
		consumerDone <- consumer.Run(ctx)
	}()

	// Wait a bit and cancel
	time.Sleep(500 * time.Millisecond)
	cancel()

	// Consumer should stop gracefully
	select {
	case err := <-consumerDone:
		require.ErrorIs(t, err, context.Canceled, "should return context.Canceled")
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop within timeout")
	}
}

// countingProcessor counts processed messages atomically.
type countingProcessor struct {
	count int64
}

func (p *countingProcessor) Process(_ context.Context, _ v2.Message) error {
	atomic.AddInt64(&p.count, 1)

	return nil
}

func (p *countingProcessor) Count() int64 {
	return atomic.LoadInt64(&p.count)
}

// TestConsumer_HighThroughput tests consumer with high message volume.
func TestConsumer_HighThroughput(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-consumer-throughput"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 3) // Multiple partitions for parallelism

	// Produce large number of messages
	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	messageCount := 100
	for i := range messageCount {
		err = producer.Produce(topic, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("message-%d", i)))
		require.NoError(t, err)
	}

	// Start consumer
	processor := &countingProcessor{}
	cfg := v2.ConsumerConfig{
		Brokers: brokerURL,
		Group:   "test-throughput-group",
		Offset:  "oldest",
		Topics:  []string{topic},
	}

	consumer, err := v2.NewConsumer(cfg, processor)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	consumerDone := make(chan error, 1)

	go func() {
		consumerDone <- consumer.Run(ctx)
	}()

	// Wait for all messages
	require.Eventually(t, func() bool {
		return processor.Count() == int64(messageCount)
	}, 15*time.Second, 100*time.Millisecond, "should consume all messages")

	cancel()
	<-consumerDone

	assert.Equal(t, int64(messageCount), processor.Count(), "should process all messages")
}
