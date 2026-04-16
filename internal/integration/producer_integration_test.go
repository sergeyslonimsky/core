package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/internal/integration/testhelpers"
	"github.com/sergeyslonimsky/core/kafka"
)

// newTestProducer is a shortcut: creates a kafka.Producer pointing at the
// given broker and registers Shutdown with t.Cleanup.
func newTestProducer(t *testing.T, brokerURL, topic string) *kafka.Producer {
	t.Helper()

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers: []string{brokerURL},
		Topic:   topic,
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = producer.Shutdown(context.Background()) })

	return producer
}

// TestProducer_Produce_Success sends a single message through kafka.Producer
// and verifies it lands on the topic.
func TestProducer_Produce_Success(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-produce-single"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	producer := newTestProducer(t, brokerURL, topic)

	require.NoError(t, producer.Produce(topic, "test-key", []byte("test message")))

	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, 1, testhelpers.ShortTestTimeout)
	require.Len(t, records, 1, "should consume 1 message")
	assert.Equal(t, "test-key", string(records[0].Key))
	assert.Equal(t, "test message", string(records[0].Value))
}

// TestProducer_ProduceMultiple sends a batch of messages in sequence and
// verifies every record lands on the topic.
func TestProducer_ProduceMultiple(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-produce-multiple"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	producer := newTestProducer(t, brokerURL, topic)

	const messageCount = 10
	for i := range messageCount {
		require.NoError(t,
			producer.Produce(topic, fmt.Sprintf("key-%d", i), fmt.Appendf(nil, "message-%d", i)),
			"produce should succeed for message %d", i,
		)
	}

	records := testhelpers.ConsumeTestMessages(
		t,
		brokerURL,
		topic,
		messageCount,
		testhelpers.DefaultTestTimeout,
	)
	require.Len(t, records, messageCount)

	for i, r := range records {
		assert.Equal(t, fmt.Sprintf("key-%d", i), string(r.Key), "key mismatch at index %d", i)
		assert.Equal(
			t,
			fmt.Sprintf("message-%d", i),
			string(r.Value),
			"value mismatch at index %d",
			i,
		)
	}
}

// TestProducer_SameKeySamePartition verifies that kgo's default hash
// partitioner places messages with equal keys on the same partition.
func TestProducer_SameKeySamePartition(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-produce-key"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 3) // 3 partitions

	producer := newTestProducer(t, brokerURL, topic)

	const (
		sameKey      = "same-key"
		messageCount = 5
	)

	for i := range messageCount {
		require.NoError(t, producer.Produce(topic, sameKey, fmt.Appendf(nil, "message-%d", i)))
	}

	records := testhelpers.ConsumeTestMessages(
		t,
		brokerURL,
		topic,
		messageCount,
		testhelpers.DefaultTestTimeout,
	)
	require.Len(t, records, messageCount)

	firstPartition := records[0].Partition
	for _, r := range records {
		assert.Equal(
			t,
			firstPartition,
			r.Partition,
			"messages with same key should share a partition",
		)
		assert.Equal(t, sameKey, string(r.Key))
	}
}

// TestProducer_ConcurrentProduce covers that Produce is safe to call from
// many goroutines concurrently (the underlying kgo client is thread-safe).
func TestProducer_ConcurrentProduce(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-concurrent-produce"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 3)

	producer := newTestProducer(t, brokerURL, topic)

	const (
		goroutines           = 10
		messagesPerGoroutine = 10
	)

	totalMessages := goroutines * messagesPerGoroutine

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for g := range goroutines {
		go func(id int) {
			defer wg.Done()

			for i := range messagesPerGoroutine {
				key := fmt.Sprintf("goroutine-%d", id)
				value := fmt.Appendf(nil, "message-%d-%d", id, i)
				assert.NoError(t, producer.Produce(topic, key, value))
			}
		}(g)
	}

	wg.Wait()

	records := testhelpers.ConsumeTestMessages(
		t,
		brokerURL,
		topic,
		totalMessages,
		testhelpers.DefaultTestTimeout,
	)
	assert.Len(t, records, totalMessages, "should consume all messages from concurrent producers")
}

// TestMessage is a test struct for JSONProducer tests.
type TestMessage struct {
	ID   int    `json:"id"`
	Data string `json:"data"`
}

// TestJSONProducer_Produce_Success verifies JSONProducer encodes payloads
// and publishes them through the wrapped Publisher.
func TestJSONProducer_Produce_Success(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-json-produce"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	producer := newTestProducer(t, brokerURL, topic)
	jsonProducer := kafka.NewJSONProducer[TestMessage](topic, "default-key", producer)

	want := TestMessage{ID: 1, Data: "test data"}
	require.NoError(t, jsonProducer.Produce(t.Context(), want), "JSON produce should succeed")

	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, 1, testhelpers.ShortTestTimeout)
	require.Len(t, records, 1)

	var got TestMessage

	require.NoError(t, json.Unmarshal(records[0].Value, &got), "should decode JSON")
	assert.Equal(t, want, got)
}

// TestJSONProducer_ProduceWithKey covers the custom-key overload.
func TestJSONProducer_ProduceWithKey(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-json-produce-key"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	producer := newTestProducer(t, brokerURL, topic)
	jsonProducer := kafka.NewJSONProducer[TestMessage](topic, "default", producer)

	const customKey = "custom-key-123"

	require.NoError(
		t,
		jsonProducer.ProduceWithKey(t.Context(), customKey, TestMessage{ID: 2, Data: "custom"}),
	)

	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, 1, testhelpers.ShortTestTimeout)
	require.Len(t, records, 1)
	assert.Equal(t, customKey, string(records[0].Key))
}

// TestProducer_ShutdownIdempotent verifies Shutdown can be called twice —
// the second call is a no-op via the internal sync.Once.
func TestProducer_ShutdownIdempotent(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	producer, err := kafka.NewProducer(kafka.ProducerConfig{Brokers: []string{brokerURL}})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), testhelpers.ShortTestTimeout)
	defer cancel()

	assert.NoError(t, producer.Shutdown(ctx))
	assert.NoError(t, producer.Shutdown(ctx))
}

// TestProducer_Timeout demonstrates that a producer configured with a tiny
// produce timeout fails fast rather than blocking indefinitely when the
// broker is unreachable.
func TestProducer_Timeout(t *testing.T) {
	t.Parallel()

	producer, err := kafka.NewProducer(
		kafka.ProducerConfig{Brokers: []string{"127.0.0.1:1"}},
		kafka.WithProduceTimeout(500*time.Millisecond),
	)
	require.NoError(t, err, "producer constructor is lazy")
	t.Cleanup(func() { _ = producer.Shutdown(context.Background()) })

	err = producer.Produce("does-not-matter", "k", []byte("v"))
	require.Error(t, err, "produce to unreachable broker must fail within the configured timeout")
}
