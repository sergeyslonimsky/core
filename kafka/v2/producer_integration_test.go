//go:build integration

package v2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sergeyslonimsky/core/internal/testhelpers"
	v2 "github.com/sergeyslonimsky/core/kafka/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSyncProducer_Produce_Success tests sending a single message successfully.
func TestSyncProducer_Produce_Success(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-produce-single"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	// Produce message
	err = producer.Produce(topic, "test-key", []byte("test message"))
	require.NoError(t, err, "produce should succeed")

	// Verify message was actually sent
	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, 1, 5*time.Second)
	require.Len(t, records, 1, "should consume 1 message")
	assert.Equal(t, "test-key", string(records[0].Key))
	assert.Equal(t, "test message", string(records[0].Value))
}

// TestSyncProducer_ProduceMultiple tests sending multiple messages in sequence.
func TestSyncProducer_ProduceMultiple(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-produce-multiple"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	// Produce 10 messages
	messageCount := 10
	for i := range messageCount {
		err = producer.Produce(topic, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("message-%d", i)))
		require.NoError(t, err, "produce should succeed for message %d", i)
	}

	// Verify all messages were sent
	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, messageCount, 10*time.Second)
	require.Len(t, records, messageCount, "should consume all messages")

	// Verify content of each message
	for i, r := range records {
		expectedKey := fmt.Sprintf("key-%d", i)
		expectedValue := fmt.Sprintf("message-%d", i)
		assert.Equal(t, expectedKey, string(r.Key), "key mismatch at index %d", i)
		assert.Equal(t, expectedValue, string(r.Value), "value mismatch at index %d", i)
	}
}

// TestSyncProducer_ProduceWithKey tests that messages with the same key go to the same partition.
func TestSyncProducer_ProduceWithKey(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-produce-key"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 3) // 3 partitions

	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	sameKey := "same-key"
	messageCount := 5

	// Produce multiple messages with the same key
	for i := range messageCount {
		err = producer.Produce(topic, sameKey, []byte(fmt.Sprintf("message-%d", i)))
		require.NoError(t, err)
	}

	// Verify messages
	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, messageCount, 10*time.Second)
	require.Len(t, records, messageCount)

	// All messages with same key should be in same partition
	firstPartition := records[0].Partition
	for _, r := range records {
		assert.Equal(t, firstPartition, r.Partition, "messages with same key should be in same partition")
		assert.Equal(t, sameKey, string(r.Key))
	}
}

// TestSyncProducer_ConcurrentProduce tests concurrent message production.
func TestSyncProducer_ConcurrentProduce(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-concurrent-produce"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 3)

	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	goroutines := 10
	messagesPerGoroutine := 10
	totalMessages := goroutines * messagesPerGoroutine

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Produce from multiple goroutines
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()

			for i := range messagesPerGoroutine {
				key := fmt.Sprintf("goroutine-%d", id)
				value := []byte(fmt.Sprintf("message-%d-%d", id, i))
				err := producer.Produce(topic, key, value)
				assert.NoError(t, err)
			}
		}(g)
	}

	wg.Wait()

	// Verify all messages were sent
	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, totalMessages, 15*time.Second)
	assert.Len(t, records, totalMessages, "should consume all messages from concurrent producers")
}

// TestMessage is a test struct for JSON producer tests.
type TestMessage struct {
	ID   int    `json:"id"`
	Data string `json:"data"`
}

// TestJSONProducer_Produce_Success tests JSON serialization and production.
func TestJSONProducer_Produce_Success(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-json-produce"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	syncProducer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer syncProducer.Close()

	jsonProducer := v2.NewJSONProducer[TestMessage](topic, "default-key", syncProducer)

	// Produce JSON message
	testMsg := TestMessage{ID: 1, Data: "test data"}
	ctx := context.Background()
	err = jsonProducer.Produce(ctx, testMsg)
	require.NoError(t, err, "JSON produce should succeed")

	// Consume and verify
	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, 1, 5*time.Second)
	require.Len(t, records, 1)

	var decoded TestMessage

	err = json.Unmarshal(records[0].Value, &decoded)
	require.NoError(t, err, "should decode JSON")
	assert.Equal(t, testMsg.ID, decoded.ID)
	assert.Equal(t, testMsg.Data, decoded.Data)
}

// TestJSONProducer_ProduceWithKey_Integration tests JSON producer with custom key.
func TestJSONProducer_ProduceWithKey_Integration(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-json-produce-key"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	syncProducer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer syncProducer.Close()

	jsonProducer := v2.NewJSONProducer[TestMessage](topic, "default", syncProducer)

	// Produce with custom key
	testMsg := TestMessage{ID: 2, Data: "custom key test"}
	customKey := "custom-key-123"
	ctx := context.Background()
	err = jsonProducer.ProduceWithKey(ctx, customKey, testMsg)
	require.NoError(t, err)

	// Verify key
	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, 1, 5*time.Second)
	require.Len(t, records, 1)
	assert.Equal(t, customKey, string(records[0].Key))
}

// TestSyncProducer_DynamicTopic tests production to a newly created topic.
func TestSyncProducer_DynamicTopic(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	topic := "dynamic-test-topic"
	testKey := "test-key"
	testValue := []byte("test message")

	// Create topic first (RedPanda doesn't auto-create by default)
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	// Produce to the newly created topic
	err = producer.Produce(topic, testKey, testValue)
	require.NoError(t, err, "produce should succeed with newly created topic")

	// Verify the message was actually produced
	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, 1, 5*time.Second)
	require.Len(t, records, 1, "should consume 1 message from topic")
	assert.Equal(t, testKey, string(records[0].Key))
	assert.Equal(t, testValue, records[0].Value)
}
