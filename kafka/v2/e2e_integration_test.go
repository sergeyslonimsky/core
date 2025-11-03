//go:build integration

package v2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sergeyslonimsky/core/internal/testhelpers"
	v2 "github.com/sergeyslonimsky/core/kafka/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_ProduceAndConsume tests the full produce → consume cycle.
func TestE2E_ProduceAndConsume(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-e2e-basic"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	// Produce messages
	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	messageCount := 10
	for i := range messageCount {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("message-%d", i))
		err = producer.Produce(topic, key, value)
		require.NoError(t, err, "produce failed for message %d", i)
	}

	// Consume and verify
	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, messageCount, 10*time.Second)
	require.Len(t, records, messageCount, "should consume all messages")

	// Verify order and content
	for i, r := range records {
		expectedKey := fmt.Sprintf("key-%d", i)
		expectedValue := fmt.Sprintf("message-%d", i)
		assert.Equal(t, expectedKey, string(r.Key), "key mismatch at index %d", i)
		assert.Equal(t, expectedValue, string(r.Value), "value mismatch at index %d", i)
	}
}

// TestE2EMessage is a test struct for JSON e2e tests.
type TestE2EMessage struct {
	ID        int       `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Payload   string    `json:"payload"`
}

// TestE2E_JSONProduceAndConsume tests JSON serialization end-to-end.
func TestE2E_JSONProduceAndConsume(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-e2e-json"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	// Setup JSON producer
	syncProducer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer syncProducer.Close()

	jsonProducer := v2.NewJSONProducer[TestE2EMessage](topic, "json-key", syncProducer)

	// Produce JSON messages
	ctx := context.Background()
	now := time.Now()
	originalMessages := []TestE2EMessage{
		{ID: 1, Timestamp: now, Payload: "first message"},
		{ID: 2, Timestamp: now.Add(time.Second), Payload: "second message"},
		{ID: 3, Timestamp: now.Add(2 * time.Second), Payload: "third message"},
	}

	for _, msg := range originalMessages {
		err = jsonProducer.Produce(ctx, msg)
		require.NoError(t, err, "JSON produce failed for message %d", msg.ID)
	}

	// Consume and decode
	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, len(originalMessages), 10*time.Second)
	require.Len(t, records, len(originalMessages))

	// Verify JSON decoding
	for i, r := range records {
		var decoded TestE2EMessage

		err = json.Unmarshal(r.Value, &decoded)
		require.NoError(t, err, "JSON decode failed for record %d", i)

		assert.Equal(t, originalMessages[i].ID, decoded.ID)
		assert.Equal(t, originalMessages[i].Payload, decoded.Payload)
		// Note: time comparison might need tolerance due to JSON serialization
		assert.WithinDuration(t, originalMessages[i].Timestamp, decoded.Timestamp, time.Second)
	}
}

// TestE2E_MultiplePartitions tests partitioning behavior.
func TestE2E_MultiplePartitions(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-e2e-partitions"
	partitionCount := int32(3)
	testhelpers.CreateTestTopic(t, brokerURL, topic, partitionCount)

	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	// Produce messages with different keys
	keyGroups := []string{"group-a", "group-b", "group-c"}
	messagesPerGroup := 5
	totalMessages := len(keyGroups) * messagesPerGroup

	for _, key := range keyGroups {
		for i := range messagesPerGroup {
			value := []byte(fmt.Sprintf("%s-message-%d", key, i))
			err = producer.Produce(topic, key, value)
			require.NoError(t, err)
		}
	}

	// Consume all messages
	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, totalMessages, 15*time.Second)
	require.Len(t, records, totalMessages)

	// Group messages by key and verify they're in the same partition
	partitionsByKey := make(map[string]int32)
	for _, r := range records {
		key := string(r.Key)
		if existingPartition, exists := partitionsByKey[key]; exists {
			assert.Equal(t, existingPartition, r.Partition,
				"all messages with key %s should be in same partition", key)
		} else {
			partitionsByKey[key] = r.Partition
		}
	}

	// Verify we used multiple partitions
	uniquePartitions := make(map[int32]bool)
	for _, partition := range partitionsByKey {
		uniquePartitions[partition] = true
	}

	assert.GreaterOrEqual(t, len(uniquePartitions), 2,
		"should use at least 2 partitions with different keys")
}

// TestE2E_OrderGuarantee tests message ordering within a single partition.
func TestE2E_OrderGuarantee(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-e2e-order"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 3)

	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	// Use same key to ensure all messages go to same partition
	sameKey := "ordered-key"
	messageCount := 20

	for i := range messageCount {
		value := []byte(fmt.Sprintf("order-%d", i))
		err = producer.Produce(topic, sameKey, value)
		require.NoError(t, err)
	}

	// Consume and verify order
	records := testhelpers.ConsumeTestMessages(t, brokerURL, topic, messageCount, 10*time.Second)
	require.Len(t, records, messageCount)

	// All should be in same partition
	firstPartition := records[0].Partition
	for _, r := range records {
		assert.Equal(t, firstPartition, r.Partition)
	}

	// Verify order by offset
	for i := 1; i < len(records); i++ {
		assert.Greater(t, records[i].Offset, records[i-1].Offset,
			"offsets should be in ascending order")
	}

	// Verify message content order
	for i, r := range records {
		expectedValue := fmt.Sprintf("order-%d", i)
		assert.Equal(t, expectedValue, string(r.Value),
			"message order mismatch at index %d", i)
	}
}
