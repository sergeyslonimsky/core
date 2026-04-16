package integration_test

import (
	"context"
	"encoding/json"
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

// TestE2E_ProduceAndConsume covers the full kafka.Producer -> kafka.Consumer
// round trip with the new API (slice brokers, Shutdown(ctx)).
func TestE2E_ProduceAndConsume(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-e2e-basic"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers: []string{brokerURL},
		Topic:   topic,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = producer.Shutdown(context.Background()) })

	const messageCount = 10
	for i := range messageCount {
		require.NoError(t,
			producer.Produce(topic, fmt.Sprintf("key-%d", i), fmt.Appendf(nil, "message-%d", i)),
			"produce failed for message %d", i,
		)
	}

	// Consume via the core kafka.Consumer (not the raw helper) so we exercise
	// the end-to-end Runner lifecycle.
	processor := &recordingProcessor{}

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{brokerURL},
		Group:   "test-e2e-basic-group",
		Offset:  "oldest",
		Topics:  []string{topic},
	}, processor)
	require.NoError(t, err)

	runConsumerAsync(t, consumer, testhelpers.DefaultTestTimeout)

	require.Eventually(t, func() bool {
		return processor.Count() == messageCount
	}, testhelpers.DefaultTestTimeout, 100*time.Millisecond)

	for i, msg := range processor.Snapshot() {
		assert.Equal(t, fmt.Sprintf("key-%d", i), string(msg.Key), "key mismatch at %d", i)
		assert.Equal(
			t,
			fmt.Sprintf("message-%d", i),
			string(msg.Message),
			"value mismatch at %d",
			i,
		)
	}
}

// TestE2EMessage is a typed payload used by the JSON round-trip test.
type TestE2EMessage struct {
	ID        int       `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Payload   string    `json:"payload"`
}

// TestE2E_JSONProduceAndConsume runs JSONProducer and then decodes what the
// raw helper consumes — covers serialization fidelity across the boundary.
func TestE2E_JSONProduceAndConsume(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-e2e-json"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers: []string{brokerURL},
		Topic:   topic,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = producer.Shutdown(context.Background()) })

	jsonProducer := kafka.NewJSONProducer[TestE2EMessage](topic, "json-key", producer)

	now := time.Now().Truncate(time.Second)
	originalMessages := []TestE2EMessage{
		{ID: 1, Timestamp: now, Payload: "first message"},
		{ID: 2, Timestamp: now.Add(time.Second), Payload: "second message"},
		{ID: 3, Timestamp: now.Add(2 * time.Second), Payload: "third message"},
	}

	for _, msg := range originalMessages {
		require.NoError(
			t,
			jsonProducer.Produce(t.Context(), msg),
			"JSON produce failed for id=%d",
			msg.ID,
		)
	}

	records := testhelpers.ConsumeTestMessages(
		t,
		brokerURL,
		topic,
		len(originalMessages),
		testhelpers.DefaultTestTimeout,
	)
	require.Len(t, records, len(originalMessages))

	for i, r := range records {
		var decoded TestE2EMessage

		require.NoError(t, json.Unmarshal(r.Value, &decoded), "JSON decode failed for record %d", i)
		assert.Equal(t, originalMessages[i].ID, decoded.ID)
		assert.Equal(t, originalMessages[i].Payload, decoded.Payload)
		assert.WithinDuration(t, originalMessages[i].Timestamp, decoded.Timestamp, time.Second)
	}
}

// TestE2E_MultiplePartitions verifies that the default hash partitioner
// spreads distinct keys across partitions, while same-key messages land
// on a single partition.
func TestE2E_MultiplePartitions(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-e2e-partitions"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 3)

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers: []string{brokerURL},
		Topic:   topic,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = producer.Shutdown(context.Background()) })

	keyGroups := []string{"group-a", "group-b", "group-c"}

	const perGroup = 5

	totalMessages := len(keyGroups) * perGroup

	for _, key := range keyGroups {
		for i := range perGroup {
			require.NoError(
				t,
				producer.Produce(topic, key, fmt.Appendf(nil, "%s-message-%d", key, i)),
			)
		}
	}

	records := testhelpers.ConsumeTestMessages(
		t,
		brokerURL,
		topic,
		totalMessages,
		testhelpers.DefaultTestTimeout,
	)
	require.Len(t, records, totalMessages)

	partitionsByKey := make(map[string]int32)

	for _, r := range records {
		key := string(r.Key)
		if partition, seen := partitionsByKey[key]; seen {
			assert.Equal(
				t,
				partition,
				r.Partition,
				"messages with key %q must share a partition",
				key,
			)
		} else {
			partitionsByKey[key] = r.Partition
		}
	}

	uniquePartitions := make(map[int32]struct{})
	for _, p := range partitionsByKey {
		uniquePartitions[p] = struct{}{}
	}

	assert.GreaterOrEqual(
		t,
		len(uniquePartitions),
		2,
		"three distinct keys should land on at least 2 partitions",
	)
}

// TestE2E_OrderGuarantee verifies that messages with a shared key arrive in
// produce order with monotonically increasing offsets.
func TestE2E_OrderGuarantee(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-e2e-order"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 3)

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers: []string{brokerURL},
		Topic:   topic,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = producer.Shutdown(context.Background()) })

	const (
		sameKey      = "ordered-key"
		messageCount = 20
	)

	for i := range messageCount {
		require.NoError(t, producer.Produce(topic, sameKey, fmt.Appendf(nil, "order-%d", i)))
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
		assert.Equal(t, firstPartition, r.Partition)
	}

	for i := 1; i < len(records); i++ {
		assert.Greater(
			t,
			records[i].Offset,
			records[i-1].Offset,
			"offsets should be monotonically increasing",
		)
	}

	for i, r := range records {
		assert.Equal(
			t,
			fmt.Sprintf("order-%d", i),
			string(r.Value),
			"message order mismatch at index %d",
			i,
		)
	}
}

// countingProcessorAtomic is a payload-agnostic MessageProcessor that only
// counts invocations, used by the ErrorAction-retry test below.
type countingProcessorAtomic struct {
	count atomic.Int32
}

func (p *countingProcessorAtomic) Process(_ context.Context, _ kafka.Message) error {
	p.count.Add(1)

	return nil
}

// TestE2E_ErrorHandler_RetryDoesNotCommit verifies that ErrorActionRetry
// leaves records uncommitted: a restarted consumer in the same group sees
// them again on the next session. This is the contract that makes the new
// WithErrorHandler API valuable (vs silent commit on error in v1).
func TestE2E_ErrorHandler_RetryDoesNotCommit(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	topic := "test-e2e-retry"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	produceMessages(t, brokerURL, topic, 1)

	group := "test-e2e-retry-group"

	// Phase 1: consumer that always returns Retry — record must stay
	// uncommitted. Process is invoked at least once.
	var phase1Calls atomic.Int32

	phase1Processor := &recordingProcessor{fail: true}
	phase1, err := kafka.NewConsumer(
		kafka.ConsumerConfig{
			Brokers: []string{brokerURL},
			Group:   group,
			Offset:  "oldest",
			Topics:  []string{topic},
		},
		phase1Processor,
		kafka.WithErrorHandler(func(_ context.Context, _ kafka.Message, _ error) kafka.ErrorAction {
			phase1Calls.Add(1)

			return kafka.ErrorActionRetry
		}),
	)
	require.NoError(t, err)

	// Give phase 1 plenty of time: consumer-group join + rebalance can take
	// several seconds on an empty broker before PollFetches even returns.
	ctx1, cancel1 := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)
	done1 := make(chan error, 1)

	go func() { done1 <- phase1.Run(ctx1) }()

	require.Eventually(
		t,
		func() bool { return phase1Calls.Load() >= 1 },
		testhelpers.DefaultTestTimeout,
		100*time.Millisecond,
		"ErrorHandler should be invoked at least once (consumer group coordination may take several seconds)",
	)

	cancel1()
	<-done1
	require.NoError(t, phase1.Shutdown(t.Context()))

	// Phase 2: fresh consumer in the SAME group with a working processor.
	// Because Phase 1 never committed, Phase 2 should see the message.
	phase2Processor := &countingProcessorAtomic{}
	phase2, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{brokerURL},
		Group:   group,
		Offset:  "oldest",
		Topics:  []string{topic},
	}, phase2Processor)
	require.NoError(t, err)

	ctx2, cancel2 := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)

	var wg sync.WaitGroup

	wg.Go(func() {
		_ = phase2.Run(ctx2)
	})

	require.Eventually(t, func() bool { return phase2Processor.count.Load() >= 1 },
		testhelpers.DefaultTestTimeout, 100*time.Millisecond,
		"message must be redelivered to phase-2 consumer because phase-1 never committed")

	cancel2()
	wg.Wait()

	_ = phase2.Shutdown(context.Background())
}
