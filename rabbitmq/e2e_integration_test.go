//go:build integration

package rabbitmq_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/sergeyslonimsky/core/internal/testhelpers"
	"github.com/sergeyslonimsky/core/rabbitmq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// e2eTestMessage is a test message for E2E tests.
type e2eTestMessage struct {
	ID        int       `json:"id"`
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
}

// testE2EProcessor processes messages and collects them for verification.
type testE2EProcessor struct {
	messages []rabbitmq.Message[e2eTestMessage]
	mu       sync.Mutex
}

func (p *testE2EProcessor) Process(_ context.Context, msg rabbitmq.Message[e2eTestMessage]) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, msg)
	return nil
}

func (p *testE2EProcessor) GetName() string {
	return "test-e2e-processor"
}

func (p *testE2EProcessor) GetMessages() []rabbitmq.Message[e2eTestMessage] {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]rabbitmq.Message[e2eTestMessage]{}, p.messages...)
}

// TestE2E_PublishAndConsume tests the full publish → consume cycle.
func TestE2E_PublishAndConsume(t *testing.T) {
	t.Parallel()

	amqpURL, cleanup := testhelpers.SetupRabbitMQContainer(t)
	defer cleanup()

	queueName := "test-e2e-queue"
	testhelpers.CreateRabbitMQQueue(t, amqpURL, queueName, false)

	// Publish messages
	messages := []e2eTestMessage{
		{ID: 1, Text: "first message", Timestamp: time.Now()},
		{ID: 2, Text: "second message", Timestamp: time.Now()},
		{ID: 3, Text: "third message", Timestamp: time.Now()},
	}

	for _, msg := range messages {
		testhelpers.PublishRabbitMQMessage(t, amqpURL, "", queueName, msg)
	}

	// Consume and verify
	consumed := testhelpers.ConsumeRabbitMQMessages(t, amqpURL, queueName, len(messages), 10*time.Second)
	require.Len(t, consumed, len(messages))

	// Verify messages
	for i, msg := range consumed {
		var decoded e2eTestMessage
		err := json.Unmarshal(msg.Body, &decoded)
		require.NoError(t, err)
		assert.Equal(t, messages[i].ID, decoded.ID)
		assert.Equal(t, messages[i].Text, decoded.Text)
	}
}

// TestE2E_DirectExchange tests direct exchange routing.
func TestE2E_DirectExchange(t *testing.T) {
	t.Parallel()

	amqpURL, cleanup := testhelpers.SetupRabbitMQContainer(t)
	defer cleanup()

	exchangeName := "test-direct-exchange"
	queueName := "test-direct-queue"
	routingKey := "test.routing.key"

	// Setup
	testhelpers.CreateRabbitMQExchange(t, amqpURL, exchangeName, "direct")
	testhelpers.CreateRabbitMQQueue(t, amqpURL, queueName, false)
	testhelpers.BindRabbitMQQueue(t, amqpURL, queueName, exchangeName, routingKey)

	// Publish to exchange with routing key
	msg := e2eTestMessage{ID: 1, Text: "routed message", Timestamp: time.Now()}
	testhelpers.PublishRabbitMQMessage(t, amqpURL, exchangeName, routingKey, msg)

	// Consume
	consumed := testhelpers.ConsumeRabbitMQMessages(t, amqpURL, queueName, 1, 5*time.Second)
	require.Len(t, consumed, 1)

	var decoded e2eTestMessage
	err := json.Unmarshal(consumed[0].Body, &decoded)
	require.NoError(t, err)
	assert.Equal(t, msg.ID, decoded.ID)
	assert.Equal(t, msg.Text, decoded.Text)
}

// TestE2E_FanoutExchange tests fanout exchange (broadcast).
func TestE2E_FanoutExchange(t *testing.T) {
	t.Parallel()

	amqpURL, cleanup := testhelpers.SetupRabbitMQContainer(t)
	defer cleanup()

	exchangeName := "test-fanout-exchange"
	queue1 := "test-fanout-queue-1"
	queue2 := "test-fanout-queue-2"

	// Setup
	testhelpers.CreateRabbitMQExchange(t, amqpURL, exchangeName, "fanout")
	testhelpers.CreateRabbitMQQueue(t, amqpURL, queue1, false)
	testhelpers.CreateRabbitMQQueue(t, amqpURL, queue2, false)
	testhelpers.BindRabbitMQQueue(t, amqpURL, queue1, exchangeName, "")
	testhelpers.BindRabbitMQQueue(t, amqpURL, queue2, exchangeName, "")

	// Publish to fanout exchange
	msg := e2eTestMessage{ID: 1, Text: "broadcast message", Timestamp: time.Now()}
	testhelpers.PublishRabbitMQMessage(t, amqpURL, exchangeName, "", msg)

	// Both queues should receive the message
	consumed1 := testhelpers.ConsumeRabbitMQMessages(t, amqpURL, queue1, 1, 5*time.Second)
	consumed2 := testhelpers.ConsumeRabbitMQMessages(t, amqpURL, queue2, 1, 5*time.Second)

	require.Len(t, consumed1, 1)
	require.Len(t, consumed2, 1)

	// Verify both received same message
	var decoded1, decoded2 e2eTestMessage
	err := json.Unmarshal(consumed1[0].Body, &decoded1)
	require.NoError(t, err)
	err = json.Unmarshal(consumed2[0].Body, &decoded2)
	require.NoError(t, err)

	assert.Equal(t, msg.ID, decoded1.ID)
	assert.Equal(t, msg.ID, decoded2.ID)
	assert.Equal(t, msg.Text, decoded1.Text)
	assert.Equal(t, msg.Text, decoded2.Text)
}

// TestE2E_TopicExchange tests topic exchange pattern matching.
func TestE2E_TopicExchange(t *testing.T) {
	t.Parallel()

	amqpURL, cleanup := testhelpers.SetupRabbitMQContainer(t)
	defer cleanup()

	exchangeName := "test-topic-exchange"
	queue1 := "test-topic-queue-1"
	queue2 := "test-topic-queue-2"

	// Setup
	testhelpers.CreateRabbitMQExchange(t, amqpURL, exchangeName, "topic")
	testhelpers.CreateRabbitMQQueue(t, amqpURL, queue1, false)
	testhelpers.CreateRabbitMQQueue(t, amqpURL, queue2, false)
	testhelpers.BindRabbitMQQueue(t, amqpURL, queue1, exchangeName, "test.*.message")
	testhelpers.BindRabbitMQQueue(t, amqpURL, queue2, exchangeName, "test.important.*")

	// Publish with different routing keys
	msg1 := e2eTestMessage{ID: 1, Text: "info message", Timestamp: time.Now()}
	msg2 := e2eTestMessage{ID: 2, Text: "important message", Timestamp: time.Now()}

	testhelpers.PublishRabbitMQMessage(t, amqpURL, exchangeName, "test.info.message", msg1)
	testhelpers.PublishRabbitMQMessage(t, amqpURL, exchangeName, "test.important.alert", msg2)

	// queue1 should receive msg1 (matches test.*.message)
	consumed1 := testhelpers.ConsumeRabbitMQMessages(t, amqpURL, queue1, 1, 5*time.Second)
	require.Len(t, consumed1, 1)

	var decoded1 e2eTestMessage
	err := json.Unmarshal(consumed1[0].Body, &decoded1)
	require.NoError(t, err)
	assert.Equal(t, msg1.ID, decoded1.ID)

	// queue2 should receive msg2 (matches test.important.*)
	consumed2 := testhelpers.ConsumeRabbitMQMessages(t, amqpURL, queue2, 1, 5*time.Second)
	require.Len(t, consumed2, 1)

	var decoded2 e2eTestMessage
	err = json.Unmarshal(consumed2[0].Body, &decoded2)
	require.NoError(t, err)
	assert.Equal(t, msg2.ID, decoded2.ID)
}
