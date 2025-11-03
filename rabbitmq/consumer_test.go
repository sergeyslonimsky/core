package rabbitmq_test

import (
	"context"
	"testing"

	"github.com/sergeyslonimsky/core/rabbitmq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testMessage is a test message type for consumer tests.
type testMessage struct {
	ID   int    `json:"id"`
	Text string `json:"text"`
}

// mockProcessor is a mock processor for testing.
type mockProcessor struct {
	name string
}

func (m *mockProcessor) Process(_ context.Context, _ rabbitmq.Message[testMessage]) error {
	return nil
}

func (m *mockProcessor) GetName() string {
	return m.name
}

// TestNewConsumer tests creating a consumer.
func TestNewConsumer(t *testing.T) {
	t.Parallel()

	processor := &mockProcessor{name: "test-processor"}
	consumer := rabbitmq.NewConsumer("test-queue", processor)

	require.NotNil(t, consumer)
}

// TestNewConsumer_WithConsumerConfig tests consumer with ConsumerConfig option.
func TestNewConsumer_WithConsumerConfig(t *testing.T) {
	t.Parallel()

	processor := &mockProcessor{name: "test-processor"}
	config := rabbitmq.ConsumerConfig{
		AutoAck:   true,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
		Args:      nil,
	}

	consumer := rabbitmq.NewConsumer(
		"test-queue",
		processor,
		rabbitmq.WithConsumerConfig[testMessage](config),
	)

	require.NotNil(t, consumer)
}

// TestNewConsumer_WithExchange tests consumer with Exchange option.
func TestNewConsumer_WithExchange(t *testing.T) {
	t.Parallel()

	processor := &mockProcessor{name: "test-processor"}
	exchangeConfig := rabbitmq.ExchangeConfig{
		Name:       "test-exchange",
		Kind:       rabbitmq.ExchangeKindDirect,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}

	consumer := rabbitmq.NewConsumer(
		"test-queue",
		processor,
		rabbitmq.WithExchange[testMessage](exchangeConfig),
	)

	require.NotNil(t, consumer)
}

// TestNewConsumer_WithQueue tests consumer with Queue option.
func TestNewConsumer_WithQueue(t *testing.T) {
	t.Parallel()

	processor := &mockProcessor{name: "test-processor"}
	queueConfig := rabbitmq.QueueConfig{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}

	consumer := rabbitmq.NewConsumer(
		"test-queue",
		processor,
		rabbitmq.WithQueue[testMessage](queueConfig),
	)

	require.NotNil(t, consumer)
}

// TestNewConsumer_WithBindQueue tests consumer with BindQueue option.
func TestNewConsumer_WithBindQueue(t *testing.T) {
	t.Parallel()

	processor := &mockProcessor{name: "test-processor"}
	bindConfig := rabbitmq.BindQueueConfig{
		Exchange:   "test-exchange",
		RoutingKey: "test.routing.key",
		NoWait:     false,
		Args:       nil,
	}

	consumer := rabbitmq.NewConsumer(
		"test-queue",
		processor,
		rabbitmq.WithBindQueue[testMessage](bindConfig),
	)

	require.NotNil(t, consumer)
}

// TestNewConsumer_WithConsumeOpts tests consumer with ConsumeOpts option.
func TestNewConsumer_WithConsumeOpts(t *testing.T) {
	t.Parallel()

	processor := &mockProcessor{name: "test-processor"}
	consumeOpts := rabbitmq.ConsumeOpts{
		PrefetchCount: 10,
		PrefetchSize:  0,
		Global:        false,
	}

	consumer := rabbitmq.NewConsumer(
		"test-queue",
		processor,
		rabbitmq.WithConsumeOpts[testMessage](consumeOpts),
	)

	require.NotNil(t, consumer)
}

// TestNewConsumer_WithCustomBodyMarshaller tests consumer with custom marshaller.
func TestNewConsumer_WithCustomBodyMarshaller(t *testing.T) {
	t.Parallel()

	processor := &mockProcessor{name: "test-processor"}
	marshaller := func(body []byte, payload *testMessage) error {
		payload.ID = 42
		payload.Text = string(body)

		return nil
	}

	consumer := rabbitmq.NewConsumer(
		"test-queue",
		processor,
		rabbitmq.WithCustomBodyMarshaller[testMessage](marshaller),
	)

	require.NotNil(t, consumer)
}

// TestNewConsumer_WithAllOptions tests consumer with multiple options combined.
func TestNewConsumer_WithAllOptions(t *testing.T) {
	t.Parallel()

	processor := &mockProcessor{name: "test-processor"}
	consumer := rabbitmq.NewConsumer(
		"test-queue",
		processor,
		rabbitmq.WithConsumerConfig[testMessage](rabbitmq.ConsumerConfig{
			AutoAck: false,
		}),
		rabbitmq.WithExchange[testMessage](rabbitmq.ExchangeConfig{
			Name: "test-exchange",
			Kind: rabbitmq.ExchangeKindTopic,
		}),
		rabbitmq.WithQueue[testMessage](rabbitmq.QueueConfig{
			Durable: true,
		}),
		rabbitmq.WithBindQueue[testMessage](rabbitmq.BindQueueConfig{
			Exchange:   "test-exchange",
			RoutingKey: "test.*",
		}),
		rabbitmq.WithConsumeOpts[testMessage](rabbitmq.ConsumeOpts{
			PrefetchCount: 5,
		}),
	)

	require.NotNil(t, consumer)
}

// TestExchangeKind_Constants tests exchange kind constants.
func TestExchangeKind_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, rabbitmq.ExchangeKindDirect, rabbitmq.ExchangeKind("direct"))
	assert.Equal(t, rabbitmq.ExchangeKindFanout, rabbitmq.ExchangeKind("fanout"))
	assert.Equal(t, rabbitmq.ExchangeKindHeaders, rabbitmq.ExchangeKind("headers"))
	assert.Equal(t, rabbitmq.ExchangeKindTopic, rabbitmq.ExchangeKind("topic"))
}

// TestMessage tests Message struct.
func TestMessage(t *testing.T) {
	t.Parallel()

	msg := rabbitmq.Message[testMessage]{
		Payload: testMessage{
			ID:   1,
			Text: "test message",
		},
	}

	assert.Equal(t, 1, msg.Payload.ID)
	assert.Equal(t, "test message", msg.Payload.Text)
}
