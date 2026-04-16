package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/kafka"
)

// MockPublisher implements kafka.Publisher for JSONProducer tests.
type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) Produce(topic, key string, message []byte) error {
	args := m.Called(topic, key, message)

	return args.Error(0) //nolint:wrapcheck
}

func TestNewProducer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  kafka.ProducerConfig
		wantErr bool
	}{
		{
			name:    "valid brokers",
			config:  kafka.ProducerConfig{Brokers: []string{"localhost:9092"}},
			wantErr: false,
		},
		{
			name:    "empty brokers",
			config:  kafka.ProducerConfig{Brokers: nil},
			wantErr: true,
		},
		{
			name:    "multiple brokers",
			config:  kafka.ProducerConfig{Brokers: []string{"localhost:9092", "localhost:9093"}},
			wantErr: false,
		},
	}

	ctx := context.Background()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			producer, err := kafka.NewProducer(tc.config)

			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, producer)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, producer)
			require.NoError(t, producer.Shutdown(ctx))
		})
	}
}

func TestNewProducer_WithProduceTimeout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	timeout := 5 * time.Second
	producer, err := kafka.NewProducer(
		kafka.ProducerConfig{Brokers: []string{"localhost:9092"}},
		kafka.WithProduceTimeout(timeout),
	)
	require.NoError(t, err)
	require.NotNil(t, producer)

	require.NoError(t, producer.Shutdown(ctx))
}

func TestJSONProducer_Produce(t *testing.T) {
	t.Parallel()

	type TestMessage struct {
		ID   int    `json:"id"`
		Text string `json:"text"`
	}

	mockPub := &MockPublisher{}
	jsonProducer := kafka.NewJSONProducer[TestMessage]("test-topic", "default-key", mockPub)

	message := TestMessage{ID: 1, Text: "hello"}
	expectedBytes, err := json.Marshal(message)
	require.NoError(t, err)

	expectedBytes = append(expectedBytes, '\n') // json.Encoder appends '\n'

	mockPub.On("Produce", "test-topic", "default-key", expectedBytes).Return(nil)

	err = jsonProducer.Produce(context.Background(), message)
	require.NoError(t, err)
	mockPub.AssertExpectations(t)
}

func TestJSONProducer_ProduceWithKey(t *testing.T) {
	t.Parallel()

	type TestMessage struct {
		ID   int    `json:"id"`
		Text string `json:"text"`
	}

	mockPub := &MockPublisher{}
	jsonProducer := kafka.NewJSONProducer[TestMessage]("test-topic", "default-key", mockPub)

	message := TestMessage{ID: 2, Text: "world"}
	customKey := "custom-key"
	expectedBytes, err := json.Marshal(message)
	require.NoError(t, err)

	expectedBytes = append(expectedBytes, '\n')

	mockPub.On("Produce", "test-topic", customKey, expectedBytes).Return(nil)

	err = jsonProducer.ProduceWithKey(context.Background(), customKey, message)
	require.NoError(t, err)
	mockPub.AssertExpectations(t)
}

func TestJSONProducer_ProduceError(t *testing.T) {
	t.Parallel()

	type TestMessage struct {
		ID   int    `json:"id"`
		Text string `json:"text"`
	}

	expectedErr := errors.New("upstream broker unavailable")
	mockPub := &MockPublisher{}
	jsonProducer := kafka.NewJSONProducer[TestMessage]("test-topic", "default-key", mockPub)

	message := TestMessage{ID: 1, Text: "hello"}
	expectedBytes, err := json.Marshal(message)
	require.NoError(t, err)

	expectedBytes = append(expectedBytes, '\n')

	mockPub.On("Produce", "test-topic", "default-key", expectedBytes).Return(expectedErr)

	err = jsonProducer.Produce(context.Background(), message)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "produce message")
	require.ErrorIs(t, err, expectedErr)
	mockPub.AssertExpectations(t)
}
