package v2_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	v2 "github.com/sergeyslonimsky/core/kafka/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockProducer for testing JSONProducer.
type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) Produce(topic, key string, message []byte) error {
	args := m.Called(topic, key, message)

	return args.Error(0) //nolint:wrapcheck
}

func TestNewSyncProducer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		brokers string
		wantErr bool
	}{
		{
			name:    "valid brokers",
			brokers: "localhost:9092",
			wantErr: false,
		},
		{
			name:    "empty brokers",
			brokers: "",
			wantErr: true,
		},
		{
			name:    "multiple brokers",
			brokers: "localhost:9092,localhost:9093",
			wantErr: false,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			producer, err := v2.NewSyncProducer(testCase.brokers)

			if testCase.wantErr {
				require.Error(t, err)
				assert.Nil(t, producer)
			} else {
				require.NoError(t, err)
				require.NotNil(t, producer)
				assert.Equal(t, 30*time.Second, producer.GetTimeout())
				producer.Close()
			}
		})
	}
}

func TestNewSyncProducerWithTimeout(t *testing.T) {
	t.Parallel()

	timeout := 5 * time.Second
	producer, err := v2.NewSyncProducerWithTimeout("localhost:9092", timeout)

	require.NoError(t, err)
	require.NotNil(t, producer)
	assert.Equal(t, timeout, producer.GetTimeout())
	producer.Close()
}

func TestSyncProducer_Close(t *testing.T) {
	t.Parallel()

	producer, err := v2.NewSyncProducer("localhost:9092")
	require.NoError(t, err)
	require.NotNil(t, producer)

	err = producer.Close()
	require.NoError(t, err)

	// Should be safe to call multiple times
	err = producer.Close()
	require.NoError(t, err)
}

func TestJSONProducer_Produce(t *testing.T) {
	t.Parallel()

	type TestMessage struct {
		ID   int    `json:"id"`
		Text string `json:"text"`
	}

	mockProducer := &MockProducer{}
	jsonProducer := v2.NewJSONProducer[TestMessage]("test-topic", "default-key", mockProducer)

	message := TestMessage{ID: 1, Text: "hello"}
	expectedBytes, err := json.Marshal(message)
	require.NoError(t, err)

	expectedBytes = append(expectedBytes, '\n') // json.Encoder adds \n

	mockProducer.On("Produce", "test-topic", "default-key", expectedBytes).Return(nil)

	err = jsonProducer.Produce(context.Background(), message)

	require.NoError(t, err)
	mockProducer.AssertExpectations(t)
}

func TestJSONProducer_ProduceWithKey(t *testing.T) {
	t.Parallel()

	type TestMessage struct {
		ID   int    `json:"id"`
		Text string `json:"text"`
	}

	mockProducer := &MockProducer{}
	jsonProducer := v2.NewJSONProducer[TestMessage]("test-topic", "default-key", mockProducer)

	message := TestMessage{ID: 2, Text: "world"}
	customKey := "custom-key"
	expectedBytes, err := json.Marshal(message)
	require.NoError(t, err)

	expectedBytes = append(expectedBytes, '\n') // json.Encoder adds \n

	mockProducer.On("Produce", "test-topic", customKey, expectedBytes).Return(nil)

	err = jsonProducer.ProduceWithKey(context.Background(), customKey, message)

	require.NoError(t, err)
	mockProducer.AssertExpectations(t)
}

func TestJSONProducer_ProduceError(t *testing.T) {
	t.Parallel()

	type TestMessage struct {
		ID   int    `json:"id"`
		Text string `json:"text"`
	}

	mockProducer := &MockProducer{}
	jsonProducer := v2.NewJSONProducer[TestMessage]("test-topic", "default-key", mockProducer)

	message := TestMessage{ID: 1, Text: "hello"}
	expectedBytes, err := json.Marshal(message)
	require.NoError(t, err)

	expectedBytes = append(expectedBytes, '\n')

	expectedErr := v2.ErrProduceFailed
	mockProducer.On("Produce", "test-topic", "default-key", expectedBytes).Return(expectedErr)

	err = jsonProducer.Produce(context.Background(), message)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "produce message")
	require.ErrorIs(t, err, v2.ErrProduceFailed)
	mockProducer.AssertExpectations(t)
}
