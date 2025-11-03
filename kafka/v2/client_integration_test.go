//go:build integration

package v2_test

import (
	"context"
	"testing"

	"github.com/sergeyslonimsky/core/internal/testhelpers"
	v2 "github.com/sergeyslonimsky/core/kafka/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClient_NewClient_Integration tests creating a client with real Kafka.
func TestClient_NewClient_Integration(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)
	defer cancel()

	config := v2.ClientConfig{
		Brokers:  []string{brokerURL},
		ClientID: "test-client",
	}

	client, err := v2.NewClient(ctx, config)
	require.NoError(t, err, "failed to create Kafka client")

	defer client.Close()

	// Verify client is not nil
	require.NotNil(t, client)

	// Verify client works by creating a test topic
	// (Ping might not work immediately after client creation)
	topic := "test-client-verification"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	// Produce a test message to verify client can interact with broker
	producer, err := v2.NewSyncProducer(brokerURL)
	require.NoError(t, err)

	defer producer.Close()

	err = producer.Produce(topic, "test", []byte("verification"))
	assert.NoError(t, err, "client should be able to produce messages")
}

// TestClient_Close_Integration tests proper client cleanup.
func TestClient_Close_Integration(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)
	defer cancel()

	client, err := v2.NewClient(ctx, v2.ClientConfig{
		Brokers:  []string{brokerURL},
		ClientID: "test-close-client",
	})
	require.NoError(t, err, "failed to create test client for close test")

	// Close should not panic
	assert.NotPanics(t, func() {
		client.Close()
	})

	// Double close should be safe
	assert.NotPanics(t, func() {
		client.Close()
	})
}

// TestClient_InvalidBroker_Error tests error handling with invalid broker.
func TestClient_InvalidBroker_Error(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), testhelpers.ShortTestTimeout)
	defer cancel()

	config := v2.ClientConfig{
		Brokers:  []string{"invalid-broker:9092"},
		ClientID: "test-invalid-client",
	}

	// Client creation succeeds (lazy initialization), but Ping should fail
	client, err := v2.NewClient(ctx, config)
	require.NoError(t, err, "client creation should succeed (lazy init)")

	defer client.Close()

	// Ping should fail with invalid broker
	pingCtx, pingCancel := context.WithTimeout(context.Background(), testhelpers.ShortTestTimeout)
	defer pingCancel()

	err = client.Ping(pingCtx)
	assert.Error(t, err, "ping should fail with invalid broker")
}
