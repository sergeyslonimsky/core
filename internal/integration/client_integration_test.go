package integration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/internal/integration/testhelpers"
	"github.com/sergeyslonimsky/core/kafka"
)

// TestClient_NewClient_Integration verifies a kafka.Client can be created
// against a real broker and reports itself healthy after discovering
// brokers.
func TestClient_NewClient_Integration(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)
	defer cancel()

	client, err := kafka.NewClient(ctx, kafka.ClientConfig{
		Brokers:  []string{brokerURL},
		ClientID: "test-client",
	})
	require.NoError(t, err, "failed to create kafka client")
	t.Cleanup(func() { _ = client.Shutdown(context.Background()) })

	require.NotNil(t, client)
	require.NotNil(t, client.Unwrap(), "Unwrap should return the underlying *kgo.Client")

	// Produce one record via a separate producer so the underlying client
	// discovers brokers and metadata, then verify Healthcheck reports OK.
	topic := "test-client-verification"
	testhelpers.CreateTestTopic(t, brokerURL, topic, 1)

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers: []string{brokerURL},
		Topic:   topic,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = producer.Shutdown(context.Background()) })

	require.NoError(t, producer.Produce(topic, "test", []byte("verification")))

	// kafka.Client is lazy: NewClient does not hit the network, so
	// DiscoveredBrokers() is empty until some operation triggers a
	// metadata fetch. Ping the raw kgo.Client to force it — this is
	// also what a real service would do on its first Healthcheck.
	require.NoError(t, client.Unwrap().Ping(ctx), "ping should succeed against live broker")
	assert.NoError(t, client.Healthcheck(ctx),
		"Healthcheck should report healthy after brokers are discovered")
}

// TestClient_Shutdown_Idempotent verifies Shutdown can be called twice
// without panic or error — guaranteed by the internal sync.Once.
func TestClient_Shutdown_Idempotent(t *testing.T) {
	t.Parallel()

	brokerURL, cleanup := testhelpers.SetupKafkaContainer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), testhelpers.DefaultTestTimeout)
	defer cancel()

	client, err := kafka.NewClient(ctx, kafka.ClientConfig{
		Brokers:  []string{brokerURL},
		ClientID: "test-close-client",
	})
	require.NoError(t, err, "failed to create kafka client")

	assert.NoError(t, client.Shutdown(ctx), "first Shutdown should succeed")
	assert.NoError(t, client.Shutdown(ctx), "second Shutdown should be a no-op")
}

// TestClient_InvalidBroker_HealthcheckFails covers the "seed brokers
// unreachable" case: the constructor still succeeds (kgo is lazy), but
// Healthcheck reports ErrNoBrokersDiscovered.
func TestClient_InvalidBroker_HealthcheckFails(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), testhelpers.ShortTestTimeout)
	defer cancel()

	client, err := kafka.NewClient(ctx, kafka.ClientConfig{
		Brokers:  []string{"invalid-broker:9092"},
		ClientID: "test-invalid-client",
	})
	require.NoError(t, err, "client creation should succeed (lazy init)")
	t.Cleanup(func() { _ = client.Shutdown(context.Background()) })

	err = client.Healthcheck(ctx)
	assert.ErrorIs(t, err, kafka.ErrNoBrokersDiscovered,
		"Healthcheck should return ErrNoBrokersDiscovered for an unreachable seed broker")
}
