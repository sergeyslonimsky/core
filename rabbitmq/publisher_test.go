package rabbitmq_test

import (
	"testing"

	"github.com/sergeyslonimsky/core/rabbitmq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewRabbitPublisher tests creating a publisher with config.
func TestNewRabbitPublisher(t *testing.T) {
	t.Parallel()

	cfg := rabbitmq.Config{
		Host:     "localhost",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
	}

	publisher := rabbitmq.NewRabbitPublisher(cfg)
	require.NotNil(t, publisher)
}

// TestNewRabbitPublisherWithConnector tests creating a publisher with custom connector.
func TestNewRabbitPublisherWithConnector(t *testing.T) {
	t.Parallel()

	cfg := rabbitmq.Config{
		Host:     "localhost",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
	}

	connector := rabbitmq.NewRabbitConnector(cfg)
	publisher := rabbitmq.NewRabbitPublisherWithConnector(connector)

	require.NotNil(t, publisher)
}

// TestPublishOpts tests PublishOpts structure.
func TestPublishOpts(t *testing.T) {
	t.Parallel()

	opts := rabbitmq.PublishOpts{
		ContentType: "application/json",
		MessageID:   "test-id-123",
		AppID:       "test-app",
		UserID:      "test-user",
		Priority:    5,
		Type:        "test-type",
	}

	assert.Equal(t, "application/json", opts.ContentType)
	assert.Equal(t, "test-id-123", opts.MessageID)
	assert.Equal(t, "test-app", opts.AppID)
	assert.Equal(t, "test-user", opts.UserID)
	assert.Equal(t, uint8(5), opts.Priority)
	assert.Equal(t, "test-type", opts.Type)
}
