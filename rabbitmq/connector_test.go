package rabbitmq_test

import (
	"testing"
	"time"

	"github.com/sergeyslonimsky/core/rabbitmq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewRabbitConnector tests creating a connector with default options.
func TestNewRabbitConnector(t *testing.T) {
	t.Parallel()

	cfg := rabbitmq.Config{
		Host:     "localhost",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
	}

	connector := rabbitmq.NewRabbitConnector(cfg)
	require.NotNil(t, connector)
	assert.NotNil(t, connector.GetConnector())
}

// TestNewRabbitConnector_WithReconnectionOpts tests connector with custom reconnection options.
func TestNewRabbitConnector_WithReconnectionOpts(t *testing.T) {
	t.Parallel()

	cfg := rabbitmq.Config{
		Host:     "localhost",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
	}

	connector := rabbitmq.NewRabbitConnector(
		cfg,
		rabbitmq.WithConnectorReconnectionOpts(5, 3*time.Second),
	)

	require.NotNil(t, connector)
	assert.NotNil(t, connector.GetConnector())
}

// TestNewRabbitConnector_WithReconnectionOpts_Multiple tests connector with all options combined.
func TestNewRabbitConnector_WithReconnectionOpts_Multiple(t *testing.T) {
	t.Parallel()

	cfg := rabbitmq.Config{
		Host:     "localhost",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
	}

	connector := rabbitmq.NewRabbitConnector(
		cfg,
		rabbitmq.WithConnectorReconnectionOpts(10, 5*time.Second),
	)

	require.NotNil(t, connector)
	assert.NotNil(t, connector.GetConnector())
}
