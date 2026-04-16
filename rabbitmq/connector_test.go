package rabbitmq_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/rabbitmq"
)

func TestNewConnector(t *testing.T) {
	t.Parallel()

	cfg := rabbitmq.Config{
		Host:     "localhost",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
	}

	connector := rabbitmq.NewConnector(cfg)
	require.NotNil(t, connector)
	assert.NotNil(t, connector.Inner())
}

func TestNewConnector_WithReconnectOptions(t *testing.T) {
	t.Parallel()

	cfg := rabbitmq.Config{
		Host:     "localhost",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
	}

	connector := rabbitmq.NewConnector(
		cfg,
		rabbitmq.WithReconnectAttempts(5),
		rabbitmq.WithReconnectWait(3*time.Second),
	)

	require.NotNil(t, connector)
	assert.NotNil(t, connector.Inner())
}
