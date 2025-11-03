package rabbitmq_test

import (
	"testing"

	"github.com/sergeyslonimsky/core/rabbitmq"
	"github.com/stretchr/testify/assert"
)

func TestConfig_dsn(t *testing.T) {
	t.Parallel()

	cfg := rabbitmq.Config{
		Host:     "host",
		Port:     "1234",
		User:     "user",
		Password: "pass",
	}

	assert.Equal(t, "amqp://user:pass@host:1234", cfg.DSN())
}
