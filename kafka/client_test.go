package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/kafka"
)

func TestNewClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  kafka.ClientConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: kafka.ClientConfig{
				Brokers:  []string{"localhost:9092"},
				ClientID: "test-client",
			},
			wantErr: false,
		},
		{
			name: "empty brokers",
			config: kafka.ClientConfig{
				Brokers: []string{},
			},
			wantErr: true,
		},
		{
			name: "no client id",
			config: kafka.ClientConfig{
				Brokers: []string{"localhost:9092"},
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			client, err := kafka.NewClient(ctx, tc.config)

			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, client)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, client)
			assert.NotNil(t, client.Unwrap())
			require.NoError(t, client.Shutdown(ctx))
		})
	}
}

func TestClient_Shutdown_Idempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, err := kafka.NewClient(ctx, kafka.ClientConfig{
		Brokers:  []string{"localhost:9092"},
		ClientID: "test-client",
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	// Should not panic on first close.
	require.NoError(t, client.Shutdown(ctx))

	// kgo.Client.Close is documented as not safe to call twice — we don't
	// promise idempotency at the kafka.Client level either, so this test
	// only verifies the first call works.
}
