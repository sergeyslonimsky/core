package v2_test

import (
	"context"
	"testing"

	v2 "github.com/sergeyslonimsky/core/kafka/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  v2.ClientConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: v2.ClientConfig{
				Brokers:  []string{"localhost:9092"},
				ClientID: "test-client",
			},
			wantErr: false,
		},
		{
			name: "empty brokers",
			config: v2.ClientConfig{
				Brokers: []string{},
			},
			wantErr: true,
		},
		{
			name: "no client id",
			config: v2.ClientConfig{
				Brokers: []string{"localhost:9092"},
			},
			wantErr: false,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			client, err := v2.NewClient(ctx, testCase.config)

			if testCase.wantErr {
				require.Error(t, err)
				assert.Nil(t, client)
			} else {
				require.NoError(t, err)
				require.NotNil(t, client)
				assert.NotNil(t, client.GetKgoClient())
				client.Close()
			}
		})
	}
}

func TestNewClientFromBrokerString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		brokers  string
		clientID string
		wantErr  bool
	}{
		{
			name:     "single broker",
			brokers:  "localhost:9092",
			clientID: "test-client",
			wantErr:  false,
		},
		{
			name:     "multiple brokers",
			brokers:  "localhost:9092,localhost:9093,localhost:9094",
			clientID: "test-client",
			wantErr:  false,
		},
		{
			name:     "empty brokers",
			brokers:  "",
			clientID: "test-client",
			wantErr:  true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			client, err := v2.NewClientFromBrokerString(ctx, testCase.brokers, testCase.clientID)

			if testCase.wantErr {
				require.Error(t, err)
				assert.Nil(t, client)
			} else {
				require.NoError(t, err)
				require.NotNil(t, client)
				client.Close()
			}
		})
	}
}

func TestClient_Close(t *testing.T) {
	t.Parallel()

	client, err := v2.NewClient(t.Context(), v2.ClientConfig{
		Brokers:  []string{"localhost:9092"},
		ClientID: "test-client",
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	// Should not panic
	client.Close()

	// Should be safe to call multiple times
	client.Close()
}
