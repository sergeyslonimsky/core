//nolint:testpackage // white-box test: validates unexported connectNATS.
package nats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr error
	}{
		{
			name:    "empty URL returns ErrEmptyURL",
			cfg:     Config{},
			wantErr: ErrEmptyURL,
		},
		{
			name:    "non-empty URL is valid",
			cfg:     Config{URL: "nats://example:4222"},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.cfg.validate()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConnectNATS_invalidURL(t *testing.T) {
	t.Parallel()

	// Bogus URL — dial must fail; we only care that we get a wrapped error.
	cfg := Config{URL: "nats://127.0.0.1:1", DialTimeout: 50 * time.Millisecond}

	_, err := connectNATS(cfg, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connect to NATS URL")
}

func TestConnectNATS_defaultsApplied(t *testing.T) {
	t.Parallel()

	// Sanity check: nil logger does not panic; defaults are applied without
	// crashing the option-build path. We don't actually dial here.
	cfg := Config{URL: "nats://127.0.0.1:1", DialTimeout: 50 * time.Millisecond}

	// Expect error (no server), but no panic on missing logger.
	_, err := connectNATS(cfg, nil, nil)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrEmptyURL)
}
