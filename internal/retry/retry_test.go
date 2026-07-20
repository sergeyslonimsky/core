package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/internal/retry"
)

func TestDo(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")

	tests := []struct {
		name      string
		attempts  int
		backoff   time.Duration
		failUntil int // number of leading calls that fail before succeeding; -1 = always fail
		wantCalls int
		wantErrIs error
	}{
		{
			name:      "succeeds on first attempt",
			attempts:  3,
			backoff:   time.Millisecond,
			failUntil: 0,
			wantCalls: 1,
			wantErrIs: nil,
		},
		{
			name:      "no retry when attempts is zero",
			attempts:  0,
			backoff:   time.Millisecond,
			failUntil: -1,
			wantCalls: 1,
			wantErrIs: errBoom,
		},
		{
			name:      "no retry when backoff is zero",
			attempts:  3,
			backoff:   0,
			failUntil: -1,
			wantCalls: 1,
			wantErrIs: errBoom,
		},
		{
			name:      "succeeds on the last retry",
			attempts:  3,
			backoff:   time.Millisecond,
			failUntil: 3,
			wantCalls: 4,
			wantErrIs: nil,
		},
		{
			name:      "returns last error when budget exhausted",
			attempts:  2,
			backoff:   time.Millisecond,
			failUntil: -1,
			wantCalls: 3,
			wantErrIs: errBoom,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			calls := 0
			err := retry.Do(t.Context(), tt.attempts, tt.backoff, func(context.Context) error {
				calls++
				if tt.failUntil < 0 || calls <= tt.failUntil {
					return errBoom
				}

				return nil
			})

			assert.Equal(t, tt.wantCalls, calls)

			if tt.wantErrIs == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErrIs)
			}
		})
	}
}

func TestDo_ContextCancelledMidWait(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")

	ctx, cancel := context.WithCancel(t.Context())

	calls := 0
	err := retry.Do(ctx, 5, time.Hour, func(context.Context) error {
		calls++

		cancel() // cancel during the first (failed) attempt so the wait aborts

		return errBoom
	})

	require.Error(t, err)
	require.ErrorIs(t, err, errBoom)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls, "must not retry after ctx is cancelled")
}
