package nats_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corenats "github.com/sergeyslonimsky/core/nats"
)

var errTransient = errors.New("smtp unavailable")

func TestPermanent(t *testing.T) {
	t.Parallel()

	err := corenats.Permanent(errTransient)

	require.ErrorIs(t, err, corenats.ErrPermanent)
	require.ErrorIs(t, err, errTransient)
	assert.NoError(t, corenats.Permanent(nil))
}

func TestJSRetryPolicyHandler(t *testing.T) {
	t.Parallel()

	delays := []time.Duration{10 * time.Second, time.Minute, 5 * time.Minute}

	tests := []struct {
		name         string
		policy       corenats.JSRetryPolicy[string]
		numDelivered uint64
		err          error
		wantAction   corenats.JSErrorAction
		wantDelay    time.Duration
		wantGiveUp   bool
	}{
		{
			name:         "first failure waits the first delay",
			policy:       corenats.JSRetryPolicy[string]{Delays: delays, MaxDeliver: 5},
			numDelivered: 1,
			err:          errTransient,
			wantAction:   corenats.JSErrorActionNakDelay,
			wantDelay:    10 * time.Second,
		},
		{
			name:         "third failure waits the third delay",
			policy:       corenats.JSRetryPolicy[string]{Delays: delays, MaxDeliver: 5},
			numDelivered: 3,
			err:          errTransient,
			wantAction:   corenats.JSErrorActionNakDelay,
			wantDelay:    5 * time.Minute,
		},
		{
			name:         "last delay repeats after the list",
			policy:       corenats.JSRetryPolicy[string]{Delays: delays, MaxDeliver: 10},
			numDelivered: 7,
			err:          errTransient,
			wantAction:   corenats.JSErrorActionNakDelay,
			wantDelay:    5 * time.Minute,
		},
		{
			name:         "no metadata uses the first delay",
			policy:       corenats.JSRetryPolicy[string]{Delays: delays, MaxDeliver: 5},
			numDelivered: 0,
			err:          errTransient,
			wantAction:   corenats.JSErrorActionNakDelay,
			wantDelay:    10 * time.Second,
		},
		{
			name:         "last allowed attempt terminates and gives up",
			policy:       corenats.JSRetryPolicy[string]{Delays: delays, MaxDeliver: 5},
			numDelivered: 5,
			err:          errTransient,
			wantAction:   corenats.JSErrorActionTerm,
			wantGiveUp:   true,
		},
		{
			name:         "zero MaxDeliver never gives up on attempts",
			policy:       corenats.JSRetryPolicy[string]{Delays: delays},
			numDelivered: 50,
			err:          errTransient,
			wantAction:   corenats.JSErrorActionNakDelay,
			wantDelay:    5 * time.Minute,
		},
		{
			name:         "permanent error terminates on the first attempt",
			policy:       corenats.JSRetryPolicy[string]{Delays: delays, MaxDeliver: 5},
			numDelivered: 1,
			err:          fmt.Errorf("send: %w", corenats.Permanent(errTransient)),
			wantAction:   corenats.JSErrorActionTerm,
			wantGiveUp:   true,
		},
		{
			name:         "decode error terminates",
			policy:       corenats.JSRetryPolicy[string]{Delays: delays, MaxDeliver: 5},
			numDelivered: 1,
			err:          fmt.Errorf("%w: json unmarshal: bad", corenats.ErrDecode),
			wantAction:   corenats.JSErrorActionTerm,
			wantGiveUp:   true,
		},
		{
			name:         "no delays keeps the plain nak",
			policy:       corenats.JSRetryPolicy[string]{MaxDeliver: 5},
			numDelivered: 1,
			err:          errTransient,
			wantAction:   corenats.JSErrorActionNak,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var gaveUp []error

			policy := tt.policy
			policy.OnGiveUp = func(_ context.Context, msg corenats.Message[string], err error) {
				assert.Equal(t, tt.numDelivered, msg.NumDelivered)

				gaveUp = append(gaveUp, err)
			}

			msg := corenats.Message[string]{NumDelivered: tt.numDelivered}

			action, delay := policy.Handler()(t.Context(), msg, tt.err)

			assert.Equal(t, tt.wantAction, action)
			assert.Equal(t, tt.wantDelay, delay)

			if tt.wantGiveUp {
				require.Len(t, gaveUp, 1)
				assert.ErrorIs(t, gaveUp[0], tt.err)
			} else {
				assert.Empty(t, gaveUp)
			}
		})
	}
}

func TestJSRetryPolicyWithoutOnGiveUp(t *testing.T) {
	t.Parallel()

	policy := corenats.JSRetryPolicy[string]{MaxDeliver: 1}
	msg := corenats.Message[string]{NumDelivered: 1}

	action, delay := policy.Handler()(t.Context(), msg, errTransient)

	assert.Equal(t, corenats.JSErrorActionTerm, action)
	assert.Zero(t, delay)
}
