// Package retry provides a minimal, dependency-free bounded retry used by the
// core resource constructors to smooth transient connection failures at
// startup — e.g. a database or cache that is not yet reachable the moment the
// pod boots. It is intentionally tiny: fixed backoff, context-aware, no jitter.
package retry

import (
	"context"
	"errors"
	"time"
)

// Do calls fn up to 1+attempts times: one initial attempt plus attempts
// retries. Between tries it waits backoff, aborting early if ctx is done.
// It returns nil on the first success, the last fn error joined with the
// context error when ctx is cancelled mid-wait, or the last fn error when the
// retry budget is exhausted.
//
// attempts <= 0 or backoff <= 0 collapses to a single attempt (no retry), so
// Do is a transparent no-op wrapper when the caller did not opt in.
func Do(ctx context.Context, attempts int, backoff time.Duration, operation func(context.Context) error) error {
	err := operation(ctx)
	if err == nil || attempts <= 0 || backoff <= 0 {
		return err
	}

	timer := time.NewTimer(backoff)
	defer timer.Stop()

	for range attempts {
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}

		err = operation(ctx)
		if err == nil {
			return nil
		}

		timer.Reset(backoff)
	}

	return err
}
