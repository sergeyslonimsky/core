package nats

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Permanent marks err as a failure that a retry cannot fix (bad input, a
// record that no longer exists). A JSRetryPolicy terminates such a message
// instead of redelivering it. errors.Is(Permanent(err), ErrPermanent) and
// errors.Is(Permanent(err), err) both hold.
func Permanent(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%w: %w", ErrPermanent, err)
}

// JSRetryPolicy builds a JSErrorHandler that spaces out redeliveries.
//
// Why: a plain NAK (JSErrorActionNak, the default) makes JetStream redeliver
// immediately — the consumer's AckWait and BackOff apply only to deliveries
// that were never acked or nak'ed. With MaxDeliver 5 a failing message burns
// all attempts within milliseconds, so a short outage of a dependency (SMTP,
// an HTTP API) loses the message.
//
// The handler returned by Handler:
//   - terminates the message (JSErrorActionTerm) when the error is
//     ErrPermanent or ErrDecode — redelivery cannot help;
//   - terminates it on the last allowed attempt (NumDelivered >= MaxDeliver)
//     and calls OnGiveUp, so the loss is visible instead of the server
//     silently stopping redelivery;
//   - otherwise NAKs with Delays[NumDelivered-1]; the last delay repeats once
//     the list is exhausted.
type JSRetryPolicy[I any] struct {
	// Delays is the wait before the next delivery after the n-th failed
	// attempt (index n-1). Empty means no delay (plain NAK behaviour).
	Delays []time.Duration

	// MaxDeliver must equal the consumer's MaxDeliver (set it through
	// WithJSConsumerConfig). Zero disables give-up detection: the server
	// still stops redelivering at its own limit, but OnGiveUp is not called.
	MaxDeliver uint64

	// OnGiveUp is called once per message that is terminated: permanent and
	// decode errors, and the last allowed attempt. Use it for an ERROR log,
	// a metric or a DLQ publish. Optional.
	OnGiveUp func(ctx context.Context, msg Message[I], err error)
}

// Handler returns the JSErrorHandler for JSConsumerHandlers.ErrorHandler.
func (p JSRetryPolicy[I]) Handler() JSErrorHandler[I] {
	return func(ctx context.Context, msg Message[I], err error) (JSErrorAction, time.Duration) {
		if errors.Is(err, ErrPermanent) || errors.Is(err, ErrDecode) || p.lastAttempt(msg) {
			if p.OnGiveUp != nil {
				p.OnGiveUp(ctx, msg, err)
			}

			return JSErrorActionTerm, 0
		}

		delay := p.delay(msg.NumDelivered)
		if delay <= 0 {
			return JSErrorActionNak, 0
		}

		return JSErrorActionNakDelay, delay
	}
}

func (p JSRetryPolicy[I]) lastAttempt(msg Message[I]) bool {
	return p.MaxDeliver > 0 && msg.NumDelivered >= p.MaxDeliver
}

// delay picks the wait after attempt numDelivered (1-based): Delays[n-1],
// the last delay once the list is exhausted. Zero numDelivered (no
// JetStream metadata) counts as the first attempt.
func (p JSRetryPolicy[I]) delay(numDelivered uint64) time.Duration {
	if len(p.Delays) == 0 {
		return 0
	}

	attempt := max(numDelivered, 1)

	var n uint64
	for _, d := range p.Delays {
		n++
		if n >= attempt {
			return d
		}
	}

	return p.Delays[len(p.Delays)-1]
}
