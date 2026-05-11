package di

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ErrInvalidEtcdRequestTimeout is returned by NewConfig at startup when
// the value of APP_CONFIG_ETCD_REQUEST_TIMEOUT does not parse as a Go
// duration (e.g. "5s", "500ms"). Fail-fast — startup is the right place
// to surface a malformed env var.
var ErrInvalidEtcdRequestTimeout = errors.New("invalid etcd request timeout")

// watchChannelBuffer is the size of the buffered channel returned by
// etcdKV.Watch. Sized to absorb short bursts of events without blocking
// the producer goroutine while consumers process updates.
const watchChannelBuffer = 16

// Default timeouts for the real etcd v3 client. Local to etcdclient.go
// so config.go doesn't need to know about transport-level concerns.
const (
	defaultEtcdDialTimeout    = 5 * time.Second
	defaultEtcdRequestTimeout = 5 * time.Second
)

// watchEventType distinguishes PUT and DELETE events from etcd Watch.
type watchEventType int

const (
	// eventPut indicates a key was created or updated. Value holds the new payload.
	eventPut watchEventType = iota
	// eventDelete indicates a key was removed. Value is nil.
	eventDelete
)

// watchEvent is one event delivered on the channel returned by
// etcdKV.Watch. Either Type+Value is set (a normal event) OR Err is set
// (a terminal error — the channel will close immediately after delivering
// this event). Callers should drain to channel close.
type watchEvent struct {
	Type  watchEventType
	Value []byte
	Err   error
}

// etcdKV is the minimal etcd surface used by the loader. Lower-level than
// the existing master watch logic so reconnect/error paths can be
// unit-tested via fakeEtcdKV without a real etcd cluster.
type etcdKV interface {
	// Get fetches the latest value for key. Returns ErrEtcdKeyNotFound
	// (wrapped) when the key is absent.
	Get(ctx context.Context, key string) ([]byte, error)

	// Watch returns a channel that emits events for key. The channel
	// closes when ctx is cancelled OR a terminal error occurs (in the
	// latter case the last delivered event has Err != nil). The
	// implementation may auto-reconnect under the hood (clientv3 does);
	// callers are responsible for re-Get'ing after a channel close to
	// catch up on possibly-missed events.
	Watch(ctx context.Context, key string) <-chan watchEvent

	// Close releases the underlying transport. Safe to call once.
	Close() error
}

// realEtcdKV wraps a single clientv3.Client. One client can serve many
// concurrent Get/Watch calls.
type realEtcdKV struct {
	cli            *clientv3.Client
	requestTimeout time.Duration
}

// newRealEtcdKV builds a realEtcdKV bound to a single etcd endpoint.
// requestTimeout bounds every Get call; pass 0 to use
// defaultEtcdRequestTimeout.
func newRealEtcdKV(endpoint string, requestTimeout time.Duration) (*realEtcdKV, error) {
	if requestTimeout <= 0 {
		requestTimeout = defaultEtcdRequestTimeout
	}

	cli, err := clientv3.New(clientv3.Config{ //nolint:exhaustruct // zero values are fine for unused fields
		Endpoints:   []string{endpoint},
		DialTimeout: defaultEtcdDialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	return &realEtcdKV{cli: cli, requestTimeout: requestTimeout}, nil
}

// Get fetches a single key, mirroring readEtcdKey in config.go: bounded
// per-request timeout, ErrEtcdKeyNotFound when the key has no value.
func (r *realEtcdKV) Get(ctx context.Context, key string) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, r.requestTimeout)
	defer cancel()

	resp, err := r.cli.Get(reqCtx, key)
	if err != nil {
		return nil, fmt.Errorf("get etcd key %s: %w", key, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrEtcdKeyNotFound, key)
	}

	return resp.Kvs[0].Value, nil
}

// Watch opens a clientv3 watch on key and proxies events into a buffered
// channel of watchEvent. Translates clientv3 event types into our union,
// surfaces terminal errors via Err and then closes the channel. A clean
// underlying close (no error) closes our channel without synthesizing an
// error — the caller treats this as "reconnect needed".
func (r *realEtcdKV) Watch(ctx context.Context, key string) <-chan watchEvent {
	out := make(chan watchEvent, watchChannelBuffer)

	go func() {
		defer close(out)

		ch := r.cli.Watch(ctx, key)

		for resp := range ch {
			if err := resp.Err(); err != nil {
				select {
				case out <- watchEvent{Err: fmt.Errorf("etcd watch response: %w", err)}:
				case <-ctx.Done():
				}

				return
			}

			for _, ev := range resp.Events {
				event := translateEvent(ev)

				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}

// Close releases the underlying clientv3.Client. Safe to call once.
//
// Ordering contract: the caller MUST cancel the ctx passed to every
// in-flight Watch before invoking Close, and wait until those Watch
// goroutines have exited (their channels closed). Closing the client
// while a Watch goroutine is still iterating the underlying clientv3
// stream can race with the transport teardown. The dynamicWatcher in
// loader_etcd.go honours this — it cancels its context, waits on its
// WaitGroup, and only then calls kv.Close().
func (r *realEtcdKV) Close() error {
	if err := r.cli.Close(); err != nil {
		return fmt.Errorf("close etcd client: %w", err)
	}

	return nil
}

// translateEvent converts a clientv3 event into our watchEvent. PUT
// carries the new value; DELETE has no payload.
func translateEvent(ev *clientv3.Event) watchEvent {
	switch ev.Type {
	case clientv3.EventTypeDelete:
		return watchEvent{Type: eventDelete}
	default:
		// clientv3.EventTypePut and any future additions land here as PUT.
		return watchEvent{Type: eventPut, Value: ev.Kv.Value}
	}
}

// Compile-time check that realEtcdKV satisfies etcdKV.
var _ etcdKV = (*realEtcdKV)(nil)

// resolveEtcdRequestTimeout reads APP_CONFIG_ETCD_REQUEST_TIMEOUT via
// envLookup, parses it as a Go duration ("5s", "500ms", "1m") and
// returns the value. Unset or empty falls back to defaultEtcdRequestTimeout.
// A non-empty value that fails to parse yields ErrInvalidEtcdRequestTimeout.
func resolveEtcdRequestTimeout(envLookup func(string) (string, bool)) (time.Duration, error) {
	raw, ok := lookupConfigEnv(envLookup, "app.config.etcd.request.timeout")
	if !ok || raw == "" {
		return defaultEtcdRequestTimeout, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %q: %w", ErrInvalidEtcdRequestTimeout, raw, err)
	}

	if d <= 0 {
		return 0, fmt.Errorf("%w: %q: must be positive", ErrInvalidEtcdRequestTimeout, raw)
	}

	return d, nil
}
