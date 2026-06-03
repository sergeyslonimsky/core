package di

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeEtcdKV is an in-memory etcdKV implementation used by the di package
// tests. It records Close calls, supports per-key fan-out to multiple
// watchers, and exposes Set/Delete/SetError helpers so tests can drive the
// watcher behaviour deterministically.
type fakeEtcdKV struct {
	mu       sync.Mutex
	data     map[string][]byte
	watchers map[string][]*fakeWatcher
	closed   atomic.Int32
}

type fakeWatcher struct {
	ch     chan watchEvent
	cancel context.CancelFunc
	done   chan struct{}
}

// Compile-time guarantee that fakeEtcdKV implements etcdKV. Stage 4 tests
// rely on this so they can pass *fakeEtcdKV through any place expecting
// etcdKV.
var _ etcdKV = (*fakeEtcdKV)(nil)

func newFakeEtcdKV() *fakeEtcdKV {
	return &fakeEtcdKV{
		data:     make(map[string][]byte),
		watchers: make(map[string][]*fakeWatcher),
	}
}

// Get returns ErrEtcdKeyNotFound when key is absent, otherwise a copy of
// the stored bytes (so callers mutating the slice do not affect future
// Gets).
func (f *fakeEtcdKV) Get(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	v, ok := f.data[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrEtcdKeyNotFound, key)
	}

	return bytes.Clone(v), nil
}

// Watch registers a fresh subscriber for key. The returned channel is
// closed when ctx is cancelled, when SetError is called for the key, or
// when Close is called on the fake.
func (f *fakeEtcdKV) Watch(ctx context.Context, key string) <-chan watchEvent {
	ctx, cancel := context.WithCancel(ctx)

	w := &fakeWatcher{
		ch:     make(chan watchEvent, watchChannelBuffer),
		cancel: cancel,
		done:   make(chan struct{}),
	}

	f.mu.Lock()
	f.watchers[key] = append(f.watchers[key], w)
	f.mu.Unlock()

	go func() {
		<-ctx.Done()
		f.removeWatcher(key, w)
	}()

	return w.ch
}

// Close releases the fake. Currently-open watcher channels are closed so
// outstanding Watch goroutines unblock. Safe to call multiple times; the
// counter increments on every call.
func (f *fakeEtcdKV) Close() error {
	f.closed.Add(1)

	f.mu.Lock()

	all := f.watchers
	f.watchers = make(map[string][]*fakeWatcher)

	f.mu.Unlock()

	for _, ws := range all {
		for _, w := range ws {
			finishWatcher(w)
		}
	}

	return nil
}

// CloseCount reports how many times Close has been called.
func (f *fakeEtcdKV) CloseCount() int {
	return int(f.closed.Load())
}

// Set stores value (deep copy) under key and pushes a Put event to every
// current subscriber for the key.
func (f *fakeEtcdKV) Set(key string, value []byte) {
	stored := bytes.Clone(value)

	f.mu.Lock()
	f.data[key] = stored
	subs := append([]*fakeWatcher(nil), f.watchers[key]...)
	f.mu.Unlock()

	for _, w := range subs {
		f.sendEvent(w, watchEvent{Type: eventPut, Value: bytes.Clone(stored)})
	}
}

// WaitForWatcher blocks until at least n watchers are registered for key,
// or the deadline expires. Returns true if the count was reached. Used by
// tests to avoid the race where a Set is delivered before the watcher
// goroutine has registered its subscription.
func (f *fakeEtcdKV) WaitForWatcher(key string, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for {
		f.mu.Lock()
		count := len(f.watchers[key])
		f.mu.Unlock()

		if count >= n {
			return true
		}

		if time.Now().After(deadline) {
			return false
		}

		time.Sleep(time.Millisecond)
	}
}

// SetData stores value (deep copy) under key WITHOUT pushing an event to
// any subscriber. Used by tests that need to seed the data seen by a
// future kv.Get (e.g. resync after a terminal stream error) without also
// triggering a Put delivery on a still-open channel.
func (f *fakeEtcdKV) SetData(key string, value []byte) {
	stored := bytes.Clone(value)

	f.mu.Lock()
	f.data[key] = stored
	f.mu.Unlock()
}

// Delete removes the value under key and pushes a Delete event to every
// current subscriber.
func (f *fakeEtcdKV) Delete(key string) {
	f.mu.Lock()
	delete(f.data, key)
	subs := append([]*fakeWatcher(nil), f.watchers[key]...)
	f.mu.Unlock()

	for _, w := range subs {
		f.sendEvent(w, watchEvent{Type: eventDelete})
	}
}

// SetError pushes a terminal-error event to every current subscriber for
// key, then closes their channels. Subsequent Watch(key) calls receive a
// fresh channel — emulating "watcher needs to reconnect".
func (f *fakeEtcdKV) SetError(key string, err error) {
	f.mu.Lock()
	subs := f.watchers[key]
	delete(f.watchers, key)
	f.mu.Unlock()

	for _, w := range subs {
		f.sendEvent(w, watchEvent{Err: err})
		finishWatcher(w)
	}
}

// sendEvent delivers ev on w.ch unless the watcher has already finished.
func (f *fakeEtcdKV) sendEvent(w *fakeWatcher, ev watchEvent) {
	select {
	case <-w.done:
		return
	default:
	}

	select {
	case w.ch <- ev:
	case <-w.done:
	}
}

// removeWatcher detaches w from key's subscriber list and finishes it.
// Idempotent — safe if SetError/Close already removed the watcher.
func (f *fakeEtcdKV) removeWatcher(key string, w *fakeWatcher) {
	f.mu.Lock()

	subs := f.watchers[key]

	for i, s := range subs {
		if s == w {
			f.watchers[key] = append(subs[:i], subs[i+1:]...)

			break
		}
	}

	if len(f.watchers[key]) == 0 {
		delete(f.watchers, key)
	}

	f.mu.Unlock()

	finishWatcher(w)
}

// finishWatcher idempotently closes a watcher's channel and signals done.
func finishWatcher(w *fakeWatcher) {
	select {
	case <-w.done:
		return
	default:
	}

	// Best-effort signal that we are finishing. A concurrent finishWatcher
	// caller may also reach this point; the close-once guard below makes
	// the operation idempotent.
	defer func() {
		_ = recover()
	}()

	close(w.done)
	close(w.ch)

	if w.cancel != nil {
		w.cancel()
	}
}

// ----- contract tests for fakeEtcdKV -----
//
// fakeEtcdKV is test infrastructure consumed by every other test in
// the package. We do not test its concurrency or idempotency surface
// in isolation — those properties surface through the consumers. We
// keep two tests for the contract pieces that consumers rely on but
// would only fail in subtle ways if broken:
//
//   - Get/Set/Delete round-trip (so consumers can trust state).
//   - SetError emits a terminal event AND closes the channel (so the
//     loader's reconnect path is exercised against the same contract
//     the real clientv3 provides).

func TestFakeEtcdKV_GetSetDeleteRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()

	_, err := f.Get(t.Context(), "k")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEtcdKeyNotFound)

	f.Set("k", []byte("v"))

	got, err := f.Get(t.Context(), "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), got)

	f.Delete("k")

	_, err = f.Get(t.Context(), "k")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEtcdKeyNotFound)
}

func TestFakeEtcdKV_SetErrorEmitsErrorAndCloses(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	ch := f.Watch(t.Context(), "k")
	errBoom := errors.New("boom")
	f.SetError("k", errBoom)

	ev := mustReceive(t, ch)
	require.Error(t, ev.Err)
	require.ErrorIs(t, ev.Err, errBoom)

	_, ok := receiveWithTimeout(t, ch, time.Second)
	assert.False(t, ok, "channel must be closed after terminal error")
}

// mustReceive reads one event from ch within a generous timeout, failing
// the test if the channel is closed or no event arrives in time.
func mustReceive(t *testing.T, ch <-chan watchEvent) watchEvent {
	t.Helper()

	ev, ok := receiveWithTimeout(t, ch, 2*time.Second)
	require.True(t, ok, "expected event but channel was closed")

	return ev
}

func receiveWithTimeout(t *testing.T, ch <-chan watchEvent, d time.Duration) (watchEvent, bool) {
	t.Helper()

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case ev, ok := <-ch:
		return ev, ok
	case <-timer.C:
		t.Fatalf("timeout waiting for event after %v", d)

		return watchEvent{}, false
	}
}
