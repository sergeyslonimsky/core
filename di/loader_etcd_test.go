package di

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----- parseConfigBytes / loadEtcdOnce / loadEtcdPerPath -----

func TestParseConfigBytes_YAML(t *testing.T) {
	t.Parallel()

	got, err := parseConfigBytes([]byte("a:\n  b: 1\nc: hi\n"), "yaml")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"a.b": 1, "c": "hi"}, got)
}

func TestParseConfigBytes_YML(t *testing.T) {
	t.Parallel()

	got, err := parseConfigBytes([]byte("k: v"), "yml")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"k": "v"}, got)
}

func TestParseConfigBytes_JSON(t *testing.T) {
	t.Parallel()

	got, err := parseConfigBytes([]byte(`{"a":{"b":1},"c":"hi"}`), "json")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"a.b": float64(1), "c": "hi"}, got)
}

func TestParseConfigBytes_EmptyYAML(t *testing.T) {
	t.Parallel()

	got, err := parseConfigBytes(nil, "yaml")
	require.NoError(t, err)
	assert.Empty(t, got)

	got2, err := parseConfigBytes([]byte(""), "yaml")
	require.NoError(t, err)
	assert.Empty(t, got2)
}

func TestParseConfigBytes_EmptyJSON(t *testing.T) {
	t.Parallel()

	got, err := parseConfigBytes(nil, "json")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestParseConfigBytes_NullYAML(t *testing.T) {
	t.Parallel()

	got, err := parseConfigBytes([]byte("null\n"), "yaml")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestParseConfigBytes_NullJSON(t *testing.T) {
	t.Parallel()

	got, err := parseConfigBytes([]byte(`null`), "json")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestParseConfigBytes_InvalidYAML(t *testing.T) {
	t.Parallel()

	_, err := parseConfigBytes([]byte("a: [unterminated"), "yaml")
	require.Error(t, err)
}

func TestParseConfigBytes_InvalidJSON(t *testing.T) {
	t.Parallel()

	_, err := parseConfigBytes([]byte(`{not json`), "json")
	require.Error(t, err)
}

func TestParseConfigBytes_UnknownType(t *testing.T) {
	t.Parallel()

	_, err := parseConfigBytes([]byte("k=v"), "toml")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnknownConfigType))
}

func TestLoadEtcdOnce_Empty(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()

	got, err := loadEtcdOnce(t.Context(), f, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestLoadEtcdOnce_SinglePath(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	f.Set("svc/a.yaml", []byte("k: v\n"))

	got, err := loadEtcdOnce(t.Context(), f, []string{"svc/a.yaml"})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"k": "v"}, got)
}

func TestLoadEtcdOnce_TwoPathsSecondWins(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	f.Set("svc/a.yaml", []byte("k: from-a\nonlyA: 1\n"))
	f.Set("svc/b.yaml", []byte("k: from-b\nonlyB: 2\n"))

	got, err := loadEtcdOnce(t.Context(), f, []string{"svc/a.yaml", "svc/b.yaml"})
	require.NoError(t, err)
	assert.Equal(t, "from-b", got["k"])
	assert.Equal(t, 1, got["onlyA"])
	assert.Equal(t, 2, got["onlyB"])
}

func TestLoadEtcdOnce_PathNotFound(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()

	_, err := loadEtcdOnce(t.Context(), f, []string{"missing.yaml"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEtcdKeyNotFound))
	assert.Contains(t, err.Error(), "missing.yaml")
}

func TestLoadEtcdOnce_JSON(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	f.Set("svc/a.json", []byte(`{"k":"v"}`))

	got, err := loadEtcdOnce(t.Context(), f, []string{"svc/a.json"})
	require.NoError(t, err)
	assert.Equal(t, "v", got["k"])
}

func TestLoadEtcdOnce_InvalidYaml(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	f.Set("svc/a.yaml", []byte("a: [unterminated"))

	_, err := loadEtcdOnce(t.Context(), f, []string{"svc/a.yaml"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "svc/a.yaml")
}

func TestLoadEtcdOnce_UnknownType(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	f.Set("svc/a.toml", []byte("k=v"))

	_, err := loadEtcdOnce(t.Context(), f, []string{"svc/a.toml"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnknownConfigType))
}

func TestLoadEtcdPerPath_ReturnsPerPathMaps(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	f.Set("svc/a.yaml", []byte("k1: a\n"))
	f.Set("svc/b.yaml", []byte("k2: b\n"))

	got, err := loadEtcdPerPath(t.Context(), f, []string{"svc/a.yaml", "svc/b.yaml"})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"k1": "a"}, got["svc/a.yaml"])
	assert.Equal(t, map[string]any{"k2": "b"}, got["svc/b.yaml"])
}

func TestLoadEtcdPerPath_PathError(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	f.Set("svc/a.yaml", []byte("k: v"))

	_, err := loadEtcdPerPath(t.Context(), f, []string{"svc/a.yaml", "missing.yaml"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEtcdKeyNotFound))
}

// ----- dynamicWatcher.run -----

// snapshotChan wraps a publish callback so tests can synchronously consume
// snapshot updates from the watcher.
func snapshotChan() (func(map[string]any), <-chan map[string]any) {
	ch := make(chan map[string]any, 16)

	return func(snap map[string]any) {
		ch <- snap
	}, ch
}

// fastBackoff is the backoff configuration used by all watcher tests so
// reconnect timing doesn't add real seconds to the suite.
var fastBackoff = backoffConfig{
	initial: 1 * time.Millisecond,
	max:     10 * time.Millisecond,
	factor:  2,
}

// recvSnapshot reads one snapshot with a generous timeout, failing the test
// if none arrives.
func recvSnapshot(t *testing.T, ch <-chan map[string]any) map[string]any {
	t.Helper()

	select {
	case s := <-ch:
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for snapshot")

		return nil
	}
}

func TestDynamicWatcher_PutTriggersSnapshot(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	publish, snaps := snapshotChan()

	pre := map[string]any{"a": "from-pre"}
	initial := map[string]map[string]any{
		"p0.yaml": {"b": "from-initial"},
	}

	w := newDynamicWatcher(f, []string{"p0.yaml"}, pre, initial, publish)
	w.backoff = fastBackoff

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		w.run(ctx)
		close(done)
	}()

	require.True(t, f.WaitForWatcher("p0.yaml", 1, 2*time.Second))

	f.Set("p0.yaml", []byte("b: changed\nc: new\n"))

	snap := recvSnapshot(t, snaps)
	assert.Equal(t, "from-pre", snap["a"])
	assert.Equal(t, "changed", snap["b"])
	assert.Equal(t, "new", snap["c"])

	cancel()
	<-done
}

func TestDynamicWatcher_MultiplePathsIndependent(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	publish, snaps := snapshotChan()

	initial := map[string]map[string]any{
		"p0.yaml": {"x": "p0-init"},
		"p1.yaml": {"y": "p1-init"},
	}

	w := newDynamicWatcher(f, []string{"p0.yaml", "p1.yaml"}, nil, initial, publish)
	w.backoff = fastBackoff

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		w.run(ctx)
		close(done)
	}()

	require.True(t, f.WaitForWatcher("p0.yaml", 1, 2*time.Second))
	require.True(t, f.WaitForWatcher("p1.yaml", 1, 2*time.Second))

	f.Set("p0.yaml", []byte("x: p0-new\n"))
	snap1 := recvSnapshot(t, snaps)
	assert.Equal(t, "p0-new", snap1["x"])
	assert.Equal(t, "p1-init", snap1["y"])

	f.Set("p1.yaml", []byte("y: p1-new\n"))
	snap2 := recvSnapshot(t, snaps)
	assert.Equal(t, "p0-new", snap2["x"])
	assert.Equal(t, "p1-new", snap2["y"])

	cancel()
	<-done
}

func TestDynamicWatcher_StaleKeyFix(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	publish, snaps := snapshotChan()

	initial := map[string]map[string]any{
		"p0.yaml": {"k": "v1", "stale": "old"},
	}

	w := newDynamicWatcher(f, []string{"p0.yaml"}, nil, initial, publish)
	w.backoff = fastBackoff

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		w.run(ctx)
		close(done)
	}()

	require.True(t, f.WaitForWatcher("p0.yaml", 1, 2*time.Second))

	// New yaml drops "stale" entirely.
	f.Set("p0.yaml", []byte("k: v2\n"))

	snap := recvSnapshot(t, snaps)
	assert.Equal(t, "v2", snap["k"])

	_, ok := snap["stale"]
	assert.False(t, ok, "stale key must be absent after PUT without it")

	cancel()
	<-done
}

func TestDynamicWatcher_DeleteIgnored(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	publish, snaps := snapshotChan()

	initial := map[string]map[string]any{
		"p0.yaml": {"k": "v"},
	}

	w := newDynamicWatcher(f, []string{"p0.yaml"}, nil, initial, publish)
	w.backoff = fastBackoff

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		w.run(ctx)
		close(done)
	}()

	require.True(t, f.WaitForWatcher("p0.yaml", 1, 2*time.Second))

	f.Delete("p0.yaml")

	// No publish should arrive on DELETE.
	select {
	case snap := <-snaps:
		t.Fatalf("unexpected publish on DELETE: %v", snap)
	case <-time.After(50 * time.Millisecond):
		// OK — DELETE was ignored.
	}

	cancel()
	<-done
}

func TestDynamicWatcher_TerminalErrorReconnectsAndResyncs(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	publish, snaps := snapshotChan()

	w := newDynamicWatcher(f, []string{"p0.yaml"}, nil, nil, publish)
	w.backoff = fastBackoff

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		w.run(ctx)
		close(done)
	}()

	require.True(t, f.WaitForWatcher("p0.yaml", 1, 2*time.Second))

	// Seed with initial value via PUT once the watcher is subscribed.
	f.Set("p0.yaml", []byte("k: v1\n"))

	// First PUT goes through.
	snap1 := recvSnapshot(t, snaps)
	assert.Equal(t, "v1", snap1["k"])

	// Update stored data silently so resync (Get) sees v2 without an
	// intervening Put delivery (the channel has been killed by SetError).
	f.SetData("p0.yaml", []byte("k: v2\n"))

	// Force the stream to terminate with an error, triggering reconnect.
	f.SetError("p0.yaml", errors.New("boom"))

	// Eventually resync runs Get → parses v2 → publishes.
	snap2 := recvSnapshot(t, snaps)
	assert.Equal(t, "v2", snap2["k"])

	cancel()
	<-done
}

func TestDynamicWatcher_CtxCancelStopsAndClosesKV(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	publish, _ := snapshotChan()

	w := newDynamicWatcher(
		f,
		[]string{"p0.yaml", "p1.yaml", "p2.yaml"},
		nil,
		nil,
		publish,
	)
	w.backoff = fastBackoff

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		w.run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not stop after ctx cancel")
	}

	assert.Equal(t, 1, f.CloseCount())
}

func TestDynamicWatcher_RaceLoad(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()

	var (
		mu       sync.Mutex
		received []map[string]any
	)

	publish := func(snap map[string]any) {
		mu.Lock()
		received = append(received, snap)
		mu.Unlock()
	}

	paths := []string{"p0.yaml", "p1.yaml", "p2.yaml"}

	w := newDynamicWatcher(f, paths, nil, nil, publish)
	w.backoff = fastBackoff

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		w.run(ctx)
		close(done)
	}()

	for _, p := range paths {
		require.True(t, f.WaitForWatcher(p, 1, 2*time.Second))
	}

	const writes = 1000

	var writers sync.WaitGroup

	writers.Go(func() {
		for i := range writes {
			path := paths[i%len(paths)]
			f.Set(path, fmt.Appendf(nil, "k: v%d\n", i))
		}
	})

	const readers = 100

	var readerWG sync.WaitGroup

	stopReaders := make(chan struct{})

	var totalReads atomic.Int64

	for range readers {
		readerWG.Go(func() {
			for {
				select {
				case <-stopReaders:
					return
				default:
				}

				mu.Lock()
				_ = len(received)
				mu.Unlock()

				totalReads.Add(1)
			}
		})
	}

	writers.Wait()
	close(stopReaders)
	readerWG.Wait()

	cancel()
	<-done

	assert.Positive(t, totalReads.Load())

	mu.Lock()
	assert.NotEmpty(t, received)
	mu.Unlock()
}

func TestDynamicWatcher_InvalidYAMLOnPutDoesNotCrash(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	publish, snaps := snapshotChan()

	initial := map[string]map[string]any{
		"p0.yaml": {"k": "init"},
	}

	w := newDynamicWatcher(f, []string{"p0.yaml"}, nil, initial, publish)
	w.backoff = fastBackoff

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		w.run(ctx)
		close(done)
	}()

	require.True(t, f.WaitForWatcher("p0.yaml", 1, 2*time.Second))

	// Bad payload is logged & skipped.
	f.Set("p0.yaml", []byte("a: [bad"))

	// No publish for the bad payload, then a good one publishes.
	select {
	case snap := <-snaps:
		t.Fatalf("unexpected publish on bad yaml: %v", snap)
	case <-time.After(50 * time.Millisecond):
	}

	f.Set("p0.yaml", []byte("k: ok\n"))

	snap := recvSnapshot(t, snaps)
	assert.Equal(t, "ok", snap["k"])

	cancel()
	<-done
}

func TestDynamicWatcher_AdvanceBackoff(t *testing.T) {
	t.Parallel()

	w := &dynamicWatcher{ //nolint:exhaustruct // testing helper directly
		backoff: backoffConfig{initial: 1 * time.Millisecond, max: 8 * time.Millisecond, factor: 2},
	}

	// Doubles up to max.
	assert.Equal(t, 2*time.Millisecond, w.advanceBackoff(1*time.Millisecond))
	assert.Equal(t, 4*time.Millisecond, w.advanceBackoff(2*time.Millisecond))
	assert.Equal(t, 8*time.Millisecond, w.advanceBackoff(4*time.Millisecond))
	// Caps at max.
	assert.Equal(t, 8*time.Millisecond, w.advanceBackoff(8*time.Millisecond))
}

func TestDefaultBackoffConfig(t *testing.T) {
	t.Parallel()

	bc := defaultBackoffConfig()
	assert.Equal(t, initialWatchBackoff, bc.initial)
	assert.Equal(t, maxWatchBackoff, bc.max)
	assert.Equal(t, backoffGrowthFactor, bc.factor)
}

func TestDynamicWatcher_ResyncErrorBacksOffAndRetries(t *testing.T) {
	t.Parallel()

	f := newFakeEtcdKV()
	publish, snaps := snapshotChan()

	w := newDynamicWatcher(f, []string{"p0.yaml"}, nil, nil, publish)
	w.backoff = fastBackoff

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		w.run(ctx)
		close(done)
	}()

	require.True(t, f.WaitForWatcher("p0.yaml", 1, 2*time.Second))

	// SetError on a non-existing key causes streamPath to return; resync via
	// Get will fail (key not found); watcher backs off and retries.
	f.SetError("p0.yaml", errors.New("boom"))

	// Wait until the watcher resubscribes (after one or more failed resyncs).
	require.True(t, f.WaitForWatcher("p0.yaml", 1, 2*time.Second))

	f.Set("p0.yaml", []byte("k: ok\n"))

	deadline := time.After(2 * time.Second)
	for {
		select {
		case snap := <-snaps:
			if snap["k"] == "ok" {
				cancel()
				<-done

				return
			}
		case <-deadline:
			t.Fatal("never received recovery snapshot")
		}
	}
}

// Ensure parseConfigBytes consumed by snapshot path is reachable through
// composeSnapshot when state[p] is missing for a path (defensive case for
// future code paths where seed is incomplete).
func TestDynamicWatcher_ComposeSnapshotSkipsMissingState(t *testing.T) {
	t.Parallel()

	w := newDynamicWatcher(
		newFakeEtcdKV(),
		[]string{"p0", "p1"},
		map[string]any{"a": 1},
		map[string]map[string]any{"p1": {"b": 2}},
		func(map[string]any) {},
	)

	w.mu.Lock()
	snap := w.composeSnapshotLocked()
	w.mu.Unlock()

	assert.Equal(t, 1, snap["a"])
	assert.Equal(t, 2, snap["b"])
}

// Verify the package-level errBoom-style sentinel is checked through wrapping.
func TestParseConfigBytes_UnknownTypeContainsString(t *testing.T) {
	t.Parallel()

	_, err := parseConfigBytes(nil, "ini")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "ini"))
}
