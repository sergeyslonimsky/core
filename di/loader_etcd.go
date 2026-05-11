package di

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// ErrUnknownConfigType is returned by parseConfigBytes when the configType
// argument is not one of the supported formats (yaml, yml, json).
var ErrUnknownConfigType = errors.New("unknown config type")

// parseConfigBytes parses a single etcd payload as the given configType
// ("yaml"/"yml"/"json") and returns a flat dotted-key map (via the same
// flatten used by the file loader). Empty payload yields an empty map.
// Unknown configType wraps ErrUnknownConfigType with the offending string.
func parseConfigBytes(data []byte, configType string) (map[string]any, error) {
	switch configType {
	case "yaml", "yml":
		if len(data) == 0 {
			return map[string]any{}, nil
		}

		var parsed map[string]any

		dec := yaml.NewDecoder(bytes.NewReader(data))
		if err := dec.Decode(&parsed); err != nil {
			if errors.Is(err, io.EOF) {
				return map[string]any{}, nil
			}

			return nil, fmt.Errorf("decode yaml: %w", err)
		}

		if parsed == nil {
			return map[string]any{}, nil
		}

		return flatten(parsed), nil
	case "json":
		if len(data) == 0 {
			return map[string]any{}, nil
		}

		var parsed map[string]any
		if err := json.Unmarshal(data, &parsed); err != nil {
			return nil, fmt.Errorf("decode json: %w", err)
		}

		if parsed == nil {
			return map[string]any{}, nil
		}

		return flatten(parsed), nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownConfigType, configType)
	}
}

// loadEtcdOnce reads each path via kv.Get, parses it according to the path
// extension, and returns the deep-merged flat map. Used for the static load
// path. On any error returns immediately, wrapping the failing path in the
// error message. Empty paths yields an empty map and no error.
func loadEtcdOnce(ctx context.Context, kv etcdKV, paths []string) (map[string]any, error) {
	acc := map[string]any{}

	for _, p := range paths {
		parsed, err := loadEtcdSingle(ctx, kv, p)
		if err != nil {
			return nil, err
		}

		acc = merge(acc, parsed)
	}

	return acc, nil
}

// loadEtcdPerPath is like loadEtcdOnce but returns the per-path parsed
// maps (not merged). Used to seed the dynamicWatcher's pathState.
func loadEtcdPerPath(ctx context.Context, kv etcdKV, paths []string) (map[string]map[string]any, error) {
	out := make(map[string]map[string]any, len(paths))

	for _, p := range paths {
		parsed, err := loadEtcdSingle(ctx, kv, p)
		if err != nil {
			return nil, err
		}

		out[p] = parsed
	}

	return out, nil
}

// loadEtcdSingle reads a single path and parses it.
func loadEtcdSingle(ctx context.Context, kv etcdKV, path string) (map[string]any, error) {
	data, err := kv.Get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("load etcd %s: %w", path, err)
	}

	parsed, err := parseConfigBytes(data, getConfigTypeFromPath(path))
	if err != nil {
		return nil, fmt.Errorf("parse etcd %s: %w", path, err)
	}

	return parsed, nil
}

// backoffConfig parameterises the dynamic watcher's reconnect backoff so
// tests can shrink delays without time.Sleep'ing real seconds.
type backoffConfig struct {
	initial time.Duration
	max     time.Duration
	factor  int
}

// defaultBackoffConfig mirrors the master watcher's backoff (initialWatchBackoff,
// maxWatchBackoff, backoffGrowthFactor).
func defaultBackoffConfig() backoffConfig {
	return backoffConfig{
		initial: initialWatchBackoff,
		max:     maxWatchBackoff,
		factor:  backoffGrowthFactor,
	}
}

// dynamicWatcher keeps per-path "last parsed map" state and rebuilds the
// full merged snapshot on every PUT update. Per-path state is the fix for
// the master-era stale-key bug: previously, when an operator removed a key
// from a yaml in etcd, the next PUT would still leave the stale key in the
// snapshot (mergeBytesIntoViper merged keys but never reset).
//
// DELETE events are intentionally ignored, preserving the viper/remote
// contract: state[path] keeps its last known value on DELETE, so DELETEs do
// not trigger a publish.
type dynamicWatcher struct {
	kv      etcdKV
	paths   []string
	pre     map[string]any
	state   map[string]map[string]any
	publish func(map[string]any)

	mu      sync.Mutex
	backoff backoffConfig
}

// newDynamicWatcher initialises a watcher. `initial` is consumed (callers
// must not mutate it after handing off). `pre` is expected to already
// contain defaults+file+static merged together.
//
// publish is invoked for every state change after run starts; the caller is
// responsible for publishing the initial snapshot separately before run.
func newDynamicWatcher(
	kv etcdKV,
	paths []string,
	pre map[string]any,
	initial map[string]map[string]any,
	publish func(map[string]any),
) *dynamicWatcher {
	if initial == nil {
		initial = map[string]map[string]any{}
	}

	return &dynamicWatcher{
		kv:      kv,
		paths:   append([]string(nil), paths...),
		pre:     pre,
		state:   initial,
		publish: publish,
		backoff: defaultBackoffConfig(),
	}
}

// run starts one goroutine per path, blocks until ctx is cancelled and all
// goroutines exit, then closes kv exactly once. Caller should invoke as
// `go w.run(ctx)`.
//
// publish is called after run starts for every PUT (and after a successful
// resync). DELETE events are ignored. The caller is responsible for
// publishing the initial state before invoking run.
func (w *dynamicWatcher) run(ctx context.Context) {
	defer func() {
		if err := w.kv.Close(); err != nil {
			log.Printf("close etcd client: %v", err)
		}
	}()

	var wg sync.WaitGroup

	for _, p := range w.paths {
		wg.Go(func() {
			w.watchSingle(ctx, p)
		})
	}

	wg.Wait()
}

// watchSingle runs the per-path stream/reconnect loop. Mirrors master's
// watchSingleKey but talks through the etcdKV interface and updates
// per-path state.
func (w *dynamicWatcher) watchSingle(ctx context.Context, path string) {
	current := w.backoff.initial

	for ctx.Err() == nil {
		w.streamPath(ctx, path)

		if ctx.Err() != nil {
			return
		}

		// Jitter the delay so a fleet of services doesn't synchronise its
		// reconnect storm when etcd recovers.
		delay := current + rand.N(current/2+1) //nolint:gosec // not security-sensitive

		log.Printf("etcd dynamic watch for %s ended; reconnecting in %v", path, delay)

		if !sleepWithCtx(ctx, delay) {
			return
		}

		if err := w.resync(ctx, path); err != nil {
			log.Printf("etcd dynamic resync for %s failed: %v", path, err)

			current = w.advanceBackoff(current)

			continue
		}

		current = w.backoff.initial
	}
}

// streamPath consumes events from kv.Watch until the channel closes or a
// terminal error is delivered. PUT events update state and publish.
// DELETE events are intentionally ignored. Err events are logged and
// terminate the loop so the caller can reconnect with backoff.
func (w *dynamicWatcher) streamPath(ctx context.Context, path string) {
	ch := w.kv.Watch(ctx, path)

	for ev := range ch {
		if ev.Err != nil {
			log.Printf("etcd dynamic watch for %s: %v", path, ev.Err)

			return
		}

		switch ev.Type {
		case eventPut:
			parsed, err := parseConfigBytes(ev.Value, getConfigTypeFromPath(path))
			if err != nil {
				log.Printf("etcd dynamic parse %s: %v", path, err)

				continue
			}

			w.applyUpdate(path, parsed)
		case eventDelete:
			// Preserve master/viper-remote contract: DELETE leaves state
			// untouched and triggers no publish.
			continue
		}
	}
}

// resync re-reads a path via Get and applies the result. Used after a
// stream loop exits, to catch up on events potentially missed during
// disconnect/compaction.
func (w *dynamicWatcher) resync(ctx context.Context, path string) error {
	data, err := w.kv.Get(ctx, path)
	if err != nil {
		return fmt.Errorf("resync %s: %w", path, err)
	}

	parsed, err := parseConfigBytes(data, getConfigTypeFromPath(path))
	if err != nil {
		return fmt.Errorf("resync parse %s: %w", path, err)
	}

	w.applyUpdate(path, parsed)

	return nil
}

// applyUpdate stores the latest parsed map for path, builds a fresh merged
// snapshot under the lock, then releases the lock and publishes. The
// snapshot is built local to the goroutine so publish never runs under mu.
func (w *dynamicWatcher) applyUpdate(path string, parsed map[string]any) {
	w.mu.Lock()
	w.state[path] = parsed
	snap := w.composeSnapshotLocked()
	w.mu.Unlock()

	w.publish(snap)
}

// composeSnapshotLocked rebuilds the full snapshot from pre + per-path
// state. Iteration is in path order so later paths win deterministically.
// Caller must hold w.mu.
func (w *dynamicWatcher) composeSnapshotLocked() map[string]any {
	snap := cloneMap(w.pre)

	for _, p := range w.paths {
		state, ok := w.state[p]
		if !ok {
			continue
		}

		snap = merge(snap, state)
	}

	return snap
}

// advanceBackoff doubles current up to backoff.max.
func (w *dynamicWatcher) advanceBackoff(current time.Duration) time.Duration {
	next := current * time.Duration(w.backoff.factor)
	if next > w.backoff.max {
		return w.backoff.max
	}

	return next
}
