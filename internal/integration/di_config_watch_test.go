package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/di"
	"github.com/sergeyslonimsky/core/internal/integration/testhelpers"
)

const (
	watchPropagationTimeout  = 5 * time.Second
	watchPropagationInterval = 50 * time.Millisecond
	// Reconnect can take initialWatchBackoff (1s) plus up to half of that as
	// jitter, plus the resync round-trip. Give generous headroom so CI
	// variance doesn't flake.
	reconnectPropagationTimeout = 15 * time.Second
)

// TestNewConfig_EtcdDynamic_NativeWatchIsFast asserts that updates land in
// Config within a second or two, since native clientv3.Watch is event-driven
// (versus the old 5s poll interval under viper/remote).
func TestNewConfig_EtcdDynamic_NativeWatchIsFast(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration test in short mode")
	}

	endpoint, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	testhelpers.PutEtcdValue(t, endpoint, "test/fast-service/dynamic.yaml", map[string]any{
		"limits": map[string]any{"rateLimit": 100},
	})

	t.Setenv("APP_CONFIG_ETCD_ENDPOINT", endpoint)
	t.Setenv("APP_CONFIG_ETCD_DYNAMIC_PATHS", "dynamic.yaml")
	t.Setenv("APP_ENV", "test")
	t.Setenv("APP_SERVICE_NAME", "fast-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)
	require.Equal(t, 100, cfg.GetInt("limits.rateLimit"))

	testhelpers.PutEtcdValue(t, endpoint, "test/fast-service/dynamic.yaml", map[string]any{
		"limits": map[string]any{"rateLimit": 200},
	})

	require.Eventually(t, func() bool {
		return cfg.GetInt("limits.rateLimit") == 200
	}, watchPropagationTimeout, watchPropagationInterval,
		"expected rateLimit=200 within %s (native watch should propagate within ~seconds)",
		watchPropagationTimeout)
}

// TestNewConfig_EtcdDynamic_MultiplePathsAllWatched verifies that updates to
// any dynamic path propagate. The viper/remote-based implementation only
// watched the first registered provider — a subtle bug this rewrite fixes.
func TestNewConfig_EtcdDynamic_MultiplePathsAllWatched(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration test in short mode")
	}

	endpoint, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	basePath := "test/multi-service/base.yaml"
	extraPath := "test/multi-service/extra.yaml"

	testhelpers.PutEtcdValue(t, endpoint, basePath, map[string]any{
		"base": map[string]any{"value": "base-initial"},
	})
	testhelpers.PutEtcdValue(t, endpoint, extraPath, map[string]any{
		"extra": map[string]any{"value": "extra-initial"},
	})

	t.Setenv("APP_CONFIG_ETCD_ENDPOINT", endpoint)
	t.Setenv("APP_CONFIG_ETCD_DYNAMIC_PATHS", "base.yaml,extra.yaml")
	t.Setenv("APP_ENV", "test")
	t.Setenv("APP_SERVICE_NAME", "multi-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)
	require.Equal(t, "base-initial", cfg.GetString("base.value"))
	require.Equal(t, "extra-initial", cfg.GetString("extra.value"))

	// Update the SECOND path — under the old viper/remote implementation
	// this would never propagate, because only the first provider was polled.
	testhelpers.PutEtcdValue(t, endpoint, extraPath, map[string]any{
		"extra": map[string]any{"value": "extra-updated"},
	})

	require.Eventually(t, func() bool {
		return cfg.GetString("extra.value") == "extra-updated"
	}, watchPropagationTimeout, watchPropagationInterval,
		"second dynamic path must be watched, not just the first")

	// First path still works too.
	testhelpers.PutEtcdValue(t, endpoint, basePath, map[string]any{
		"base": map[string]any{"value": "base-updated"},
	})

	require.Eventually(t, func() bool {
		return cfg.GetString("base.value") == "base-updated"
	}, watchPropagationTimeout, watchPropagationInterval)
}

// TestNewConfig_EtcdDynamic_DeleteIgnored verifies that DELETE events on a
// watched key do not wipe the last-known value — matches the documented
// contract inherited from the viper/remote era.
func TestNewConfig_EtcdDynamic_DeleteIgnored(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration test in short mode")
	}

	endpoint, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	key := "test/delete-service/dynamic.yaml"
	testhelpers.PutEtcdValue(t, endpoint, key, map[string]any{
		"feature": map[string]any{"enabled": true},
	})

	t.Setenv("APP_CONFIG_ETCD_ENDPOINT", endpoint)
	t.Setenv("APP_CONFIG_ETCD_DYNAMIC_PATHS", "dynamic.yaml")
	t.Setenv("APP_ENV", "test")
	t.Setenv("APP_SERVICE_NAME", "delete-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)
	require.True(t, cfg.GetBool("feature.enabled"))

	// Delete the key — the watcher sees a DELETE event but must not touch
	// Config state.
	testhelpers.DeleteEtcdKey(t, endpoint, key)

	// Give the watcher a moment to process the DELETE so we know it was
	// consciously ignored (not simply racing ahead of us).
	time.Sleep(500 * time.Millisecond)

	assert.True(t, cfg.GetBool("feature.enabled"),
		"DELETE must not wipe the last-known value")
}

// TestNewConfig_EtcdDynamic_ReconnectAfterCompaction forces a compaction on
// the watched key, which makes clientv3.Watch close the channel with
// rpctypes.ErrCompacted. The watcher must backoff, reconnect, re-sync via
// Get, and continue observing subsequent PUTs.
func TestNewConfig_EtcdDynamic_ReconnectAfterCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration test in short mode")
	}

	endpoint, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	key := "test/reconnect-service/dynamic.yaml"
	testhelpers.PutEtcdValue(t, endpoint, key, map[string]any{
		"limits": map[string]any{"rateLimit": 10},
	})

	t.Setenv("APP_CONFIG_ETCD_ENDPOINT", endpoint)
	t.Setenv("APP_CONFIG_ETCD_DYNAMIC_PATHS", "dynamic.yaml")
	t.Setenv("APP_ENV", "test")
	t.Setenv("APP_SERVICE_NAME", "reconnect-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)
	require.Equal(t, 10, cfg.GetInt("limits.rateLimit"))

	// Normal update through watch — sanity check the happy path.
	testhelpers.PutEtcdValue(t, endpoint, key, map[string]any{
		"limits": map[string]any{"rateLimit": 20},
	})
	require.Eventually(t, func() bool {
		return cfg.GetInt("limits.rateLimit") == 20
	}, watchPropagationTimeout, watchPropagationInterval)

	// Force compaction — this cancels the active watch with ErrCompacted.
	// Our streamKeyEvents returns the error, watchSingleKey sleeps backoff,
	// resyncs via Get (which picks up the next PUT), and reopens the watch.
	testhelpers.CompactEtcdAtLatest(t, endpoint)

	// Now PUT a new value. Depending on timing, this may be caught either
	// by the resyncKey Get (if we PUT before the reconnect) or by the new
	// watch (if we PUT after). Both paths must end up with the new value.
	testhelpers.PutEtcdValue(t, endpoint, key, map[string]any{
		"limits": map[string]any{"rateLimit": 99},
	})

	require.Eventually(t, func() bool {
		return cfg.GetInt("limits.rateLimit") == 99
	}, reconnectPropagationTimeout, watchPropagationInterval,
		"watcher must reconnect after compaction and reflect the post-compact PUT")
}
