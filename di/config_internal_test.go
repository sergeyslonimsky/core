package di

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseCommaSeparatedPaths tests parsing comma-separated path strings into a slice.
func TestParseCommaSeparatedPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "single path",
			input:    "/path/to/config",
			expected: []string{"/path/to/config"},
		},
		{
			name:     "multiple paths",
			input:    "/path/one,/path/two,/path/three",
			expected: []string{"/path/one", "/path/two", "/path/three"},
		},
		{
			name:     "paths with spaces",
			input:    " /path/one , /path/two , /path/three ",
			expected: []string{"/path/one", "/path/two", "/path/three"},
		},
		{
			name:     "paths with extra commas",
			input:    "/path/one,,/path/two,",
			expected: []string{"/path/one", "/path/two"},
		},
		{
			name:     "only commas",
			input:    ",,,",
			expected: []string{},
		},
		{
			name:     "mixed valid and empty",
			input:    "valid1, , valid2, ,valid3",
			expected: []string{"valid1", "valid2", "valid3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := parseCommaSeparatedPaths(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestBuildEtcdPath tests building etcd key paths from environment, service, and config path.
func TestBuildEtcdPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		env         string
		serviceName string
		configPath  string
		expected    string
	}{
		{
			name:        "basic path",
			env:         "prod",
			serviceName: "myservice",
			configPath:  "config.yaml",
			expected:    "prod/myservice/config.yaml",
		},
		{
			name:        "dev environment",
			env:         "dev",
			serviceName: "test-service",
			configPath:  "base.yaml",
			expected:    "dev/test-service/base.yaml",
		},
		{
			name:        "nested config path",
			env:         "staging",
			serviceName: "api",
			configPath:  "infra/postgres.yaml",
			expected:    "staging/api/infra/postgres.yaml",
		},
		{
			name:        "empty values",
			env:         "",
			serviceName: "",
			configPath:  "config.yaml",
			expected:    "//config.yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := buildEtcdPath(tt.env, tt.serviceName, tt.configPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestGetConfigTypeFromPath tests determining configuration type from file path extension.
func TestGetConfigTypeFromPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "yaml extension",
			path:     "config.yaml",
			expected: "yaml",
		},
		{
			name:     "yml extension",
			path:     "config.yml",
			expected: "yml",
		},
		{
			name:     "json extension",
			path:     "config.json",
			expected: "json",
		},
		{
			name:     "toml extension",
			path:     "config.toml",
			expected: "toml",
		},
		{
			name:     "no extension",
			path:     "config",
			expected: "yaml",
		},
		{
			name:     "path with directory",
			path:     "/path/to/config.yaml",
			expected: "yaml",
		},
		{
			name:     "multiple dots",
			path:     "my.config.yaml",
			expected: "yaml",
		},
		{
			name:     "empty path",
			path:     "",
			expected: "yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := getConfigTypeFromPath(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestMergeBytesIntoViper verifies that parsed etcd payloads land in the
// target viper via Set. Also covers malformed payloads and overwrite
// semantics (later merges win over earlier values for the same key).
func TestMergeBytesIntoViper(t *testing.T) {
	t.Parallel()

	t.Run("yaml merges nested keys", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		data := []byte("database:\n  host: db.example.com\n  port: 5432\nfeature:\n  enabled: true\n")

		err := mergeBytesIntoViper(v, data, "yaml")
		require.NoError(t, err)

		assert.Equal(t, "db.example.com", v.GetString("database.host"))
		assert.Equal(t, 5432, v.GetInt("database.port"))
		assert.True(t, v.GetBool("feature.enabled"))
	})

	t.Run("json merges flat keys", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		data := []byte(`{"timeout": "30s", "retries": 3}`)

		err := mergeBytesIntoViper(v, data, "json")
		require.NoError(t, err)

		assert.Equal(t, "30s", v.GetString("timeout"))
		assert.Equal(t, 3, v.GetInt("retries"))
	})

	t.Run("later merge overwrites earlier value", func(t *testing.T) {
		t.Parallel()

		v := viper.New()

		first := []byte("server:\n  port: 8080\n")
		require.NoError(t, mergeBytesIntoViper(v, first, "yaml"))
		assert.Equal(t, 8080, v.GetInt("server.port"))

		second := []byte("server:\n  port: 9090\n")
		require.NoError(t, mergeBytesIntoViper(v, second, "yaml"))
		assert.Equal(t, 9090, v.GetInt("server.port"))
	})

	t.Run("malformed yaml returns error", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		data := []byte("this: is: not: valid: yaml:\n  : :")

		err := mergeBytesIntoViper(v, data, "yaml")
		assert.Error(t, err)
	})

	t.Run("empty payload is a no-op", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		err := mergeBytesIntoViper(v, []byte(""), "yaml")
		require.NoError(t, err)
		assert.Empty(t, v.AllKeys())
	})
}

// TestResolveDynamicConfigPaths covers the precedence rules for resolving
// which etcd paths to watch: the new comma-separated key wins over the
// legacy single-path key, and an empty multi key after parsing is an error.
func TestResolveDynamicConfigPaths(t *testing.T) {
	t.Parallel()

	t.Run("multi-paths key wins over legacy single", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		v.Set("app.config.etcd.dynamic.paths", "a.yaml,b.yaml")
		v.Set("app.config.etcd.path", "legacy.yaml")

		paths, err := resolveDynamicConfigPaths(v)
		require.NoError(t, err)
		assert.Equal(t, []string{"a.yaml", "b.yaml"}, paths)
	})

	t.Run("falls back to legacy single path", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		v.Set("app.config.etcd.path", "legacy.yaml")

		paths, err := resolveDynamicConfigPaths(v)
		require.NoError(t, err)
		assert.Equal(t, []string{"legacy.yaml"}, paths)
	})

	t.Run("neither key set returns nil without error", func(t *testing.T) {
		t.Parallel()

		v := viper.New()

		paths, err := resolveDynamicConfigPaths(v)
		require.NoError(t, err)
		assert.Nil(t, paths)
	})

	t.Run("multi key with only commas errors", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		v.Set("app.config.etcd.dynamic.paths", ",,,")

		_, err := resolveDynamicConfigPaths(v)
		assert.ErrorIs(t, err, ErrEmptyEtcdDynamicPaths)
	})
}

// TestApplyEtcdUpdate verifies that watch-triggered updates land in cfg
// under the write lock and that subsequent reads via Config getters see
// the new value.
func TestApplyEtcdUpdate(t *testing.T) {
	t.Parallel()

	cfg := &Config{ //nolint:exhaustruct // mu has a valid zero value
		configStorage: viper.New(),
	}

	applyEtcdUpdate(cfg, "prod/svc/dynamic.yaml", []byte("feature:\n  enabled: true\n"))
	assert.True(t, cfg.GetBool("feature.enabled"))

	applyEtcdUpdate(cfg, "prod/svc/dynamic.yaml", []byte("feature:\n  enabled: false\n"))
	assert.False(t, cfg.GetBool("feature.enabled"))
}

// TestApplyEtcdUpdateMalformedPayload asserts that applyEtcdUpdate swallows
// parse errors (logged) rather than panicking or corrupting cfg state — a
// single bad payload must not take down the watcher goroutine.
func TestApplyEtcdUpdateMalformedPayload(t *testing.T) {
	t.Parallel()

	cfg := &Config{ //nolint:exhaustruct // mu has a valid zero value
		configStorage: viper.New(),
	}
	cfg.configStorage.Set("feature.enabled", true)

	assert.NotPanics(t, func() {
		applyEtcdUpdate(cfg, "prod/svc/dynamic.yaml", []byte("this: is: not: valid: yaml:\n  : :"))
	})

	// Previous state is preserved.
	assert.True(t, cfg.GetBool("feature.enabled"))
}

// TestConfigConcurrentReadersAndWriters exercises the real production
// pattern: one watcher goroutine merging etcd payloads via applyEtcdUpdate
// while many reader goroutines hit the typed getters. The test must pass
// under `-race` — if cfg.mu is ever bypassed, viper's internal maps race
// and the detector fires.
func TestConfigConcurrentReadersAndWriters(t *testing.T) {
	t.Parallel()

	cfg := &Config{ //nolint:exhaustruct // mu has a valid zero value
		configStorage: viper.New(),
	}

	cfg.configStorage.Set("feature.enabled", true)
	cfg.configStorage.Set("timeout", "1s")

	const (
		readers    = 8
		iterations = 200
	)

	var (
		done    = make(chan struct{})
		wg      sync.WaitGroup
		payload = [][]byte{
			[]byte("feature:\n  enabled: true\ntimeout: 1s\n"),
			[]byte("feature:\n  enabled: false\ntimeout: 2s\n"),
		}
	)

	// Writer goroutine simulates the watcher merging updates.
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < iterations; i++ {
			applyEtcdUpdate(cfg, "prod/svc/dynamic.yaml", payload[i%len(payload)])
		}

		close(done)
	}()

	// Reader goroutines hit every typed getter concurrently with the writer.
	for r := 0; r < readers; r++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
					_ = cfg.GetBool("feature.enabled")
					_ = cfg.GetDuration("timeout")
					_ = cfg.GetString("feature.enabled")
				}
			}
		}()
	}

	wg.Wait()
}

// TestNextBackoff verifies the exponential growth with cap at maxWatchBackoff.
func TestNextBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		current time.Duration
		want    time.Duration
	}{
		{"doubles from initial", initialWatchBackoff, 2 * time.Second},
		{"doubles mid-range", 4 * time.Second, 8 * time.Second},
		{"caps at max when would exceed", 20 * time.Second, maxWatchBackoff},
		{"stays at max", maxWatchBackoff, maxWatchBackoff},
		{"exact half of max doubles to max", maxWatchBackoff / 2, maxWatchBackoff},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := nextBackoff(tt.current)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestSleepWithCtx verifies both exit paths: natural timer expiry and
// context cancellation mid-sleep.
func TestSleepWithCtx(t *testing.T) {
	t.Parallel()

	t.Run("returns true after full sleep", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		start := time.Now()

		ok := sleepWithCtx(ctx, 20*time.Millisecond)

		assert.True(t, ok)
		assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	})

	t.Run("returns false when ctx cancelled during sleep", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		// Cancel before the timer would fire.
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		start := time.Now()
		ok := sleepWithCtx(ctx, time.Second)

		assert.False(t, ok)
		assert.Less(t, time.Since(start), 500*time.Millisecond)
	})

	t.Run("returns false immediately when ctx already cancelled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		ok := sleepWithCtx(ctx, time.Second)

		assert.False(t, ok)
		assert.Less(t, time.Since(start), 50*time.Millisecond)
	})
}
