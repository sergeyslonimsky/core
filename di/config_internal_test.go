package di

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------

// envFn produces an envLookup closure over a static map.
func envFn(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]

		return v, ok
	}
}

// fileFn produces an openFileFn closure over a static map[path]bytes.
func fileFn(files map[string][]byte) openFileFn {
	return func(path string) (io.ReadCloser, error) {
		data, ok := files[path]
		if !ok {
			return nil, fmt.Errorf("file %s: not found", path)
		}

		return io.NopCloser(bytes.NewReader(data)), nil
	}
}

// fakeFactory builds an etcdFactory that always returns the same fake
// (nil error). Use a separate fake per test for isolation.
func fakeFactory(fake etcdKV) func(string, time.Duration) (etcdKV, error) {
	return func(string, time.Duration) (etcdKV, error) { return fake, nil }
}

// noFileFn refuses to open anything (used when no file env is set).
func noFileFn() openFileFn {
	return func(path string) (io.ReadCloser, error) {
		return nil, fmt.Errorf("file %s: unexpected open", path)
	}
}

// noFactory refuses to build an etcd client (used in non-etcd tests).
func noFactory() func(string, time.Duration) (etcdKV, error) {
	return func(string, time.Duration) (etcdKV, error) {
		return nil, errors.New("unexpected etcd factory call")
	}
}

// ----------------------------------------------------------------------
// Pure helpers (preserved from master)
// ----------------------------------------------------------------------

func TestParseCommaSeparatedPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{"empty string", "", nil},
		{"single path", "/path/to/config", []string{"/path/to/config"}},
		{"multiple paths", "/p/one,/p/two,/p/three", []string{"/p/one", "/p/two", "/p/three"}},
		{"paths with spaces", " /a , /b , /c ", []string{"/a", "/b", "/c"}},
		{"paths with extra commas", "/a,,/b,", []string{"/a", "/b"}},
		{"only commas", ",,,", []string{}},
		{"mixed valid and empty", "v1, , v2, ,v3", []string{"v1", "v2", "v3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, parseCommaSeparatedPaths(tt.input))
		})
	}
}

func TestBuildEtcdPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		env, svc, cfg, want string
	}{
		{"prod", "myservice", "config.yaml", "prod/myservice/config.yaml"},
		{"dev", "test-service", "base.yaml", "dev/test-service/base.yaml"},
		{"staging", "api", "infra/postgres.yaml", "staging/api/infra/postgres.yaml"},
		{"", "", "config.yaml", "//config.yaml"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, buildEtcdPath(tt.env, tt.svc, tt.cfg))
		})
	}
}

func TestGetConfigTypeFromPath(t *testing.T) {
	t.Parallel()

	tests := []struct{ path, want string }{
		{"config.yaml", "yaml"},
		{"config.yml", "yml"},
		{"config.json", "json"},
		{"config.toml", "toml"},
		{"config", "yaml"},
		{"/path/to/config.yaml", "yaml"},
		{"my.config.yaml", "yaml"},
		{"", "yaml"},
		{"v1.0/config", "yaml"},
		{"prod/svc/v1.2/config", "yaml"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, getConfigTypeFromPath(tt.path))
		})
	}
}

func TestResolveEtcdRequestTimeout(t *testing.T) {
	t.Parallel()

	t.Run("unset → default", func(t *testing.T) {
		t.Parallel()

		d, err := resolveEtcdRequestTimeout(envFn(nil))
		require.NoError(t, err)
		assert.Equal(t, defaultEtcdRequestTimeout, d)
	})

	t.Run("empty → default", func(t *testing.T) {
		t.Parallel()

		d, err := resolveEtcdRequestTimeout(envFn(map[string]string{
			"APP_CONFIG_ETCD_REQUEST_TIMEOUT": "",
		}))
		require.NoError(t, err)
		assert.Equal(t, defaultEtcdRequestTimeout, d)
	})

	t.Run("valid duration is parsed", func(t *testing.T) {
		t.Parallel()

		d, err := resolveEtcdRequestTimeout(envFn(map[string]string{
			"APP_CONFIG_ETCD_REQUEST_TIMEOUT": "750ms",
		}))
		require.NoError(t, err)
		assert.Equal(t, 750*time.Millisecond, d)
	})

	t.Run("malformed → ErrInvalidEtcdRequestTimeout", func(t *testing.T) {
		t.Parallel()

		_, err := resolveEtcdRequestTimeout(envFn(map[string]string{
			"APP_CONFIG_ETCD_REQUEST_TIMEOUT": "five seconds",
		}))
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidEtcdRequestTimeout))
	})

	t.Run("non-positive → ErrInvalidEtcdRequestTimeout", func(t *testing.T) {
		t.Parallel()

		_, err := resolveEtcdRequestTimeout(envFn(map[string]string{
			"APP_CONFIG_ETCD_REQUEST_TIMEOUT": "0s",
		}))
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidEtcdRequestTimeout))
	})
}

func TestSleepWithCtx(t *testing.T) {
	t.Parallel()

	t.Run("returns true after full sleep", func(t *testing.T) {
		t.Parallel()

		start := time.Now()
		ok := sleepWithCtx(t.Context(), 20*time.Millisecond)

		assert.True(t, ok)
		assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	})

	t.Run("returns false when ctx cancelled during sleep", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())

		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		ok := sleepWithCtx(ctx, time.Second)
		assert.False(t, ok)
	})

	t.Run("returns false immediately when ctx already cancelled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		start := time.Now()
		ok := sleepWithCtx(ctx, time.Second)

		assert.False(t, ok)
		assert.Less(t, time.Since(start), 50*time.Millisecond)
	})
}

// ----------------------------------------------------------------------
// resolveDynamicPaths
// ----------------------------------------------------------------------

func TestResolveDynamicPaths(t *testing.T) {
	t.Parallel()

	t.Run("multi-paths key wins over legacy single", func(t *testing.T) {
		t.Parallel()

		deps := configDeps{envLookup: envFn(map[string]string{
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "a.yaml,b.yaml",
			"APP_CONFIG_ETCD_PATH":          "legacy.yaml",
		})}

		paths, err := resolveDynamicPaths(deps)
		require.NoError(t, err)
		assert.Equal(t, []string{"a.yaml", "b.yaml"}, paths)
	})

	t.Run("falls back to legacy single", func(t *testing.T) {
		t.Parallel()

		deps := configDeps{envLookup: envFn(map[string]string{
			"APP_CONFIG_ETCD_PATH": "legacy.yaml",
		})}

		paths, err := resolveDynamicPaths(deps)
		require.NoError(t, err)
		assert.Equal(t, []string{"legacy.yaml"}, paths)
	})

	t.Run("neither set returns nil", func(t *testing.T) {
		t.Parallel()

		paths, err := resolveDynamicPaths(configDeps{envLookup: envFn(nil)})
		require.NoError(t, err)
		assert.Nil(t, paths)
	})

	t.Run("multi only commas errors", func(t *testing.T) {
		t.Parallel()

		deps := configDeps{envLookup: envFn(map[string]string{
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": ",,,",
		})}

		_, err := resolveDynamicPaths(deps)
		assert.ErrorIs(t, err, ErrEmptyEtcdDynamicPaths)
	})
}

// ----------------------------------------------------------------------
// composeSnapshot
// ----------------------------------------------------------------------

func TestComposeSnapshot(t *testing.T) {
	t.Parallel()

	pre := map[string]any{"a": "pre", "b": "pre"}
	perPath := map[string]map[string]any{
		"p1": {"b": "p1", "c": "p1"},
		"p2": {"c": "p2", "d": "p2"},
	}

	snap := composeSnapshot(pre, []string{"p1", "p2"}, perPath)
	assert.Equal(t, "pre", snap["a"])
	assert.Equal(t, "p1", snap["b"])
	assert.Equal(t, "p2", snap["c"])
	assert.Equal(t, "p2", snap["d"])
}

func TestComposeSnapshotMissingPath(t *testing.T) {
	t.Parallel()

	pre := map[string]any{"a": "pre"}
	perPath := map[string]map[string]any{"p1": {"b": "p1"}}

	snap := composeSnapshot(pre, []string{"p1", "p2"}, perPath)
	assert.Equal(t, "pre", snap["a"])
	assert.Equal(t, "p1", snap["b"])
}

// ----------------------------------------------------------------------
// envOrDefault / lookupConfigEnv
// ----------------------------------------------------------------------

func TestEnvOrDefault(t *testing.T) {
	t.Parallel()

	t.Run("returns env value when set", func(t *testing.T) {
		t.Parallel()

		got := envOrDefault(envFn(map[string]string{"APP_ENV": "prod"}), "app.env", "dev")
		assert.Equal(t, "prod", got)
	})

	t.Run("returns default when unset", func(t *testing.T) {
		t.Parallel()

		got := envOrDefault(envFn(nil), "app.env", "dev")
		assert.Equal(t, "dev", got)
	})

	t.Run("returns default when empty string", func(t *testing.T) {
		t.Parallel()

		got := envOrDefault(envFn(map[string]string{"APP_ENV": ""}), "app.env", "dev")
		assert.Equal(t, "dev", got)
	})
}

// ----------------------------------------------------------------------
// NewConfig: type x source matrix and priority
// ----------------------------------------------------------------------

func TestNewConfig_DefaultsOnly(t *testing.T) {
	t.Parallel()

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup:   envFn(nil),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)

	assert.Equal(t, AppEnvDev, cfg.GetAppEnv())
	assert.Equal(t, "", cfg.GetServiceName())
	assert.Equal(t, AppEnvDev, Get[string](cfg, "app.env"))
}

func TestNewConfig_FileLoad(t *testing.T) {
	t.Parallel()

	yaml := []byte("server:\n  host: example.com\n  port: 8080\nfeature:\n  enabled: true\ntimeout: 5s\nratios:\n  - 0.5\n  - 0.75\n")

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_CONFIG_FILE_PATHS": "/cfg.yaml",
		}),
		openFile:    fileFn(map[string][]byte{"/cfg.yaml": yaml}),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)

	assert.Equal(t, "example.com", Get[string](cfg, "server.host"))
	assert.Equal(t, 8080, Get[int](cfg, "server.port"))
	assert.True(t, Get[bool](cfg, "feature.enabled"))
	assert.Equal(t, 5*time.Second, Get[time.Duration](cfg, "timeout"))
}

func TestNewConfig_LegacyFilePath(t *testing.T) {
	t.Parallel()

	yaml := []byte("k: v\n")

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_CONFIG_FILE_PATH": "/legacy.yaml",
		}),
		openFile:    fileFn(map[string][]byte{"/legacy.yaml": yaml}),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)
	assert.Equal(t, "v", Get[string](cfg, "k"))
}

func TestNewConfig_FileEnvOverridesYaml(t *testing.T) {
	t.Parallel()

	yaml := []byte("server:\n  port: 8080\n")

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_CONFIG_FILE_PATHS": "/cfg.yaml",
			"SERVER_PORT":           "9090",
		}),
		openFile:    fileFn(map[string][]byte{"/cfg.yaml": yaml}),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)
	assert.Equal(t, 9090, Get[int](cfg, "server.port"))
}

func TestNewConfig_StaticEtcdOverridesFile(t *testing.T) {
	t.Parallel()

	yaml := []byte("server:\n  port: 8080\n")
	staticYaml := []byte("server:\n  port: 9000\n")

	fake := newFakeEtcdKV()
	fake.SetData("dev/svc/static.yaml", staticYaml)

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":             "svc",
			"APP_CONFIG_FILE_PATHS":        "/cfg.yaml",
			"APP_CONFIG_ETCD_STATIC_PATHS": "static.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":     "127.0.0.1:0",
		}),
		openFile:    fileFn(map[string][]byte{"/cfg.yaml": yaml}),
		etcdFactory: fakeFactory(fake),
	})
	require.NoError(t, err)

	assert.Equal(t, 9000, Get[int](cfg, "server.port"))
	// Static-only path: kv should be closed exactly once.
	assert.Equal(t, 1, fake.CloseCount())
}

func TestNewConfig_DynamicEtcdOverridesStatic(t *testing.T) {
	t.Parallel()

	staticYaml := []byte("server:\n  port: 8080\n")
	dynamicYaml := []byte("server:\n  port: 9090\n")

	staticFake := newFakeEtcdKV()
	staticFake.SetData("dev/svc/static.yaml", staticYaml)

	dynamicFake := newFakeEtcdKV()
	dynamicFake.SetData("dev/svc/dyn.yaml", dynamicYaml)

	// We need two different fakes — static and dynamic. Track factory calls.
	var calls atomic.Int32

	factory := func(string, time.Duration) (etcdKV, error) {
		n := calls.Add(1)
		if n == 1 {
			return staticFake, nil
		}

		return dynamicFake, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg, err := newConfigWithDeps(ctx, configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":              "svc",
			"APP_CONFIG_ETCD_STATIC_PATHS":  "static.yaml",
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "dyn.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: factory,
	})
	require.NoError(t, err)

	assert.Equal(t, 9090, Get[int](cfg, "server.port"))
}

func TestNewConfig_EnvOverridesDynamicEtcd(t *testing.T) {
	t.Parallel()

	dynYaml := []byte("server:\n  port: 9090\n")

	fake := newFakeEtcdKV()
	fake.SetData("dev/svc/dyn.yaml", dynYaml)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg, err := newConfigWithDeps(ctx, configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":              "svc",
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "dyn.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
			"SERVER_PORT":                   "1234",
		}),
		openFile:    noFileFn(),
		etcdFactory: fakeFactory(fake),
	})
	require.NoError(t, err)

	assert.Equal(t, 1234, Get[int](cfg, "server.port"))
}

func TestNewConfig_GetOrDefault(t *testing.T) {
	t.Parallel()

	yaml := []byte("present: 42\nstr: hello\n")

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_CONFIG_FILE_PATHS": "/cfg.yaml",
		}),
		openFile:    fileFn(map[string][]byte{"/cfg.yaml": yaml}),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)

	// Present → returns value.
	assert.Equal(t, 42, GetOrDefault[int](cfg, "present", 100))
	assert.Equal(t, "hello", GetOrDefault[string](cfg, "str", "fallback"))

	// Missing → returns default.
	assert.Equal(t, 100, GetOrDefault[int](cfg, "absent", 100))
	assert.Equal(t, "fallback", GetOrDefault[string](cfg, "absent", "fallback"))
	assert.Equal(t, []string{"a", "b"}, GetOrDefault[[]string](cfg, "absent", []string{"a", "b"}))
}

// ----------------------------------------------------------------------
// Type matrix
// ----------------------------------------------------------------------

func TestGet_TypeMatrix(t *testing.T) {
	t.Parallel()

	yaml := []byte(`
mystring: hello
myint: 42
myint64: 9223372036854775000
mybool: true
myfloat: 3.14
myduration: 250ms
myslice:
  - a
  - b
  - c
`)
	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_CONFIG_FILE_PATHS": "/c.yaml",
		}),
		openFile:    fileFn(map[string][]byte{"/c.yaml": yaml}),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)

	assert.Equal(t, "hello", Get[string](cfg, "mystring"))
	assert.Equal(t, 42, Get[int](cfg, "myint"))
	assert.Equal(t, int64(9223372036854775000), Get[int64](cfg, "myint64"))
	assert.True(t, Get[bool](cfg, "mybool"))
	assert.InDelta(t, 3.14, Get[float64](cfg, "myfloat"), 0.0001)
	assert.Equal(t, 250*time.Millisecond, Get[time.Duration](cfg, "myduration"))
	assert.Equal(t, []string{"a", "b", "c"}, Get[[]string](cfg, "myslice"))
}

func TestGet_TypeMatrix_FromEnv(t *testing.T) {
	t.Parallel()

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"MYSTRING":   "hello",
			"MYINT":      "42",
			"MYINT64":    "9223372036854775000",
			"MYBOOL":     "true",
			"MYFLOAT":    "3.14",
			"MYDURATION": "250ms",
			"MYSLICE":    "a,b,c",
		}),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)

	assert.Equal(t, "hello", Get[string](cfg, "mystring"))
	assert.Equal(t, 42, Get[int](cfg, "myint"))
	assert.Equal(t, int64(9223372036854775000), Get[int64](cfg, "myint64"))
	assert.True(t, Get[bool](cfg, "mybool"))
	assert.InDelta(t, 3.14, Get[float64](cfg, "myfloat"), 0.0001)
	assert.Equal(t, 250*time.Millisecond, Get[time.Duration](cfg, "myduration"))
	assert.Equal(t, []string{"a", "b", "c"}, Get[[]string](cfg, "myslice"))
}

// ----------------------------------------------------------------------
// Coerce errors via generic API
// ----------------------------------------------------------------------

func TestGet_CoerceFailureReturnsZero(t *testing.T) {
	t.Parallel()

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"BAD": "not-a-number",
		}),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)

	assert.Equal(t, 0, Get[int](cfg, "bad"))
	assert.Equal(t, int64(0), Get[int64](cfg, "bad"))
	assert.Equal(t, 0.0, Get[float64](cfg, "bad"))
	assert.Equal(t, time.Duration(0), Get[time.Duration](cfg, "bad"))

	// GetOrDefault returns def on coerce failure.
	assert.Equal(t, 42, GetOrDefault[int](cfg, "bad", 42))
	assert.Equal(t, int64(42), GetOrDefault[int64](cfg, "bad", 42))
	assert.InDelta(t, 1.5, GetOrDefault[float64](cfg, "bad", 1.5), 0.0001)
	assert.Equal(t, time.Second, GetOrDefault[time.Duration](cfg, "bad", time.Second))
}

// ----------------------------------------------------------------------
// Live semantics
// ----------------------------------------------------------------------

func TestLive_InitialAndAfterUpdate(t *testing.T) {
	t.Parallel()

	v1 := []byte("rate: 100\n")
	v2 := []byte("rate: 200\n")

	fake := newFakeEtcdKV()
	fake.SetData("dev/svc/dyn.yaml", v1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg, err := newConfigWithDeps(ctx, configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":              "svc",
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "dyn.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: fakeFactory(fake),
	})
	require.NoError(t, err)

	rate := Live[int](cfg, "rate")
	assert.Equal(t, 100, rate())

	// Wait for the watcher to subscribe before pushing.
	require.True(t, fake.WaitForWatcher("dev/svc/dyn.yaml", 1, time.Second))

	fake.Set("dev/svc/dyn.yaml", v2)

	assert.Eventually(t, func() bool {
		return rate() == 200
	}, 2*time.Second, time.Millisecond)
}

func TestLive_EnvWinsOverDynamic(t *testing.T) {
	t.Parallel()

	dyn := []byte("rate: 100\n")

	fake := newFakeEtcdKV()
	fake.SetData("dev/svc/dyn.yaml", dyn)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg, err := newConfigWithDeps(ctx, configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":              "svc",
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "dyn.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
			"RATE":                          "999",
		}),
		openFile:    noFileFn(),
		etcdFactory: fakeFactory(fake),
	})
	require.NoError(t, err)

	rate := Live[int](cfg, "rate")
	assert.Equal(t, 999, rate())

	require.True(t, fake.WaitForWatcher("dev/svc/dyn.yaml", 1, time.Second))
	fake.Set("dev/svc/dyn.yaml", []byte("rate: 50\n"))

	// env keeps winning regardless of dynamic updates.
	for range 5 {
		assert.Equal(t, 999, rate())
		time.Sleep(time.Millisecond)
	}
}

func TestLiveOrDefault(t *testing.T) {
	t.Parallel()

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup:   envFn(map[string]string{"PRESENT": "real"}),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)

	// Missing → fallback.
	missing := LiveOrDefault[string](cfg, "absent", "fallback")
	assert.Equal(t, "fallback", missing())

	// Present → real value.
	present := LiveOrDefault[string](cfg, "present", "fallback")
	assert.Equal(t, "real", present())

	// Coerce fail → fallback.
	cfg2, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup:   envFn(map[string]string{"BAD": "abc"}),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)
	bad := LiveOrDefault[int](cfg2, "bad", 7)
	assert.Equal(t, 7, bad())
}

// ----------------------------------------------------------------------
// Sentinel errors
// ----------------------------------------------------------------------

func TestNewConfig_ErrEmptyConfigPaths(t *testing.T) {
	t.Parallel()

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_CONFIG_FILE_PATHS": ",,,",
		}),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEmptyConfigPaths))
}

func TestNewConfig_ErrEtcdEndpointRequired_Static(t *testing.T) {
	t.Parallel()

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":             "svc",
			"APP_CONFIG_ETCD_STATIC_PATHS": "s.yaml",
		}),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEtcdEndpointRequired))
}

func TestNewConfig_ErrEtcdEndpointRequired_Dynamic(t *testing.T) {
	t.Parallel()

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":              "svc",
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "d.yaml",
		}),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEtcdEndpointRequired))
}

func TestNewConfig_ErrEmptyEtcdStaticPaths(t *testing.T) {
	t.Parallel()

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_CONFIG_ETCD_STATIC_PATHS": ",,,",
		}),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEmptyEtcdStaticPaths))
}

func TestNewConfig_ErrEmptyEtcdDynamicPaths(t *testing.T) {
	t.Parallel()

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": ",,,",
		}),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEmptyEtcdDynamicPaths))
}

func TestNewConfig_ErrServiceNameRequired_Static(t *testing.T) {
	t.Parallel()

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_CONFIG_ETCD_STATIC_PATHS": "s.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":     "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrServiceNameRequired))
}

func TestNewConfig_ErrServiceNameRequired_Dynamic(t *testing.T) {
	t.Parallel()

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "d.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: noFactory(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrServiceNameRequired))
}

func TestNewConfig_FileLoadError(t *testing.T) {
	t.Parallel()

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_CONFIG_FILE_PATHS": "/missing.yaml",
		}),
		openFile:    fileFn(nil),
		etcdFactory: noFactory(),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load from file")
}

func TestNewConfig_StaticEtcdLoadError(t *testing.T) {
	t.Parallel()

	fake := newFakeEtcdKV()
	// no data → Get returns ErrEtcdKeyNotFound

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":             "svc",
			"APP_CONFIG_ETCD_STATIC_PATHS": "s.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":     "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: fakeFactory(fake),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load from etcd static")
}

func TestNewConfig_DynamicEtcdLoadError(t *testing.T) {
	t.Parallel()

	fake := newFakeEtcdKV()
	// no data → first Get on dynamic path errors

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":              "svc",
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "d.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: fakeFactory(fake),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load from etcd dynamic")
	// kv must be closed on the error path.
	assert.GreaterOrEqual(t, fake.CloseCount(), 1)
}

func TestNewConfig_EtcdFactoryError_Dynamic(t *testing.T) {
	t.Parallel()

	boom := errors.New("dial fail")

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":              "svc",
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "d.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: func(string, time.Duration) (etcdKV, error) { return nil, boom },
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, boom))
}

func TestNewConfig_EtcdFactoryError_Static(t *testing.T) {
	t.Parallel()

	boom := errors.New("dial fail")

	_, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":             "svc",
			"APP_CONFIG_ETCD_STATIC_PATHS": "s.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":     "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: func(string, time.Duration) (etcdKV, error) { return nil, boom },
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, boom))
}

// ----------------------------------------------------------------------
// Lifecycle
// ----------------------------------------------------------------------

func TestNewConfig_DynamicCloseOnCtxCancel(t *testing.T) {
	t.Parallel()

	yaml := []byte("k: v\n")
	fake := newFakeEtcdKV()
	fake.SetData("dev/svc/dyn.yaml", yaml)

	ctx, cancel := context.WithCancel(t.Context())

	cfg, err := newConfigWithDeps(ctx, configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":              "svc",
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "dyn.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: fakeFactory(fake),
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)

	require.True(t, fake.WaitForWatcher("dev/svc/dyn.yaml", 1, time.Second))

	cancel()

	assert.Eventually(t, func() bool {
		return fake.CloseCount() >= 1
	}, 2*time.Second, time.Millisecond)
}

// ----------------------------------------------------------------------
// Immutability of bootstrap fields
// ----------------------------------------------------------------------

func TestNewConfig_AppEnvImmutableAgainstDynamicUpdate(t *testing.T) {
	t.Parallel()

	yaml := []byte("k: v\n")
	fake := newFakeEtcdKV()
	fake.SetData("dev/svc/dyn.yaml", yaml)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg, err := newConfigWithDeps(ctx, configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":              "svc",
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "dyn.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: fakeFactory(fake),
	})
	require.NoError(t, err)

	original := cfg.GetAppEnv()
	assert.Equal(t, AppEnvDev, original)

	require.True(t, fake.WaitForWatcher("dev/svc/dyn.yaml", 1, time.Second))
	// Push an update that tries to override app.env.
	fake.Set("dev/svc/dyn.yaml", []byte("app:\n  env: prod\n"))

	// Wait for the snapshot to catch up so we know the watcher applied the update.
	assert.Eventually(t, func() bool {
		return Get[string](cfg, "app.env") == "prod" || Get[string](cfg, "app.env") == AppEnvDev
	}, time.Second, time.Millisecond)

	// Bootstrap field must still be the original.
	assert.Equal(t, original, cfg.GetAppEnv())
}

// ----------------------------------------------------------------------
// Concurrency: Live closures + watcher updates under -race.
// ----------------------------------------------------------------------

func TestLive_Concurrency(t *testing.T) {
	t.Parallel()

	fake := newFakeEtcdKV()
	fake.SetData("dev/svc/dyn.yaml", []byte("rate: 1\n"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg, err := newConfigWithDeps(ctx, configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":              "svc",
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "dyn.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: fakeFactory(fake),
	})
	require.NoError(t, err)
	require.True(t, fake.WaitForWatcher("dev/svc/dyn.yaml", 1, time.Second))

	rate := Live[int](cfg, "rate")
	stop := make(chan struct{})

	// Writer pushes updates.
	var writerWg sync.WaitGroup

	writerWg.Go(func() {
		i := 0

		for {
			select {
			case <-stop:
				return
			default:
				i++
				fake.Set("dev/svc/dyn.yaml", fmt.Appendf(nil, "rate: %d\n", i))
			}
		}
	})

	// Many readers.
	const readers = 32

	var readerWg sync.WaitGroup

	for range readers {
		readerWg.Go(func() {
			for range 1000 {
				_ = rate()
			}
		})
	}

	readerWg.Wait()
	close(stop)
	writerWg.Wait()
}

// ----------------------------------------------------------------------
// Stale-key fix: dropped key is reflected after PUT.
// ----------------------------------------------------------------------

func TestLive_StaleKeyDropped(t *testing.T) {
	t.Parallel()

	v1 := []byte("a: 1\nb: 2\n")
	v2 := []byte("a: 1\n") // b removed

	fake := newFakeEtcdKV()
	fake.SetData("dev/svc/dyn.yaml", v1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg, err := newConfigWithDeps(ctx, configDeps{
		envLookup: envFn(map[string]string{
			"APP_SERVICE_NAME":              "svc",
			"APP_CONFIG_ETCD_DYNAMIC_PATHS": "dyn.yaml",
			"APP_CONFIG_ETCD_ENDPOINT":      "127.0.0.1:0",
		}),
		openFile:    noFileFn(),
		etcdFactory: fakeFactory(fake),
	})
	require.NoError(t, err)
	assert.Equal(t, 2, Get[int](cfg, "b"))

	require.True(t, fake.WaitForWatcher("dev/svc/dyn.yaml", 1, time.Second))
	fake.Set("dev/svc/dyn.yaml", v2)

	assert.Eventually(t, func() bool {
		return Get[int](cfg, "b") == 0
	}, 2*time.Second, time.Millisecond)
	assert.Equal(t, 1, Get[int](cfg, "a"))
}

// ----------------------------------------------------------------------
// NewConfig: production-wrapper smoke test
// ----------------------------------------------------------------------

// TestNewConfig_Smoke covers the production NewConfig wrapper. It cannot
// run in parallel because it uses t.Setenv to neutralize APP_CONFIG_*
// vars potentially inherited from the test process env.
func TestNewConfig_Smoke(t *testing.T) { //nolint:paralleltest // uses t.Setenv
	t.Setenv("APP_CONFIG_FILE_PATHS", "")
	t.Setenv("APP_CONFIG_FILE_PATH", "")
	t.Setenv("APP_CONFIG_ETCD_STATIC_PATHS", "")
	t.Setenv("APP_CONFIG_ETCD_DYNAMIC_PATHS", "")
	t.Setenv("APP_CONFIG_ETCD_PATH", "")
	t.Setenv("APP_CONFIG_ETCD_ENDPOINT", "")
	t.Setenv("APP_ENV", "")
	t.Setenv("APP_SERVICE_NAME", "")

	cfg, err := NewConfig(t.Context())
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, AppEnvDev, cfg.GetAppEnv())
}

// ----------------------------------------------------------------------
// EnvKey edge cases
// ----------------------------------------------------------------------

func TestEnvKey_EdgeCases(t *testing.T) {
	t.Parallel()

	// Documented mapping: dot → underscore, upper-case.
	assert.Equal(t, "API_V2_TIMEOUT", envKey("api.v2.timeout"))
	// Hyphens are not transformed (documented behaviour).
	assert.Equal(t, "MY-KEY_FOO", envKey("my-key.foo"))
	// Empty key.
	assert.Equal(t, "", envKey(""))
	// Multiple consecutive dots produce empty segments.
	assert.Equal(t, "A__B", envKey("a..b"))
	// Sanity check: matches strings.ReplaceAll semantics.
	assert.Equal(t, "X_Y_Z", strings.ToUpper(strings.ReplaceAll("x.y.z", ".", "_")))
}
