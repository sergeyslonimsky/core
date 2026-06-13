// Package di provides a generic, lock-free configuration loader and a
// thin Container wrapper for the two-step bootstrap pattern used by
// services built on core.
//
// Configuration sources are loaded in this order, from lowest to
// highest priority:
//
//	defaults  <  yaml file  <  etcd static  <  etcd dynamic  <  env vars
//
// env vars always win over every other source. Dotted keys map to
// underscore env vars, so http.frontend.port reads from HTTP_FRONTEND_PORT.
//
// Reading values is done via the package-level generic functions
// Get[T], GetOrDefault[T], Live[T] and LiveOrDefault[T]; the *Config
// struct exposes only the immutable bootstrap fields (GetAppEnv,
// GetServiceName) captured at NewConfig time.
//
// The dynamic etcd watcher composes the snapshot per-path (each path's
// last parsed map is stored separately and merged on every update),
// fixing the silent stale-key bug inherited from viper-remote: a PUT
// that drops a key now correctly removes it from the snapshot.
//
// Etcd access uses the native etcd v3 client (go.etcd.io/etcd/client/v3)
// behind the small etcdKV interface — no viper, no viper-remote.
// fakeEtcdKV drives dynamic-update paths in unit tests without a real
// etcd cluster.
//
// Path ownership: the caller passes full etcd keys via the
// app.config.etcd.{static,dynamic}.paths env vars. core/di does not
// prepend the service name or app env to the path. This keeps the
// loader storage-agnostic — vanilla etcd accepts arbitrary keys, while
// Elara and other layered stores require their own prefix conventions
// (e.g. "/{namespace}/...") which the deployment system is responsible
// for templating into the env var.
//
// Tunables (env vars):
//
//   - APP_CONFIG_ETCD_REQUEST_TIMEOUT: Go-duration string ("5s", "500ms")
//     bounding every etcd Get call. Defaults to 5s. Invalid value fails
//     startup with ErrInvalidEtcdRequestTimeout.
//
// Sentinel errors are split across files for locality:
//
//   - This file: ErrEmptyConfigPaths, ErrEtcdEndpointRequired,
//     ErrEmptyEtcdStaticPaths, ErrEmptyEtcdDynamicPaths,
//     ErrEtcdKeyNotFound, ErrServiceNameRequired.
//   - etcdclient.go: ErrInvalidEtcdRequestTimeout.
//   - loader_file.go: ErrNotRegularFile, ErrConfigFileTooLarge.
//   - loader_etcd.go: ErrUnknownConfigType.
package di

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

// AppEnvDev is the default value for app.env when no environment is set
// via env or yaml.
const AppEnvDev = "dev"

// Sentinel errors returned by NewConfig at startup.
var (
	ErrEmptyConfigPaths      = errors.New("config paths is set but empty after parsing")
	ErrEtcdEndpointRequired  = errors.New("etcd endpoint is required when using etcd")
	ErrEmptyEtcdStaticPaths  = errors.New("etcd static paths is set but empty after parsing")
	ErrEmptyEtcdDynamicPaths = errors.New("etcd dynamic paths is set but empty after parsing")
	ErrEtcdKeyNotFound       = errors.New("etcd key not found")
	ErrServiceNameRequired   = errors.New("service name is required when etcd paths are configured")
)

// Config holds the resolved configuration store plus the immutable
// bootstrap fields (app env, service name) captured at NewConfig time.
//
// Reads happen through the package-level generic functions di.Get[T] /
// di.GetOrDefault[T] / di.Live[T] / di.LiveOrDefault[T]. Reads are
// lock-free; the underlying snapshot is replaced atomically by the
// dynamic-watch goroutine when an etcd PUT arrives.
type Config struct {
	store       *store
	serviceName string
	appEnv      string
}

// NewConfig is the production constructor. It loads the configuration
// from yaml files, etcd static, and etcd dynamic in priority order,
// publishes the initial snapshot, and starts the dynamic-update
// watcher in a background goroutine bound to ctx.
//
// Behaviour:
//   - app.env defaults to AppEnvDev when not set in env.
//   - app.service.name is required if any etcd paths are configured.
//   - app.config.etcd.endpoint is required if any etcd paths are
//     configured.
//   - The static-load etcd client is opened and closed within the
//     load; the dynamic watcher owns its own client and closes it on
//     ctx cancellation.
func NewConfig(ctx context.Context) (*Config, error) {
	return newConfigWithDeps(ctx, configDeps{
		envLookup: os.LookupEnv,
		openFile:  osOpenFile,
		etcdFactory: func(endpoint string, requestTimeout time.Duration) (etcdKV, error) {
			return newRealEtcdKV(endpoint, requestTimeout)
		},
	})
}

// GetAppEnv returns the application environment captured at NewConfig
// time. The value is immutable for the process lifetime. Surfaces the
// service's bootstrap identity (logs, metrics, traces); it is no longer
// used to build etcd paths — see the package doc on path ownership.
func (c *Config) GetAppEnv() string {
	return c.appEnv
}

// GetServiceName returns the service name captured at NewConfig time.
// Immutable for the same reasons as GetAppEnv.
func (c *Config) GetServiceName() string {
	return c.serviceName
}

// ValueType is the closed set of types supported by the generic
// readers. Adding a new type means extending coerce[T] in lockstep.
type ValueType interface {
	string | int | int64 | bool | float64 | time.Duration | []string
}

// Get returns the value for key as T. If key is absent or the value
// cannot be coerced to T, the zero value of T is returned. Use Get
// for one-shot reads at startup; values pulled from etcd dynamic
// updates after startup will not be reflected without re-reading.
func Get[T ValueType](cfg *Config, key string) T {
	v, _ := read[T](cfg, key)

	return v
}

// GetOrDefault returns the value for key as T, or def if the key is
// absent OR the value cannot be coerced to T.
func GetOrDefault[T ValueType](cfg *Config, key string, def T) T {
	if v, ok := read[T](cfg, key); ok {
		return v
	}

	return def
}

// Live returns a closure that reads the current value of key on every
// invocation. Use Live for hot-reloadable values (rate limits, feature
// flags) that may change at runtime via etcd dynamic.
//
// Note: env vars override etcd dynamic. A key bound in env is locked
// for the process lifetime and Live() will keep returning the env
// value even if etcd PUTs a different value.
func Live[T ValueType](cfg *Config, key string) func() T {
	return func() T {
		v, _ := read[T](cfg, key)

		return v
	}
}

// LiveOrDefault is Live with a fallback when the key is absent or the
// value can't be coerced.
func LiveOrDefault[T ValueType](cfg *Config, key string, def T) func() T {
	return func() T {
		if v, ok := read[T](cfg, key); ok {
			return v
		}

		return def
	}
}

// read performs a single lookup-and-coerce. Returns (zero, false) when
// the key is missing or coercion fails.
func read[T ValueType](cfg *Config, key string) (T, bool) {
	raw, ok := cfg.store.lookup(key)
	if !ok {
		var zero T

		return zero, false
	}

	return coerce[T](raw)
}

// coerce dispatches to the type-specific coercion helpers based on T.
// Each branch must produce a value of type T (the conversion via
// any(zero).(T) is statically guaranteed by the ValueType union).
func coerce[T ValueType](v any) (T, bool) {
	var zero T

	switch any(zero).(type) {
	case string:
		s, ok := toString(v)

		return any(s).(T), ok //nolint:forcetypeassert // guarded by ValueType union
	case int:
		n, ok := toInt(v)

		return any(n).(T), ok //nolint:forcetypeassert // guarded by ValueType union
	case int64:
		n, ok := toInt64(v)

		return any(n).(T), ok //nolint:forcetypeassert // guarded by ValueType union
	case bool:
		b, ok := toBool(v)

		return any(b).(T), ok //nolint:forcetypeassert // guarded by ValueType union
	case float64:
		f, ok := toFloat64(v)

		return any(f).(T), ok //nolint:forcetypeassert // guarded by ValueType union
	case time.Duration:
		d, ok := toDuration(v)

		return any(d).(T), ok //nolint:forcetypeassert // guarded by ValueType union
	case []string:
		s, ok := toStringSlice(v)

		return any(s).(T), ok //nolint:forcetypeassert // guarded by ValueType union
	}

	return zero, false
}

// configDeps wires Config construction to its environment, file system
// and etcd. Production wiring lives in NewConfig; tests build Config
// via newConfigWithDeps.
type configDeps struct {
	envLookup   func(string) (string, bool)
	openFile    openFileFn
	etcdFactory func(endpoint string, requestTimeout time.Duration) (etcdKV, error)
}

// newConfigWithDeps is the testable constructor. The production
// NewConfig wraps it with default deps.
//
//nolint:funlen // linear bootstrap sequence, clearer in one function
func newConfigWithDeps(ctx context.Context, deps configDeps) (*Config, error) {
	// 1. Bootstrap fields are read directly from env, never from file
	// or etcd. They are immutable for the process lifetime and feed
	// the service identity (GetAppEnv/GetServiceName) used by logs,
	// metrics and traces. Path construction itself lives in the caller.
	appEnv := envOrDefault(deps.envLookup, "app.env", AppEnvDev)
	serviceName, _ := lookupConfigEnv(deps.envLookup, "app.service.name")

	defaults := map[string]any{"app.env": appEnv}
	s := newStore(deps.envLookup, defaults)

	cfg := &Config{
		store:       s,
		appEnv:      appEnv,
		serviceName: serviceName,
	}

	// 2. Resolve etcd per-request timeout up front so both static and
	// dynamic loads use the same bound. Fail-fast on a malformed env value.
	requestTimeout, err := resolveEtcdRequestTimeout(deps.envLookup)
	if err != nil {
		return nil, err
	}

	// 3. yaml files (if configured).
	fileMap, err := loadFileConfigs(deps)
	if err != nil {
		return nil, fmt.Errorf("load from file: %w", err)
	}

	// 4. etcd static (if configured). Uses a transient client closed
	// inside the helper.
	staticMap, err := loadStaticEtcd(ctx, deps, serviceName, requestTimeout)
	if err != nil {
		return nil, fmt.Errorf("load from etcd static: %w", err)
	}

	// 4. Pre-snapshot = file ⊕ static. defaults live in the store
	// separately and are consulted only when both env and snapshot
	// miss.
	pre := merge(fileMap, staticMap)

	// 5. Dynamic paths (if configured) — start the watcher.
	dynamicPaths, err := resolveDynamicPaths(deps)
	if err != nil {
		return nil, err
	}

	if len(dynamicPaths) == 0 {
		s.setSnapshot(pre)

		return cfg, nil
	}

	endpoint, err := resolveEtcdAccess(deps, serviceName)
	if err != nil {
		return nil, err
	}

	kv, err := deps.etcdFactory(endpoint, requestTimeout)
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	initial, err := loadEtcdPerPath(ctx, kv, dynamicPaths)
	if err != nil {
		_ = kv.Close()

		return nil, fmt.Errorf("load from etcd dynamic: %w", err)
	}

	initialSnap := composeSnapshot(pre, dynamicPaths, initial)
	s.setSnapshot(initialSnap)

	w := newDynamicWatcher(kv, dynamicPaths, pre, initial, s.setSnapshot)

	go w.run(ctx)

	return cfg, nil
}

// envOrDefault reads dottedKey from env (after dot→underscore upcase)
// and returns the value, or def when unset / empty.
//
//nolint:unparam // reusable helper designed for multiple potential keys
func envOrDefault(envLookup func(string) (string, bool), dottedKey, def string) string {
	if v, ok := lookupConfigEnv(envLookup, dottedKey); ok && v != "" {
		return v
	}

	return def
}

// lookupConfigEnv applies the standard env-var transform (dotted →
// SHOUTING_SNAKE) and looks up the result via envLookup.
func lookupConfigEnv(envLookup func(string) (string, bool), dottedKey string) (string, bool) {
	return envLookup(envKey(dottedKey))
}

// loadFileConfigs reads either app.config.file.paths (CSV) or the
// legacy app.config.file.path single value from env, then parses every
// referenced yaml. Returns (nil, nil) when neither env is set.
func loadFileConfigs(deps configDeps) (map[string]any, error) {
	multi, hasMulti := lookupConfigEnv(deps.envLookup, "app.config.file.paths")
	if hasMulti && multi != "" {
		paths := parseCommaSeparatedPaths(multi)
		if len(paths) == 0 {
			return nil, ErrEmptyConfigPaths
		}

		return loadFiles(deps.openFile, paths)
	}

	single, hasSingle := lookupConfigEnv(deps.envLookup, "app.config.file.path")
	if hasSingle && single != "" {
		return loadFiles(deps.openFile, []string{single})
	}

	return map[string]any{}, nil
}

// loadStaticEtcd reads app.config.etcd.static.paths (CSV) and merges
// every referenced etcd key. Opens a transient etcd client, closes it
// before returning. Returns (nil, nil) when no static paths are set.
//
// Paths are passed to etcd as-is. The caller (typically via env var
// templated by the deployment system) is responsible for any
// namespacing convention required by the etcd implementation it talks
// to (vanilla etcd: anything; Elara: leading "/").
func loadStaticEtcd(
	ctx context.Context,
	deps configDeps,
	serviceName string,
	requestTimeout time.Duration,
) (map[string]any, error) {
	raw, has := lookupConfigEnv(deps.envLookup, "app.config.etcd.static.paths")
	if !has || raw == "" {
		return map[string]any{}, nil
	}

	paths := parseCommaSeparatedPaths(raw)
	if len(paths) == 0 {
		return nil, ErrEmptyEtcdStaticPaths
	}

	endpoint, err := resolveEtcdAccess(deps, serviceName)
	if err != nil {
		return nil, err
	}

	kv, err := deps.etcdFactory(endpoint, requestTimeout)
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	defer func() {
		_ = kv.Close()
	}()

	return loadEtcdOnce(ctx, kv, paths)
}

// resolveDynamicPaths returns the list of relative paths to watch on
// etcd. Supports the new comma-separated app.config.etcd.dynamic.paths
// and the legacy single app.config.etcd.path. Returns nil when neither
// is set.
func resolveDynamicPaths(deps configDeps) ([]string, error) {
	if multi, ok := lookupConfigEnv(deps.envLookup, "app.config.etcd.dynamic.paths"); ok && multi != "" {
		paths := parseCommaSeparatedPaths(multi)
		if len(paths) == 0 {
			return nil, ErrEmptyEtcdDynamicPaths
		}

		return paths, nil
	}

	if single, ok := lookupConfigEnv(deps.envLookup, "app.config.etcd.path"); ok && single != "" {
		return []string{single}, nil
	}

	return nil, nil
}

// composeSnapshot merges pre with every per-path map in `paths` order.
// Later paths win on conflict. Used for the initial snapshot publish
// before the dynamic watcher starts (after start, the watcher rebuilds
// snapshots itself).
func composeSnapshot(pre map[string]any, paths []string, perPath map[string]map[string]any) map[string]any {
	snap := cloneMap(pre)

	for _, p := range paths {
		state, ok := perPath[p]
		if !ok {
			continue
		}

		snap = merge(snap, state)
	}

	return snap
}

// Compile-time guard that osOpenFile satisfies openFileFn — the alias
// stays visible to tests that build a configDeps{...} literal.
var _ openFileFn = osOpenFile

// resolveEtcdAccess returns the configured endpoint (required when any
// etcd path is set) and validates that serviceName is non-empty.
func resolveEtcdAccess(deps configDeps, serviceName string) (string, error) {
	endpoint, _ := lookupConfigEnv(deps.envLookup, "app.config.etcd.endpoint")
	if endpoint == "" {
		return "", ErrEtcdEndpointRequired
	}

	if serviceName == "" {
		return "", ErrServiceNameRequired
	}

	return endpoint, nil
}
