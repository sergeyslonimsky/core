// Package di provides a viper-backed configuration loader and a thin
// generic Container wrapper for the two-step bootstrap pattern used by
// services built on core.
//
// The package intentionally does NOT provide:
//
//   - Typed config binding (no mapstructure tags on core package Config
//     structs). Consumer apps compose core Config structs inside their own
//     app-level config and perform explicit viper-key → field mapping in
//     their own NewConfig().
//
//   - Cleanup tracking (no AddOnClose). Every component that holds
//     resources implements lifecycle.Resource and is registered with
//     app.App, which handles LIFO shutdown across the service.
//
// Config loads values in order from file → static etcd → dynamic etcd, and
// supports Watch* subscriptions for dynamic keys that may change at runtime.
//
// Etcd access uses the native etcd v3 client directly (go.etcd.io/etcd/client/v3)
// rather than viper/remote, which transitively pulls in consul/serf/crypt.
package di

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultEtcdDialTimeout    = 5 * time.Second
	defaultEtcdRequestTimeout = 5 * time.Second

	// Watch reconnect backoff. On disconnect (network drop, server restart,
	// compaction) the per-key goroutine sleeps and reopens the watch — delay
	// doubles on consecutive failures, capped at maxWatchBackoff.
	initialWatchBackoff = 1 * time.Second
	maxWatchBackoff     = 30 * time.Second
)

// AppEnvDev is the default value for app.env when no environment is set
// via config or environment variable.
const AppEnvDev = "dev"

// Sentinel errors returned by NewConfig for configuration mistakes at startup.
var (
	ErrEmptyConfigPaths      = errors.New("config paths is set but empty after parsing")
	ErrNoConfigPathSet       = errors.New("neither config.file.paths nor config.file.path is set")
	ErrEtcdEndpointRequired  = errors.New("etcd endpoint is required when using etcd")
	ErrEmptyEtcdStaticPaths  = errors.New("etcd static paths is set but empty after parsing")
	ErrEmptyEtcdDynamicPaths = errors.New("etcd dynamic paths is set but empty after parsing")
	ErrEtcdKeyNotFound       = errors.New("etcd key not found")
)

// Config wraps a *viper.Viper with a read-write mutex so dynamic etcd updates
// can proceed concurrently with reads from application code. Instances are
// constructed by NewConfig and passed to the application's own typed config
// builder, which performs explicit viper-key → field mapping.
//
// All Get* and Watch* methods are safe for concurrent use.
type Config struct {
	configStorage *viper.Viper
	serviceName   string
	appEnv        string
	mu            sync.RWMutex // Protects concurrent access to configStorage
}

// NewConfig loads the application configuration from the configured sources
// in order:
//
//  1. file (app.config.file.paths or app.config.file.path)
//  2. etcd static snapshot (app.config.etcd.static.paths)
//  3. etcd dynamic configs (app.config.etcd.dynamic.paths) — watched in a
//     background goroutine that stops when ctx is cancelled
//
// Environment variables override all sources (viper.AutomaticEnv). Dotted
// keys map to underscore-separated env vars, so http.frontend.port reads
// from HTTP_FRONTEND_PORT.
//
// The returned *Config must be used by the consumer app to build its own
// typed config struct — core packages never see *Config directly.
func NewConfig(ctx context.Context) (*Config, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetDefault("app.env", AppEnvDev)

	// Capture app.env and app.service.name from environment before loading any configs.
	// These values are immutable and used for building etcd paths - they should not be
	// affected by values loaded from config files or etcd to prevent path inconsistencies.
	appEnv := v.GetString("app.env")
	serviceName := v.GetString("app.service.name")

	// Create Config early so we can use its mutex for thread-safe dynamic config updates
	cfg := &Config{ //nolint:exhaustruct // mu field has valid zero value
		configStorage: v,
		serviceName:   serviceName,
		appEnv:        appEnv,
	}

	// Load configs in order: file -> static etcd -> dynamic etcd
	// All sources can be combined and will be merged

	// 1. Load file configs (static, loaded once)
	if v.GetString("app.config.file.paths") != "" {
		if err := loadFromFile(v); err != nil {
			return nil, fmt.Errorf("load from file: %w", err)
		}
	}

	// 2. Load static etcd configs (no watching)
	if v.GetString("app.config.etcd.static.paths") != "" {
		if err := loadFromEtcdStatic(ctx, v, appEnv, serviceName); err != nil {
			return nil, fmt.Errorf("load from etcd static: %w", err)
		}
	}

	// 3. Load dynamic etcd configs (with watching)
	if v.GetString("app.config.etcd.dynamic.paths") != "" {
		if err := loadFromEtcdDynamic(ctx, cfg, appEnv, serviceName); err != nil {
			return nil, fmt.Errorf("load from etcd dynamic: %w", err)
		}
	}

	return cfg, nil
}

// GetAppEnv returns the application environment (e.g., "dev", "prod") as
// captured from app.env at config startup. The value is immutable — it is
// used to build etcd paths and must not change during a process's lifetime.
func (c *Config) GetAppEnv() string {
	return c.appEnv
}

// GetServiceName returns the service name as captured from app.service.name
// at config startup. Immutable for the same reasons as GetAppEnv.
func (c *Config) GetServiceName() string {
	return c.serviceName
}

// GetStorage returns the underlying *viper.Viper. Prefer the Get*/Watch*
// methods on Config when possible — direct viper access bypasses the mutex
// and can race with dynamic etcd updates.
func (c *Config) GetStorage() *viper.Viper {
	return c.configStorage
}

// GetString returns the string value associated with key, or "" if the key
// is absent. Concurrent-safe.
func (c *Config) GetString(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.configStorage.GetString(key)
}

// GetStringOrDefault returns the string value for key, or defaultValue if
// the key is absent or the configured value is empty.
func (c *Config) GetStringOrDefault(key, defaultValue string) string {
	val := c.GetString(key)

	if val != "" {
		return val
	}

	return defaultValue
}

// GetStringSlice returns the value for key as a string slice (viper splits
// a comma-separated string or returns an array as-is). Returns nil if the
// key is absent.
func (c *Config) GetStringSlice(key string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.configStorage.GetStringSlice(key)
}

// GetInt returns the integer value for key, or 0 if the key is absent or
// cannot be parsed as an integer.
func (c *Config) GetInt(key string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.configStorage.GetInt(key)
}

// GetBool returns the boolean value for key, or false if the key is absent
// or cannot be parsed as a boolean.
func (c *Config) GetBool(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.configStorage.GetBool(key)
}

// GetDuration returns the value for key parsed as a time.Duration (e.g.,
// "5s", "250ms"), or 0 if the key is absent or malformed.
func (c *Config) GetDuration(key string) time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.configStorage.GetDuration(key)
}

// WatchString returns a closure that reads the current value of key on every
// invocation. Use for dynamically-reloadable values sourced from etcd — each
// call reflects the latest watched value, not the value at closure-creation
// time.
func (c *Config) WatchString(key string) func() string {
	return func() string {
		c.mu.RLock()
		defer c.mu.RUnlock()

		return c.configStorage.GetString(key)
	}
}

// WatchStringSlice returns a closure that reads the current string-slice
// value of key on every invocation. See WatchString for semantics.
func (c *Config) WatchStringSlice(key string) func() []string {
	return func() []string {
		c.mu.RLock()
		defer c.mu.RUnlock()

		return c.configStorage.GetStringSlice(key)
	}
}

// WatchInt returns a closure that reads the current integer value of key on
// every invocation. See WatchString for semantics.
func (c *Config) WatchInt(key string) func() int {
	return func() int {
		c.mu.RLock()
		defer c.mu.RUnlock()

		return c.configStorage.GetInt(key)
	}
}

// WatchBool returns a closure that reads the current boolean value of key
// on every invocation. See WatchString for semantics.
func (c *Config) WatchBool(key string) func() bool {
	return func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()

		return c.configStorage.GetBool(key)
	}
}

// WatchDuration returns a closure that reads the current duration value of
// key on every invocation. See WatchString for semantics.
func (c *Config) WatchDuration(key string) func() time.Duration {
	return func() time.Duration {
		c.mu.RLock()
		defer c.mu.RUnlock()

		return c.configStorage.GetDuration(key)
	}
}

// parseCommaSeparatedPaths splits a comma-separated string into a slice of trimmed paths.
func parseCommaSeparatedPaths(pathsStr string) []string {
	if pathsStr == "" {
		return nil
	}

	paths := strings.Split(pathsStr, ",")
	result := make([]string, 0, len(paths))

	for _, path := range paths {
		trimmed := strings.TrimSpace(path)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}

// buildEtcdPath constructs the full etcd path from env, service name, and config path.
// Uses immutable appEnv and serviceName to ensure paths don't change during config loading.
func buildEtcdPath(appEnv, serviceName, configPath string) string {
	return fmt.Sprintf(
		"%s/%s/%s",
		appEnv,
		serviceName,
		configPath,
	)
}

// getConfigTypeFromPath extracts config type from path (e.g., "config.yaml" -> "yaml").
func getConfigTypeFromPath(path string) string {
	parsedPath := strings.Split(path, ".")
	if len(parsedPath) > 1 {
		return parsedPath[len(parsedPath)-1]
	}

	return "yaml"
}

func loadFromFile(v *viper.Viper) error {
	v.SetConfigName("config")
	v.SetConfigType("yaml")

	// Check for multiple file paths (new approach)
	configPathsStr := v.GetString("app.config.file.paths")
	if configPathsStr != "" {
		configPaths := parseCommaSeparatedPaths(configPathsStr)
		if len(configPaths) == 0 {
			return ErrEmptyConfigPaths
		}

		// Load first config file
		v.AddConfigPath(configPaths[0])

		if err := v.ReadInConfig(); err != nil {
			return fmt.Errorf("read config from %s: %w", configPaths[0], err)
		}

		// Merge remaining config files
		for _, configPath := range configPaths[1:] {
			v.AddConfigPath(configPath)

			if err := v.MergeInConfig(); err != nil {
				return fmt.Errorf("merge config from %s: %w", configPath, err)
			}
		}

		return nil
	}

	// Fallback to single file path (backward compatibility)
	configPath := v.GetString("app.config.file.path")
	if configPath == "" {
		return ErrNoConfigPathSet
	}

	v.AddConfigPath(configPath)

	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("read config from %s: %w", configPath, err)
	}

	return nil
}

// mergeBytesIntoViper parses data as configType (yaml/json/toml/...) and
// copies every resulting key into v via Set. Pure function — no I/O.
func mergeBytesIntoViper(v *viper.Viper, data []byte, configType string) error {
	tempV := viper.New()
	tempV.SetConfigType(configType)

	if err := tempV.ReadConfig(bytes.NewReader(data)); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	for _, key := range tempV.AllKeys() {
		v.Set(key, tempV.Get(key))
	}

	return nil
}

// newEtcdClient builds a clientv3.Client for a single endpoint.
func newEtcdClient(endpoint string) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{ //nolint:exhaustruct // zero values are fine for unused fields
		Endpoints:   []string{endpoint},
		DialTimeout: defaultEtcdDialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	return cli, nil
}

// readEtcdKey fetches a single key from etcd and returns its value. Returns
// ErrEtcdKeyNotFound when the key has no value (matches the old viper/remote
// behavior, which failed on empty reads).
func readEtcdKey(ctx context.Context, cli *clientv3.Client, key string) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, defaultEtcdRequestTimeout)
	defer cancel()

	resp, err := cli.Get(reqCtx, key)
	if err != nil {
		return nil, fmt.Errorf("get etcd key %s: %w", key, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrEtcdKeyNotFound, key)
	}

	return resp.Kvs[0].Value, nil
}

// readAndMergeEtcdKey reads a key from etcd and merges its parsed contents
// into v. The config type is derived from the key's file extension.
func readAndMergeEtcdKey(ctx context.Context, cli *clientv3.Client, v *viper.Viper, key string) error {
	data, err := readEtcdKey(ctx, cli, key)
	if err != nil {
		return err
	}

	if err := mergeBytesIntoViper(v, data, getConfigTypeFromPath(key)); err != nil {
		return fmt.Errorf("merge from %s: %w", key, err)
	}

	return nil
}

func loadFromEtcdStatic(ctx context.Context, v *viper.Viper, appEnv, serviceName string) error {
	configPathsStr := v.GetString("app.config.etcd.static.paths")
	if configPathsStr == "" {
		return nil // No static etcd configs to load
	}

	configPaths := parseCommaSeparatedPaths(configPathsStr)
	if len(configPaths) == 0 {
		return ErrEmptyEtcdStaticPaths
	}

	endpoint := v.GetString("app.config.etcd.endpoint")
	if endpoint == "" {
		return ErrEtcdEndpointRequired
	}

	cli, err := newEtcdClient(endpoint)
	if err != nil {
		return err
	}
	defer func() {
		_ = cli.Close()
	}()

	for _, configPath := range configPaths {
		fullPath := buildEtcdPath(appEnv, serviceName, configPath)
		if err := readAndMergeEtcdKey(ctx, cli, v, fullPath); err != nil {
			return err
		}
	}

	return nil
}

func loadFromEtcdDynamic(ctx context.Context, cfg *Config, appEnv, serviceName string) error {
	configPaths, err := resolveDynamicConfigPaths(cfg.configStorage)
	if err != nil {
		return err
	}

	if len(configPaths) == 0 {
		return nil // No dynamic etcd configs to load
	}

	endpoint := cfg.configStorage.GetString("app.config.etcd.endpoint")
	if endpoint == "" {
		return ErrEtcdEndpointRequired
	}

	cli, err := newEtcdClient(endpoint)
	if err != nil {
		return err
	}

	fullPaths := make([]string, 0, len(configPaths))

	for _, configPath := range configPaths {
		fullPath := buildEtcdPath(appEnv, serviceName, configPath)

		if err := readAndMergeEtcdKey(ctx, cli, cfg.configStorage, fullPath); err != nil {
			_ = cli.Close()

			return err
		}

		fullPaths = append(fullPaths, fullPath)
	}

	// Native clientv3.Watch covers every dynamic path — unlike viper's
	// WatchRemoteConfig, which only polled the first registered provider.
	go watchEtcdKeys(ctx, cfg, cli, fullPaths)

	return nil
}

// resolveDynamicConfigPaths returns the list of etcd paths to load as
// dynamic (watched) configs. Supports both the new comma-separated
// `app.config.etcd.dynamic.paths` and the legacy single
// `app.config.etcd.path` setting. Returns an empty slice (no error) when
// neither is set — caller decides whether that is OK.
func resolveDynamicConfigPaths(v *viper.Viper) ([]string, error) {
	if multi := v.GetString("app.config.etcd.dynamic.paths"); multi != "" {
		paths := parseCommaSeparatedPaths(multi)
		if len(paths) == 0 {
			return nil, ErrEmptyEtcdDynamicPaths
		}

		return paths, nil
	}

	if single := v.GetString("app.config.etcd.path"); single != "" {
		return []string{single}, nil
	}

	// "no paths configured" is a valid non-error outcome
	return nil, nil
}

// watchEtcdKeys spawns one watcher per key, each of which reconnects with
// exponential backoff on disconnect. Exits when ctx is cancelled, then
// closes the shared client.
func watchEtcdKeys(ctx context.Context, cfg *Config, cli *clientv3.Client, paths []string) {
	defer func() {
		_ = cli.Close()
	}()

	var wg sync.WaitGroup

	for _, key := range paths {
		wg.Add(1)

		go func(key string) {
			defer wg.Done()
			watchSingleKey(ctx, cfg, cli, key)
		}(key)
	}

	wg.Wait()

	log.Printf("stopping etcd config watcher: %v", ctx.Err())
}

// watchSingleKey streams events for one key, reconnecting on any channel
// close (compaction, network drop, server restart) with exponential
// backoff. After every reconnect, re-syncs the value via Get so state
// catches up on updates missed while disconnected.
func watchSingleKey(ctx context.Context, cfg *Config, cli *clientv3.Client, key string) {
	backoff := initialWatchBackoff

	for ctx.Err() == nil {
		streamErr := streamKeyEvents(ctx, cfg, cli, key)
		if ctx.Err() != nil {
			return
		}

		// Jitter on the delay so that a fleet of services with this config
		// module doesn't synchronize their reconnect storms when etcd comes
		// back up.
		delay := backoff + rand.N(backoff/2+1) //nolint:gosec // not security-sensitive

		log.Printf("etcd watch for %s ended (%v); reconnecting in %v", key, streamErr, delay)

		if !sleepWithCtx(ctx, delay) {
			return
		}

		// After a reconnect, re-sync via Get — if etcd compacted updates
		// during the disconnect, Watch alone would miss them.
		if err := resyncKey(ctx, cfg, cli, key); err != nil {
			log.Printf("etcd resync for %s failed: %v", key, err)

			backoff = nextBackoff(backoff)

			continue
		}

		backoff = initialWatchBackoff
	}
}

// streamKeyEvents consumes Watch events for key until the channel closes.
// Returns nil when the channel closes with no error set on the last
// response (clean server-side close or ctx cancellation).
//
// DELETE events are intentionally ignored: the old viper/remote
// implementation only reacted to value reads, never to removals, and
// callers rely on that contract.
func streamKeyEvents(ctx context.Context, cfg *Config, cli *clientv3.Client, key string) error {
	ch := cli.Watch(ctx, key)

	for resp := range ch {
		// clientv3 closes the channel immediately after sending an
		// error-response, so break out here — any remaining receives
		// would be on an already-closed channel.
		if err := resp.Err(); err != nil {
			return err
		}

		for _, ev := range resp.Events {
			if ev.Type != clientv3.EventTypePut {
				continue
			}

			applyEtcdUpdate(cfg, key, ev.Kv.Value)
		}
	}

	return nil
}

// resyncKey performs a fresh Get and merges the result under the write
// lock — safe to call concurrently with reader traffic.
func resyncKey(ctx context.Context, cfg *Config, cli *clientv3.Client, key string) error {
	data, err := readEtcdKey(ctx, cli, key)
	if err != nil {
		return err
	}

	applyEtcdUpdate(cfg, key, data)

	return nil
}

// sleepWithCtx blocks for d or until ctx is cancelled. Returns true if
// the full duration elapsed, false if ctx cancelled first.
func sleepWithCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// nextBackoff doubles current up to maxWatchBackoff.
func nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > maxWatchBackoff {
		return maxWatchBackoff
	}

	return next
}

// applyEtcdUpdate merges a single etcd PUT value into cfg under the write lock.
func applyEtcdUpdate(cfg *Config, key string, value []byte) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	if err := mergeBytesIntoViper(cfg.configStorage, value, getConfigTypeFromPath(key)); err != nil {
		log.Printf("apply etcd update for %s: %v", key, err)
	}
}
