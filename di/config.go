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
package di

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	_ "github.com/spf13/viper/remote" // Required to enable viper remote config support for etcd
)

const defaultEtcdWatchInterval = 5 * time.Second

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
		if err := loadFromEtcdStatic(v, appEnv, serviceName); err != nil {
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

func loadFromEtcdStatic(v *viper.Viper, appEnv, serviceName string) error {
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

	// Read every path via a temporary viper instance and merge into the
	// main one via Set. This keeps merge semantics identical across the
	// first and subsequent iterations — the previous "first direct, rest
	// via tempV" branch was gated on `v.AllKeys() == nil`, which is
	// effectively never true after AutomaticEnv + SetDefault, leaving the
	// "direct" branch unreachable.
	for _, configPath := range configPaths {
		fullPath := buildEtcdPath(appEnv, serviceName, configPath)
		configType := getConfigTypeFromPath(fullPath)

		tempV := viper.New()
		if err := tempV.AddRemoteProvider("etcd3", endpoint, fullPath); err != nil {
			return fmt.Errorf("add remote provider for %s: %w", fullPath, err)
		}

		tempV.SetConfigType(configType)

		if err := tempV.ReadRemoteConfig(); err != nil {
			return fmt.Errorf("read remote config from %s: %w", fullPath, err)
		}

		for _, key := range tempV.AllKeys() {
			v.Set(key, tempV.Get(key))
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

	watchConfigType, err := mergeDynamicConfigs(
		cfg.configStorage,
		endpoint,
		appEnv,
		serviceName,
		configPaths,
	)
	if err != nil {
		return err
	}

	// viper's WatchRemoteConfig polls only the FIRST registered remote
	// provider. For multi-path dynamic setups, only the first path gets
	// live reload; subsequent paths are merged once at startup.
	cfg.configStorage.SetConfigType(watchConfigType)

	go watchRemoteConfig(ctx, cfg)

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

// mergeDynamicConfigs loads every dynamic path and merges into the main
// viper. The FIRST path is read directly into the main viper's kvstore
// (via ReadRemoteConfig) — this is the only path the watcher can live-
// reload because viper's WatchRemoteConfig polls the first registered
// provider and updates v.kvstore; values injected via v.Set land in
// v.override, which has higher priority than kvstore, so reads of those
// keys would not see remote updates. Subsequent paths therefore can only
// be used for initial-load merge (via tempV + Set) and are not watched.
//
// Returns the config type of the first path (used for SetConfigType so
// WatchRemoteConfig can parse incoming payloads).
func mergeDynamicConfigs(
	v *viper.Viper,
	endpoint, appEnv, serviceName string,
	configPaths []string,
) (string, error) {
	var watchConfigType string

	for idx, configPath := range configPaths {
		fullPath := buildEtcdPath(appEnv, serviceName, configPath)
		configType := getConfigTypeFromPath(fullPath)

		if idx == 0 {
			// First path: register + read into main viper so the watcher
			// (which polls the first provider and updates v.kvstore) can
			// observe remote updates.
			if err := v.AddRemoteProvider("etcd3", endpoint, fullPath); err != nil {
				return "", fmt.Errorf("add remote provider for %s: %w", fullPath, err)
			}

			v.SetConfigType(configType)

			if err := v.ReadRemoteConfig(); err != nil {
				return "", fmt.Errorf("read remote config from %s: %w", fullPath, err)
			}

			watchConfigType = configType

			continue
		}

		// Subsequent paths: initial load only (no watch). Use Set because
		// another ReadRemoteConfig on v would overwrite v.kvstore and
		// drop values from earlier paths.
		tempV := viper.New()
		if err := tempV.AddRemoteProvider("etcd3", endpoint, fullPath); err != nil {
			return "", fmt.Errorf("add remote provider for %s: %w", fullPath, err)
		}

		tempV.SetConfigType(configType)

		if err := tempV.ReadRemoteConfig(); err != nil {
			return "", fmt.Errorf("read remote config from %s: %w", fullPath, err)
		}

		for _, key := range tempV.AllKeys() {
			v.Set(key, tempV.Get(key))
		}
	}

	return watchConfigType, nil
}

// watchRemoteConfig periodically polls the last-registered remote provider
// for updates. Exits when ctx is cancelled. Acquires cfg.mu for each poll
// so WatchRemoteConfig's in-place map updates don't race with readers.
func watchRemoteConfig(ctx context.Context, cfg *Config) {
	ticker := time.NewTicker(defaultEtcdWatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("stopping etcd config watcher: %v", ctx.Err())

			return
		case <-ticker.C:
			cfg.mu.Lock()
			err := cfg.configStorage.WatchRemoteConfig()
			cfg.mu.Unlock()

			if err != nil {
				log.Printf("unable to watch remote config: %v", err)
			}
		}
	}
}
