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

const AppEnvDev = "dev"

var (
	ErrEmptyConfigPaths      = errors.New("config paths is set but empty after parsing")
	ErrNoConfigPathSet       = errors.New("neither config.file.paths nor config.file.path is set")
	ErrEtcdEndpointRequired  = errors.New("etcd endpoint is required when using etcd")
	ErrEmptyEtcdStaticPaths  = errors.New("etcd static paths is set but empty after parsing")
	ErrEmptyEtcdDynamicPaths = errors.New("etcd dynamic paths is set but empty after parsing")
)

type Config struct {
	configStorage *viper.Viper
	serviceName   string
	appEnv        string
	mu            sync.RWMutex // Protects concurrent access to configStorage
}

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

func (c *Config) GetAppEnv() string {
	return c.appEnv
}

func (c *Config) GetServiceName() string {
	return c.serviceName
}

func (c *Config) GetStorage() *viper.Viper {
	return c.configStorage
}

func (c *Config) GetString(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.configStorage.GetString(key)
}

func (c *Config) GetStringOrDefault(key, defaultValue string) string {
	val := c.GetString(key)

	if val != "" {
		return val
	}

	return defaultValue
}

func (c *Config) GetStringSlice(key string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.configStorage.GetStringSlice(key)
}

func (c *Config) GetInt(key string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.configStorage.GetInt(key)
}

func (c *Config) GetBool(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.configStorage.GetBool(key)
}

func (c *Config) GetDuration(key string) time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.configStorage.GetDuration(key)
}

func (c *Config) WatchString(key string) func() string {
	return func() string {
		c.mu.RLock()
		defer c.mu.RUnlock()

		return c.configStorage.GetString(key)
	}
}

func (c *Config) WatchStringSlice(key string) func() []string {
	return func() []string {
		c.mu.RLock()
		defer c.mu.RUnlock()

		return c.configStorage.GetStringSlice(key)
	}
}

func (c *Config) WatchInt(key string) func() int {
	return func() int {
		c.mu.RLock()
		defer c.mu.RUnlock()

		return c.configStorage.GetInt(key)
	}
}

func (c *Config) WatchBool(key string) func() bool {
	return func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()

		return c.configStorage.GetBool(key)
	}
}

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

//nolint:cyclop // Static config loading requires multiple conditions for merging
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

	for idx, configPath := range configPaths {
		fullPath := buildEtcdPath(appEnv, serviceName, configPath)
		configType := getConfigTypeFromPath(fullPath)

		// Create temporary viper instance for each remote config
		tempV := viper.New()
		if err := tempV.AddRemoteProvider("etcd3", endpoint, fullPath); err != nil {
			return fmt.Errorf("add remote provider for %s: %w", fullPath, err)
		}

		tempV.SetConfigType(configType)

		if err := tempV.ReadRemoteConfig(); err != nil {
			return fmt.Errorf("read remote config from %s: %w", fullPath, err)
		}

		// Merge the config into main viper instance
		if idx == 0 && v.AllKeys() == nil {
			// If main viper is empty, read directly
			if err := v.AddRemoteProvider("etcd3", endpoint, fullPath); err != nil {
				return fmt.Errorf("add remote provider for %s: %w", fullPath, err)
			}

			v.SetConfigType(configType)

			if err := v.ReadRemoteConfig(); err != nil {
				return fmt.Errorf("read remote config from %s: %w", fullPath, err)
			}
		} else {
			// Merge settings from temp viper to main viper
			for _, key := range tempV.AllKeys() {
				v.Set(key, tempV.Get(key))
			}
		}
	}

	return nil
}

//nolint:cyclop,funlen,gocognit,nestif // Dynamic config loading with watching requires complex logic
func loadFromEtcdDynamic(ctx context.Context, cfg *Config, appEnv, serviceName string) error {
	v := cfg.configStorage
	// Check for multiple dynamic paths (new approach)
	configPathsStr := v.GetString("app.config.etcd.dynamic.paths")

	var configPaths []string

	if configPathsStr != "" {
		configPaths = parseCommaSeparatedPaths(configPathsStr)
		if len(configPaths) == 0 {
			return ErrEmptyEtcdDynamicPaths
		}
	} else {
		// Fallback to single path (backward compatibility)
		singlePath := v.GetString("app.config.etcd.path")
		if singlePath == "" {
			return nil // No dynamic etcd configs to load
		}

		configPaths = []string{singlePath}
	}

	endpoint := v.GetString("app.config.etcd.endpoint")
	if endpoint == "" {
		return ErrEtcdEndpointRequired
	}

	// Load all dynamic configs
	// Note: For multiple paths, we load each one and merge via Set()
	// Only the last remote provider will be watched by WatchRemoteConfig()
	var lastConfigType string

	for idx, configPath := range configPaths {
		fullPath := buildEtcdPath(appEnv, serviceName, configPath)
		configType := getConfigTypeFromPath(fullPath)

		if idx == 0 {
			// First config: read directly into main viper
			if err := v.AddRemoteProvider("etcd3", endpoint, fullPath); err != nil {
				return fmt.Errorf("add remote provider for %s: %w", fullPath, err)
			}

			v.SetConfigType(configType)

			if err := v.ReadRemoteConfig(); err != nil {
				return fmt.Errorf("read remote config from %s: %w", fullPath, err)
			}

			lastConfigType = configType
		} else {
			// Additional configs: load via temporary viper and merge
			tempV := viper.New()
			if err := tempV.AddRemoteProvider("etcd3", endpoint, fullPath); err != nil {
				return fmt.Errorf("add remote provider for %s: %w", fullPath, err)
			}

			tempV.SetConfigType(configType)

			if err := tempV.ReadRemoteConfig(); err != nil {
				return fmt.Errorf("read remote config from %s: %w", fullPath, err)
			}

			// Merge settings from temp viper to main viper
			for _, key := range tempV.AllKeys() {
				v.Set(key, tempV.Get(key))
			}

			// Update remote provider for watching (last one wins)
			if err := v.AddRemoteProvider("etcd3", endpoint, fullPath); err != nil {
				return fmt.Errorf("add remote provider for %s: %w", fullPath, err)
			}

			lastConfigType = configType
		}
	}

	// Set config type to last one for watching
	v.SetConfigType(lastConfigType)

	// Start watching dynamic configs
	// NOTE: WatchRemoteConfig only watches the last registered remote provider
	// For multiple dynamic configs, only the last one will have live reload
	go func() {
		ticker := time.NewTicker(defaultEtcdWatchInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Printf("stopping etcd config watcher: %v", ctx.Err())

				return
			case <-ticker.C:
				cfg.mu.Lock()

				err := v.WatchRemoteConfig()

				cfg.mu.Unlock()

				if err != nil {
					log.Printf("unable to watch remote config: %v", err)
				}
			}
		}
	}()

	return nil
}
