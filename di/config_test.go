//go:build integration

package di_test

import (
	"testing"
	"time"

	"github.com/sergeyslonimsky/core/di"
	"github.com/sergeyslonimsky/core/internal/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewConfig_FileOnly tests loading configuration from files only.
func TestNewConfig_FileOnly(t *testing.T) {
	tests := []struct {
		name        string
		envVars     map[string]string
		wantEnv     string
		wantService string
		wantValue   string
		wantErr     bool
	}{
		{
			name: "single file with app.config.file.paths",
			envVars: map[string]string{
				"APP_CONFIG_FILE_PATHS": "./testdata",
				"APP_ENV":               "test",
				"APP_SERVICE_NAME":      "test-service",
			},
			wantEnv:     "test",
			wantService: "test-service",
			wantValue:   "localhost",
			wantErr:     false,
		},
		{
			name: "multiple files with app.config.file.paths",
			envVars: map[string]string{
				"APP_CONFIG_FILE_PATHS": "./testdata,./testdata",
				"APP_ENV":               "test",
				"APP_SERVICE_NAME":      "test-service",
			},
			wantEnv:     "test",
			wantService: "test-service",
			wantValue:   "localhost",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for key, value := range tt.envVars {
				t.Setenv(key, value)
			}

			cfg, err := di.NewConfig(t.Context())

			if tt.wantErr {
				assert.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantEnv, cfg.GetAppEnv())
			assert.Equal(t, tt.wantService, cfg.GetServiceName())
			assert.Equal(t, tt.wantValue, cfg.GetString("database.host"))
		})
	}
}

// TestNewConfig_FilesMerge tests merging of multiple configuration files.
func TestNewConfig_FilesMerge(t *testing.T) {
	t.Setenv("APP_CONFIG_FILE_PATHS", "./testdata,./testdata")
	t.Setenv("APP_ENV", "test")
	t.Setenv("APP_SERVICE_NAME", "test-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)

	// Check that config was loaded
	assert.Equal(t, "test", cfg.GetAppEnv())
	assert.Equal(t, "test-service", cfg.GetServiceName())
	assert.Equal(t, "localhost", cfg.GetString("database.host"))
	assert.Equal(t, 5432, cfg.GetInt("database.port"))
}

// TestNewConfig_FilesError tests error handling for file loading.
func TestNewConfig_FilesError(t *testing.T) {
	tests := []struct {
		name    string
		envVars map[string]string
		wantErr bool
	}{
		{
			name: "nonexistent file path",
			envVars: map[string]string{
				"APP_CONFIG_FILE_PATHS": "./nonexistent",
			},
			wantErr: true,
		},
		{
			name: "empty paths string after parsing",
			envVars: map[string]string{
				"APP_CONFIG_FILE_PATHS": ",,,",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.envVars {
				t.Setenv(key, value)
			}

			_, err := di.NewConfig(t.Context())

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestNewConfig_EtcdStatic tests loading static configuration from etcd.
func TestNewConfig_EtcdStatic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration test in short mode")
	}

	endpoint, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	// Prepare test data in etcd
	testData := map[string]interface{}{
		"database": map[string]interface{}{
			"host": "etcd-db-host",
			"port": 3306,
		},
	}

	testhelpers.PutEtcdValue(t, endpoint, "test/test-service/config.yaml", testData)

	// Set environment variables
	t.Setenv("APP_CONFIG_ETCD_ENDPOINT", endpoint)
	t.Setenv("APP_CONFIG_ETCD_STATIC_PATHS", "config.yaml")
	t.Setenv("APP_ENV", "test")
	t.Setenv("APP_SERVICE_NAME", "test-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)

	// Verify configuration was loaded from etcd
	assert.Equal(t, "test", cfg.GetAppEnv(), "app.env should be immutable from env var")
	assert.Equal(t, "test-service", cfg.GetServiceName(), "app.service.name should be immutable from env var")
	assert.Equal(t, "etcd-db-host", cfg.GetString("database.host"))
	assert.Equal(t, 3306, cfg.GetInt("database.port"))
}

// TestNewConfig_EtcdStaticMultiplePaths tests loading multiple static configs from etcd.
func TestNewConfig_EtcdStaticMultiplePaths(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration test in short mode")
	}

	endpoint, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	// Prepare first config
	baseConfig := map[string]interface{}{
		"database": map[string]interface{}{
			"host": "base-host",
			"port": 5432,
		},
	}
	testhelpers.PutEtcdValue(t, endpoint, "test/test-service/base.yaml", baseConfig)

	// Prepare second config (should override first)
	overrideConfig := map[string]interface{}{
		"database": map[string]interface{}{
			"host": "override-host",
		},
		"redis": map[string]interface{}{
			"host": "redis-host",
		},
	}
	testhelpers.PutEtcdValue(t, endpoint, "test/test-service/override.yaml", overrideConfig)

	// Set environment variables
	t.Setenv("APP_CONFIG_ETCD_ENDPOINT", endpoint)
	t.Setenv("APP_CONFIG_ETCD_STATIC_PATHS", "base.yaml,override.yaml")
	t.Setenv("APP_ENV", "test")
	t.Setenv("APP_SERVICE_NAME", "test-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)

	// Verify merge happened correctly
	assert.Equal(t, "override-host", cfg.GetString("database.host"), "should use override value")
	assert.Equal(t, 5432, cfg.GetInt("database.port"), "should keep base value")
	assert.Equal(t, "redis-host", cfg.GetString("redis.host"), "should have new value from override")
}

// TestNewConfig_EtcdDynamic tests loading dynamic configuration from etcd with watching.
func TestNewConfig_EtcdDynamic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration test in short mode")
	}

	endpoint, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	// Prepare initial data
	initialData := map[string]interface{}{
		"limits": map[string]interface{}{
			"rateLimit": 100,
		},
		"app": map[string]interface{}{
			"env": "dev",
			"service": map[string]interface{}{
				"name": "dynamic-service",
			},
		},
	}
	testhelpers.PutEtcdValue(t, endpoint, "test/test-service/dynamic.yaml", initialData)

	// Set environment variables
	t.Setenv("APP_CONFIG_ETCD_ENDPOINT", endpoint)
	t.Setenv("APP_CONFIG_ETCD_DYNAMIC_PATHS", "dynamic.yaml")
	t.Setenv("APP_ENV", "test")
	t.Setenv("APP_SERVICE_NAME", "test-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)

	// Verify initial value
	assert.Equal(t, 100, cfg.GetInt("limits.rateLimit"))

	// Update value in etcd
	updatedData := map[string]interface{}{
		"limits": map[string]interface{}{
			"rateLimit": 200,
		},
		"app": map[string]interface{}{
			"env": "dev",
			"service": map[string]interface{}{
				"name": "dynamic-service",
			},
		},
	}
	testhelpers.PutEtcdValue(t, endpoint, "test/test-service/dynamic.yaml", updatedData)

	// Wait for watch interval + buffer (etcd watches every 5 seconds)
	time.Sleep(7 * time.Second)

	// Verify value was updated
	updatedValue := cfg.GetInt("limits.rateLimit")
	assert.Equal(t, 200, updatedValue, "value should be updated via watching")
}

// TestNewConfig_FullScenario tests complete scenario with files + static etcd + dynamic etcd.
func TestNewConfig_FullScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration test in short mode")
	}

	endpoint, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	// Prepare static etcd config
	staticConfig := map[string]interface{}{
		"database": map[string]interface{}{
			"host": "etcd-static-host",
		},
	}
	testhelpers.PutEtcdValue(t, endpoint, "prod/full-service/static.yaml", staticConfig)

	// Prepare dynamic etcd config
	dynamicConfig := map[string]interface{}{
		"limits": map[string]interface{}{
			"rateLimit": 500,
		},
	}
	testhelpers.PutEtcdValue(t, endpoint, "prod/full-service/dynamic.yaml", dynamicConfig)

	// Set environment variables (not using file config to avoid conflicts)
	t.Setenv("APP_CONFIG_ETCD_ENDPOINT", endpoint)
	t.Setenv("APP_CONFIG_ETCD_STATIC_PATHS", "static.yaml")
	t.Setenv("APP_CONFIG_ETCD_DYNAMIC_PATHS", "dynamic.yaml")
	t.Setenv("APP_ENV", "prod")
	t.Setenv("APP_SERVICE_NAME", "full-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)

	// Verify values from all sources
	// Static etcd
	assert.Equal(t, "etcd-static-host", cfg.GetString("database.host"))
	// Dynamic etcd
	assert.Equal(t, 500, cfg.GetInt("limits.rateLimit"))
	// App info from environment variables (immutable)
	assert.Equal(t, "prod", cfg.GetAppEnv())
	assert.Equal(t, "full-service", cfg.GetServiceName())
}

// TestNewConfig_AppEnvImmutable tests that app.env and app.service.name from environment
// variables remain immutable even when configs try to override them.
func TestNewConfig_AppEnvImmutable(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration test in short mode")
	}

	endpoint, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	// Prepare etcd config that tries to override app.env and app.service.name
	configWithOverrides := map[string]interface{}{
		"database": map[string]interface{}{
			"host": "correct-db-host",
			"port": 5432,
		},
		"app": map[string]interface{}{
			"env": "wrong-env", // ← Should NOT override env var
			"service": map[string]interface{}{
				"name": "wrong-service", // ← Should NOT override env var
			},
		},
	}
	// Store config using CORRECT path (from env vars)
	testhelpers.PutEtcdValue(t, endpoint, "correct-env/correct-service/config.yaml", configWithOverrides)

	// Set environment variables - these should be immutable
	t.Setenv("APP_CONFIG_ETCD_ENDPOINT", endpoint)
	t.Setenv("APP_CONFIG_ETCD_STATIC_PATHS", "config.yaml")
	t.Setenv("APP_ENV", "correct-env")
	t.Setenv("APP_SERVICE_NAME", "correct-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)

	// Verify that app.env and app.service.name from env vars are preserved
	assert.Equal(t, "correct-env", cfg.GetAppEnv(), "app.env should remain immutable from env var")
	assert.Equal(t, "correct-service", cfg.GetServiceName(), "app.service.name should remain immutable from env var")

	// Verify that other config values were loaded correctly
	assert.Equal(t, "correct-db-host", cfg.GetString("database.host"))
	assert.Equal(t, 5432, cfg.GetInt("database.port"))

	// Verify that viper internal state may have changed, but Config uses immutable values
	assert.Equal(t, "wrong-env", cfg.GetStorage().GetString("app.env"),
		"viper may have wrong value internally")
	assert.Equal(t, "wrong-service", cfg.GetStorage().GetString("app.service.name"),
		"viper may have wrong value internally")
}

// TestConfig_Methods tests Config getter methods.
func TestConfig_Methods(t *testing.T) {
	t.Setenv("APP_CONFIG_FILE_PATHS", "./testdata")
	t.Setenv("APP_ENV", "test")
	t.Setenv("APP_SERVICE_NAME", "test-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)

	// Test various getter methods
	t.Run("GetString", func(t *testing.T) { //nolint:paralleltest
		assert.Equal(t, "localhost", cfg.GetString("database.host"))
	})

	t.Run("GetInt", func(t *testing.T) { //nolint:paralleltest
		assert.Equal(t, 5432, cfg.GetInt("database.port"))
	})

	t.Run("GetBool", func(t *testing.T) {
		// Set a boolean value via env for testing
		t.Setenv("FEATURE_ENABLED", "true")
		assert.True(t, cfg.GetBool("feature.enabled"))
	})

	t.Run("GetDuration", func(t *testing.T) { //nolint:paralleltest
		duration := cfg.GetDuration("limits.timeout")
		assert.Equal(t, 30*time.Second, duration)
	})

	t.Run("GetStringOrDefault", func(t *testing.T) { //nolint:paralleltest
		// Existing key
		assert.Equal(t, "localhost", cfg.GetStringOrDefault("database.host", "default"))
		// Non-existing key
		assert.Equal(t, "default", cfg.GetStringOrDefault("nonexistent.key", "default"))
	})

	t.Run("GetAppEnv", func(t *testing.T) { //nolint:paralleltest
		assert.Equal(t, "test", cfg.GetAppEnv())
	})

	t.Run("GetServiceName", func(t *testing.T) { //nolint:paralleltest
		assert.Equal(t, "test-service", cfg.GetServiceName())
	})

	t.Run("GetStorage", func(t *testing.T) { //nolint:paralleltest
		storage := cfg.GetStorage()
		assert.NotNil(t, storage)
		assert.Equal(t, "localhost", storage.GetString("database.host"))
	})
}

// TestConfig_WatchMethods tests Config watch methods.
func TestConfig_WatchMethods(t *testing.T) { //nolint:tparallel
	t.Setenv("APP_CONFIG_FILE_PATHS", "./testdata")
	t.Setenv("APP_ENV", "test")
	t.Setenv("APP_SERVICE_NAME", "test-service")

	cfg, err := di.NewConfig(t.Context())
	require.NoError(t, err)

	t.Run("WatchString", func(t *testing.T) {
		t.Parallel()

		watcher := cfg.WatchString("database.host")
		assert.Equal(t, "localhost", watcher())
	})

	t.Run("WatchInt", func(t *testing.T) {
		t.Parallel()

		watcher := cfg.WatchInt("database.port")
		assert.Equal(t, 5432, watcher())
	})

	t.Run("WatchBool", func(t *testing.T) {
		t.Parallel()

		watcher := cfg.WatchBool("some.bool.key")
		assert.False(t, watcher())
	})

	t.Run("WatchDuration", func(t *testing.T) {
		t.Parallel()

		watcher := cfg.WatchDuration("limits.timeout")
		assert.Equal(t, 30*time.Second, watcher())
	})

	t.Run("WatchStringSlice", func(t *testing.T) {
		t.Parallel()

		watcher := cfg.WatchStringSlice("some.slice.key")
		result := watcher()
		assert.Empty(t, result)
	})
}
