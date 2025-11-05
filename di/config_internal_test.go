package di

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
			expected:    "myservice/prod/config.yaml",
		},
		{
			name:        "dev environment",
			env:         "dev",
			serviceName: "test-service",
			configPath:  "base.yaml",
			expected:    "test-service/dev/base.yaml",
		},
		{
			name:        "nested config path",
			env:         "staging",
			serviceName: "api",
			configPath:  "infra/postgres.yaml",
			expected:    "api/staging/infra/postgres.yaml",
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
