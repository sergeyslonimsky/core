package di

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// Backoff parameters for the dynamic etcd watcher. Exposed at package
// scope so loader_etcd.go and any future watchers share a single source
// of truth.
const (
	// initialWatchBackoff is the first sleep after a watch stream ends.
	initialWatchBackoff = 1 * time.Second
	// maxWatchBackoff caps the backoff after consecutive failures.
	maxWatchBackoff = 30 * time.Second
	// backoffGrowthFactor multiplies the current backoff after a failed
	// resync attempt.
	backoffGrowthFactor = 2
	// configTypeYAML represents the YAML configuration format.
	configTypeYAML = "yaml"
)

// parseCommaSeparatedPaths splits a comma-separated string into a slice
// of trimmed paths. Empty input yields nil. Empty/whitespace-only items
// are dropped.
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

// buildEtcdPath constructs the full etcd path from env, service name,
// and config path.
func buildEtcdPath(appEnv, serviceName, configPath string) string {
	return fmt.Sprintf("%s/%s/%s", appEnv, serviceName, configPath)
}

// getConfigTypeFromPath extracts config type from path (e.g.,
// "config.yaml" -> "yaml"). Defaults to "yaml" when the final path
// element has no extension. Uses filepath.Ext so dots in directory
// names (e.g., "v1.0/config") do not get mistaken for an extension.
func getConfigTypeFromPath(path string) string {
	ext := filepath.Ext(path)
	if ext == "" {
		return configTypeYAML
	}

	return strings.TrimPrefix(ext, ".")
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
