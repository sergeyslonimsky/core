//go:build integration

package testhelpers

import "time"

// Test timeout constants.
const (
	DefaultTestTimeout = 30 * time.Second
	ShortTestTimeout   = 5 * time.Second
	EtcdWatchInterval  = 7 * time.Second
)

// Test configuration constants.
const (
	EtcdPort           = "2379"
	DefaultEtcdPrefix  = "/config"
	TestConfigFileName = "test_config"
)
