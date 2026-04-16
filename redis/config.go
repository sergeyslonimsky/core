package redis

import "net"

// Config describes a single Redis instance to connect to. All fields are
// plain values with no struct tags — consumer apps map their viper keys to
// fields explicitly inside their own config.NewConfig().
type Config struct {
	// Host is the Redis server hostname or IP. Required.
	Host string

	// Port is the Redis server TCP port. Required.
	Port string

	// Password is the AUTH password. Empty means no AUTH.
	Password string

	// DB is the Redis logical database number (0-15 by default). Default 0.
	DB int
}

// addr returns the host:port string used by the underlying go-redis Options.
// Uses net.JoinHostPort so IPv6 literals are properly bracketed
// (e.g. "[::1]:6379", not "::1:6379" which is unparseable).
func (c Config) addr() string {
	return net.JoinHostPort(c.Host, c.Port)
}
