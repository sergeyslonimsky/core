// Package rabbitmq wraps github.com/furdarius/rabbitroutine with the
// lifecycle contract used across core: typed Config, functional Options,
// Shutdown methods, and explicit Resource/Runner roles.
//
// The package exposes:
//
//   - Connector — low-level dial + reconnect manager (used by both Publisher
//     and ConsumerHost).
//   - Publisher — Resource. Connects in the constructor; reconnect loop
//     runs as an internal background goroutine and stops in Shutdown.
//   - ConsumerHost — Runner. Holds a set of consumers and runs their
//     amqp loops under the same connection.
//   - Consumer[I] — generic per-queue consumer logic (declare + consume +
//     marshal). Attached to a ConsumerHost.
//
// Publisher is a Resource (not a Runner): callers register it for Shutdown
// only, not for Run. ConsumerHost is the Runner that hosts consumer loops.
package rabbitmq

import (
	"fmt"
	"net"
)

// Config describes a RabbitMQ connection. Plain fields, no struct tags —
// consumer apps map their viper keys to fields explicitly inside their own
// config.NewConfig().
type Config struct {
	Host     string
	Port     string
	User     string
	Password string
}

// DSN returns the AMQP URL form of the config. Used internally by Connector
// to dial the broker.
func (c Config) DSN() string {
	return fmt.Sprintf("amqp://%s:%s@%s", c.User, c.Password, net.JoinHostPort(c.Host, c.Port))
}
