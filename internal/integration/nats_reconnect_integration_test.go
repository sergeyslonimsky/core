package integration_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corenats "github.com/sergeyslonimsky/core/nats"
)

// startServerOnPort boots an in-process NATS server on the given port. Used
// by the reconnect tests where the URL must remain stable across a restart.
func startServerOnPort(t *testing.T, port int) (*server.Server, string) {
	t.Helper()

	storeDir := t.TempDir()

	opts := &server.Options{ //nolint:exhaustruct
		Host:      "127.0.0.1",
		Port:      port,
		JetStream: true,
		StoreDir:  storeDir,
		NoLog:     true,
		NoSigs:    true,
	}

	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	go ns.Start()

	require.True(t, ns.ReadyForConnections(5*time.Second), "server did not start on port %d", port)

	return ns, ns.ClientURL()
}

// TestNATS_ReconnectMidRun verifies that a Subscriber survives a server
// restart on the same port — connection drops, reconnects, subscription
// is re-established by the NATS client.
func TestNATS_ReconnectMidRun(t *testing.T) {
	t.Parallel()

	// Random port via -1, then re-bind to the same port on restart.
	ns1, url := startServerOnPort(t, -1)

	cfg := corenats.Config{ //nolint:exhaustruct
		URL:           url,
		ReconnectWait: 100 * time.Millisecond,
		DialTimeout:   500 * time.Millisecond,
	}
	subject := "test.reconnect"

	rec := &natsRecorder{} //nolint:exhaustruct
	subscriber, err := corenats.NewSubscriber[natsEvent](
		cfg, subject, rec, corenats.SubscriberHandlers[natsEvent]{},
	)
	require.NoError(t, err)

	runNATSRunner(t, subscriber)

	publisher, err := corenats.NewPublisher(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Shutdown(t.Context()) })

	require.NoError(t, publisher.Flush(t.Context()))
	require.NoError(t, publisher.PublishJSON(t.Context(), subject, natsEvent{ID: 1, Text: "before"}))

	require.Eventually(t, func() bool { return rec.Count() == 1 }, natsEventually, natsTick)

	// Bounce the server.
	ns1.Shutdown()
	ns1.WaitForShutdown()

	// Parse the original URL to pull the port.
	hostPort, ok := parseNATSURL(url)
	require.True(t, ok)

	ns2, _ := startServerOnPort(t, hostPort)
	t.Cleanup(func() { ns2.Shutdown() })

	// Wait until both publisher and subscriber reconnect.
	require.Eventually(t, func() bool {
		return publisher.Conn().Status() == nats.CONNECTED &&
			subscriber.Conn().Status() == nats.CONNECTED
	}, 10*time.Second, 100*time.Millisecond)

	// Sanity: publish goes through on the new server.
	require.NoError(t, publisher.PublishJSON(t.Context(), subject, natsEvent{ID: 2, Text: "after"}))

	require.Eventually(t, func() bool { return rec.Count() == 2 }, natsEventually, natsTick)

	snap := rec.Snapshot()
	assert.Equal(t, "before", snap[0].Text)
	assert.Equal(t, "after", snap[1].Text)
}

// parseNATSURL extracts the port number from a "nats://host:port" URL.
// Returns 0,false on parse failure.
func parseNATSURL(url string) (int, bool) {
	// Cheap parse: nats://127.0.0.1:NNNNN
	const prefix = "nats://"
	if len(url) < len(prefix) {
		return 0, false
	}

	rest := url[len(prefix):]

	colonAt := -1

	for i := len(rest) - 1; i >= 0; i-- {
		if rest[i] == ':' {
			colonAt = i

			break
		}
	}

	if colonAt < 0 {
		return 0, false
	}

	port := 0

	for _, c := range rest[colonAt+1:] {
		if c < '0' || c > '9' {
			return 0, false
		}

		port = port*10 + int(c-'0')
	}

	return port, true
}
