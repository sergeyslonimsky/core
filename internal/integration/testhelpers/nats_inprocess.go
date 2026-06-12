package testhelpers

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
)

// natsServerStartTimeout bounds in-process NATS startup. Generous so CI hosts
// under load don't flake; the typical local startup is well under a second.
const natsServerStartTimeout = 5 * time.Second

// RunInProcessNATS starts an in-process NATS server with JetStream enabled on
// a random free port and returns its client URL. The server is shut down via
// t.Cleanup; the JetStream storage directory uses t.TempDir() so it's
// removed automatically when the test ends.
//
// Use for tests that need a live NATS server but want millisecond startup
// rather than the multi-second cost of a testcontainers container.
func RunInProcessNATS(t *testing.T) string {
	t.Helper()

	storeDir := t.TempDir()

	opts := &server.Options{ //nolint:exhaustruct
		Host:      "127.0.0.1",
		Port:      -1, // random free port
		JetStream: true,
		StoreDir:  storeDir,
		NoLog:     true,
		NoSigs:    true,
	}

	ns, err := server.NewServer(opts)
	require.NoError(t, err, "create in-process NATS server")

	go ns.Start()

	if !ns.ReadyForConnections(natsServerStartTimeout) {
		ns.Shutdown()
		t.Fatal("in-process NATS server failed to become ready")
	}

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	return ns.ClientURL()
}
