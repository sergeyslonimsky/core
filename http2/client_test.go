package http2_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/http2"
)

func TestNewClient_Defaults(t *testing.T) {
	t.Parallel()

	client, err := http2.NewClient()
	require.NoError(t, err)
	require.NotNil(t, client)

	assert.Zero(t, client.Timeout, "Timeout must be left unset so callers rely on context deadlines")
	assert.NotNil(t, client.Transport)
}

func TestNewClient_WithoutOtel_ReturnsPlainTransport(t *testing.T) {
	t.Parallel()

	client, err := http2.NewClient(http2.WithoutOtel())
	require.NoError(t, err)

	_, ok := client.Transport.(*http.Transport)
	assert.True(t, ok, "WithoutOtel should leave the *http.Transport unwrapped")
}

func TestNewClient_WithOtel_WrapsTransport(t *testing.T) {
	t.Parallel()

	client, err := http2.NewClient()
	require.NoError(t, err)

	_, ok := client.Transport.(*http.Transport)
	assert.False(t, ok, "default should wrap the transport with otelhttp instrumentation")
}

func TestNewClient_AppliesTuningOptions(t *testing.T) {
	t.Parallel()

	client, err := http2.NewClient(
		http2.WithoutOtel(),
		http2.WithMaxIdleConns(5),
		http2.WithMaxIdleConnsPerHost(2),
		http2.WithDialTimeout(time.Second),
		http2.WithKeepAlive(2*time.Second),
		http2.WithTLSHandshakeTimeout(3*time.Second),
		http2.WithResponseHeaderTimeout(4*time.Second),
		http2.WithIdleConnTimeout(5*time.Second),
	)
	require.NoError(t, err)

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok)

	assert.Equal(t, 5, transport.MaxIdleConns)
	assert.Equal(t, 2, transport.MaxIdleConnsPerHost)
	assert.Equal(t, 3*time.Second, transport.TLSHandshakeTimeout)
	assert.Equal(t, 4*time.Second, transport.ResponseHeaderTimeout)
	assert.Equal(t, 5*time.Second, transport.IdleConnTimeout)
}
