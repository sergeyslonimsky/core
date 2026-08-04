package http2_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/sergeyslonimsky/core/http2"
)

func TestNewClient_Defaults(t *testing.T) {
	t.Parallel()

	client := http2.NewClient()
	require.NotNil(t, client)

	assert.Zero(t, client.Timeout, "Timeout must be left unset so callers rely on context deadlines")

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok, "otel is opt-in: default transport must not be wrapped")
	assert.True(t, transport.ForceAttemptHTTP2, "must force HTTP/2 negotiation despite custom DialContext")
}

func TestNewClient_WithOtel_WrapsTransport(t *testing.T) {
	t.Parallel()

	client := http2.NewClient(http2.WithClientOtel())

	_, ok := client.Transport.(*otelhttp.Transport)
	assert.True(t, ok, "WithClientOtel should wrap the transport with otelhttp instrumentation")
}

func TestNewClient_AppliesTuningOptions(t *testing.T) {
	t.Parallel()

	client := http2.NewClient(
		http2.WithMaxIdleConns(5),
		http2.WithMaxIdleConnsPerHost(2),
		http2.WithDialTimeout(time.Second),
		http2.WithKeepAlive(2*time.Second),
		http2.WithTLSHandshakeTimeout(3*time.Second),
		http2.WithResponseHeaderTimeout(4*time.Second),
		http2.WithIdleConnTimeout(5*time.Second),
	)

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok)

	assert.Equal(t, 5, transport.MaxIdleConns)
	assert.Equal(t, 2, transport.MaxIdleConnsPerHost)
	assert.Equal(t, 3*time.Second, transport.TLSHandshakeTimeout)
	assert.Equal(t, 4*time.Second, transport.ResponseHeaderTimeout)
	assert.Equal(t, 5*time.Second, transport.IdleConnTimeout)
}

func TestNewClient_WithTLSConfig(t *testing.T) {
	t.Parallel()

	cfg := &tls.Config{
		ServerName: "internal.example.com",
	}

	client := http2.NewClient(http2.WithTLSConfig(cfg))

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Same(t, cfg, transport.TLSClientConfig)
}

func TestNewClient_WithTransportOptions(t *testing.T) {
	t.Parallel()

	client := http2.NewClient(http2.WithTransportOptions(func(t *http.Transport) {
		t.DisableCompression = true
	}))

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok)
	assert.True(t, transport.DisableCompression)
}

var errDialBlocked = errors.New("dial intentionally blocked for test")

func TestNewClient_WithRoundTripper_OrderAndComposition(t *testing.T) {
	t.Parallel()

	var order []string

	tag := func(name string) func(http.RoundTripper) http.RoundTripper {
		return func(next http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				order = append(order, name)

				return next.RoundTrip(req)
			})
		}
	}

	client := http2.NewClient(
		http2.WithRoundTripper(tag("outer")),
		http2.WithRoundTripper(tag("inner")),
		http2.WithTransportOptions(func(t *http.Transport) {
			// Fail fast without hitting the network — this test only cares
			// about wrapper ordering, not the actual round trip result.
			t.DialContext = func(context.Context, string, string) (net.Conn, error) {
				return nil, errDialBlocked
			}
		}),
	)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)

	_, _ = client.Do(req)

	assert.Equal(t, []string{"outer", "inner"}, order)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
