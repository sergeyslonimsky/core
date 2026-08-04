package http2

import (
	"crypto/tls"
	"net"
	"net/http"
	"slices"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	defaultClientMaxIdleConns          = 100
	defaultClientMaxIdleConnsPerHost   = 20
	defaultClientDialTimeout           = 5 * time.Second
	defaultClientKeepAlive             = 30 * time.Second
	defaultClientTLSHandshakeTimeout   = 5 * time.Second
	defaultClientResponseHeaderTimeout = 10 * time.Second
	defaultClientIdleConnTimeout       = 90 * time.Second

	// defaultClientExpectContinueTimeout matches net/http.DefaultTransport.
	// Only relevant to requests that set an "Expect: 100-continue" header
	// themselves; harmless otherwise.
	defaultClientExpectContinueTimeout = 1 * time.Second
)

type clientOptions struct {
	maxIdleConns          int
	maxIdleConnsPerHost   int
	dialTimeout           time.Duration
	keepAlive             time.Duration
	tlsHandshakeTimeout   time.Duration
	responseHeaderTimeout time.Duration
	idleConnTimeout       time.Duration
	tlsConfig             *tls.Config
	transportMutators     []func(*http.Transport)
	otelEnabled           bool
	otelOpts              []otelhttp.Option
	roundTripperWrappers  []func(http.RoundTripper) http.RoundTripper
}

// ClientOption configures NewClient.
type ClientOption func(*clientOptions)

// WithMaxIdleConns overrides the transport's global idle connection cap.
// Default: 100.
func WithMaxIdleConns(n int) ClientOption {
	return func(o *clientOptions) { o.maxIdleConns = n }
}

// WithMaxIdleConnsPerHost overrides the transport's per-host idle
// connection cap. Default: 20.
func WithMaxIdleConnsPerHost(n int) ClientOption {
	return func(o *clientOptions) { o.maxIdleConnsPerHost = n }
}

// WithDialTimeout overrides the dialer's connect timeout. Default: 5s.
func WithDialTimeout(d time.Duration) ClientOption {
	return func(o *clientOptions) { o.dialTimeout = d }
}

// WithKeepAlive overrides the dialer's TCP keep-alive interval. Default: 30s.
func WithKeepAlive(d time.Duration) ClientOption {
	return func(o *clientOptions) { o.keepAlive = d }
}

// WithTLSHandshakeTimeout overrides the transport's TLS handshake timeout.
// Default: 5s.
func WithTLSHandshakeTimeout(d time.Duration) ClientOption {
	return func(o *clientOptions) { o.tlsHandshakeTimeout = d }
}

// WithResponseHeaderTimeout overrides how long the transport waits for
// response headers after writing the request (including its body).
// Default: 10s. Raise this for slow upstreams (e.g. LLM APIs with high
// time-to-first-byte); the overall call deadline should still come from
// context, not this field.
func WithResponseHeaderTimeout(d time.Duration) ClientOption {
	return func(o *clientOptions) { o.responseHeaderTimeout = d }
}

// WithIdleConnTimeout overrides how long an idle connection is kept in the
// pool before being closed. Default: 90s.
func WithIdleConnTimeout(d time.Duration) ClientOption {
	return func(o *clientOptions) { o.idleConnTimeout = d }
}

// WithTLSConfig sets the transport's TLS client configuration — client
// certificates for mTLS against a partner API, a custom root CA pool for a
// privately-issued server certificate, or (for internal/test environments
// only) InsecureSkipVerify. Default: nil, which uses Go's standard
// certificate verification against the system trust store.
func WithTLSConfig(cfg *tls.Config) ClientOption {
	return func(o *clientOptions) { o.tlsConfig = cfg }
}

// WithTransportOptions applies arbitrary mutator functions to the built
// *http.Transport, in the order given, after every named option
// (WithMaxIdleConns, WithTLSConfig, etc.) has already been applied. Escape
// hatch for the long tail of *http.Transport fields (MaxConnsPerHost,
// DisableCompression, DisableKeepAlives, ...) that don't warrant a
// dedicated named option.
//
// Mutators run before otel instrumentation and RoundTripper wrapping, so
// they only ever see the raw *http.Transport, never the wrapped
// http.RoundTripper chain.
func WithTransportOptions(fns ...func(*http.Transport)) ClientOption {
	return func(o *clientOptions) { o.transportMutators = append(o.transportMutators, fns...) }
}

// WithRoundTripper appends a RoundTripper-wrapping function to the chain,
// for cross-cutting concerns that belong above the transport: injecting an
// Authorization header or API key, HMAC-signing requests, rate limiting,
// custom retry logic. wrap receives the next RoundTripper in the chain
// (closer to the wire) and must return the RoundTripper to use in its
// place.
//
// Chain order mirrors NewServer's WithMiddleware: the first wrapper passed
// is outermost (sees the request first). otel instrumentation (WithClientOtel)
// is always innermost, closest to the transport, so a span covers only the
// actual network round trip — not time spent in these wrappers (e.g. an
// OAuth2 token refresh should get its own span via its own instrumented
// client, not inflate this request's span).
func WithRoundTripper(wrap func(http.RoundTripper) http.RoundTripper) ClientOption {
	return func(o *clientOptions) { o.roundTripperWrappers = append(o.roundTripperWrappers, wrap) }
}

// WithClientOtel enables OpenTelemetry instrumentation of the client's
// transport via otelhttp.NewTransport, using the global TracerProvider and
// MeterProvider — otel.Setup must have been called and registered with
// app.App before NewClient so the providers are non-noop. Named distinctly
// from NewServer's WithOtel only because both Option types live in this
// package and Go disallows overloading; the convention is otherwise
// identical to WithOtel and every other core/* package's opt-in WithOtel:
// disabled unless passed.
//
// otelhttp's default instrumentation records duration and body-size
// histograms tagged with the full default attribute set. Pass
// otelhttp.WithMetricAttributesFn(...) via opts to trim cardinality; see
// the same concern documented on NewServer's WithOtel.
func WithClientOtel(opts ...otelhttp.Option) ClientOption {
	return func(o *clientOptions) {
		o.otelEnabled = true
		o.otelOpts = opts
	}
}

// NewClient builds an *http.Client tuned for outbound service-to-service
// and third-party API calls: a fresh *http.Transport with connection pool
// and timeout fields set to sane defaults, optional TLS configuration and
// RoundTripper wrapping for auth/headers, and optional otel instrumentation
// (see WithClientOtel).
//
// http.Client.Timeout is intentionally left unset — callers are expected to
// bound calls with context.WithTimeout/WithDeadline rather than a
// client-wide wall clock, so that deadlines propagate correctly through
// retries and don't clip a single slow-but-valid call inside a longer
// request chain.
func NewClient(opts ...ClientOption) *http.Client {
	o := &clientOptions{ //nolint:exhaustruct
		maxIdleConns:          defaultClientMaxIdleConns,
		maxIdleConnsPerHost:   defaultClientMaxIdleConnsPerHost,
		dialTimeout:           defaultClientDialTimeout,
		keepAlive:             defaultClientKeepAlive,
		tlsHandshakeTimeout:   defaultClientTLSHandshakeTimeout,
		responseHeaderTimeout: defaultClientResponseHeaderTimeout,
		idleConnTimeout:       defaultClientIdleConnTimeout,
	}
	for _, apply := range opts {
		apply(o)
	}

	transport := &http.Transport{ //nolint:exhaustruct
		Proxy: http.ProxyFromEnvironment,
		//nolint:exhaustruct // remaining net.Dialer fields have valid zero values
		DialContext: (&net.Dialer{
			Timeout:   o.dialTimeout,
			KeepAlive: o.keepAlive,
		}).DialContext,
		// Custom DialContext/TLSClientConfig below make net/http skip its
		// usual automatic HTTP/2 upgrade unless explicitly forced — without
		// this, an "http2" package's own client would silently negotiate
		// HTTP/1.1 only.
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          o.maxIdleConns,
		MaxIdleConnsPerHost:   o.maxIdleConnsPerHost,
		IdleConnTimeout:       o.idleConnTimeout,
		TLSHandshakeTimeout:   o.tlsHandshakeTimeout,
		TLSClientConfig:       o.tlsConfig,
		ResponseHeaderTimeout: o.responseHeaderTimeout,
		ExpectContinueTimeout: defaultClientExpectContinueTimeout,
	}

	for _, mutate := range o.transportMutators {
		mutate(transport)
	}

	var roundTripper http.RoundTripper = transport
	if o.otelEnabled {
		roundTripper = otelhttp.NewTransport(roundTripper, o.otelOpts...)
	}

	for _, wrap := range slices.Backward(o.roundTripperWrappers) {
		roundTripper = wrap(roundTripper)
	}

	//nolint:exhaustruct // Timeout is intentionally left zero; see doc comment
	return &http.Client{Transport: roundTripper}
}
