package http2

import (
	"errors"
	"net"
	"net/http"
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
)

// ErrInvalidTransport is returned by NewClient if http.DefaultTransport is
// not a *http.Transport. The stdlib guarantees this in practice, but
// NewClient checks rather than panics so a future runtime change fails
// loudly instead of crashing at a type assertion deep in a client call.
var ErrInvalidTransport = errors.New("http2: http.DefaultTransport is not *http.Transport")

type clientOptions struct {
	maxIdleConns          int
	maxIdleConnsPerHost   int
	dialTimeout           time.Duration
	keepAlive             time.Duration
	tlsHandshakeTimeout   time.Duration
	responseHeaderTimeout time.Duration
	idleConnTimeout       time.Duration
	otelDisabled          bool
	otelOpts              []otelhttp.Option
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

// WithClientOtelOptions passes otelhttp.Option values through to
// otelhttp.NewTransport, e.g. otelhttp.WithMetricAttributesFn(...) to trim
// outbound metric cardinality. No effect if combined with WithoutOtel.
func WithClientOtelOptions(opts ...otelhttp.Option) ClientOption {
	return func(o *clientOptions) { o.otelOpts = opts }
}

// WithoutOtel disables otelhttp instrumentation of the returned client's
// transport. Instrumentation is enabled by default, mirroring the
// always-traced expectation set by the server side (WithOtel on
// NewServer).
func WithoutOtel() ClientOption {
	return func(o *clientOptions) { o.otelDisabled = true }
}

// NewClient builds an *http.Client tuned for outbound service-to-service
// calls: a cloned http.DefaultTransport with connection pool and timeout
// fields set to sane defaults, wrapped in otelhttp.NewTransport for tracing
// and metrics.
//
// http.Client.Timeout is intentionally left unset — callers are expected to
// bound calls with context.WithTimeout/WithDeadline rather than a
// client-wide wall clock, so that deadlines propagate correctly through
// retries and don't clip a single slow-but-valid call inside a longer
// request chain.
//
// Returns ErrInvalidTransport if http.DefaultTransport is not a
// *http.Transport (not expected on any supported Go runtime).
func NewClient(opts ...ClientOption) (*http.Client, error) {
	o := &clientOptions{
		maxIdleConns:          defaultClientMaxIdleConns,
		maxIdleConnsPerHost:   defaultClientMaxIdleConnsPerHost,
		dialTimeout:           defaultClientDialTimeout,
		keepAlive:             defaultClientKeepAlive,
		tlsHandshakeTimeout:   defaultClientTLSHandshakeTimeout,
		responseHeaderTimeout: defaultClientResponseHeaderTimeout,
		idleConnTimeout:       defaultClientIdleConnTimeout,
		otelDisabled:          false,
		otelOpts:              nil,
	}
	for _, apply := range opts {
		apply(o)
	}

	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, ErrInvalidTransport
	}

	transport := defaultTransport.Clone()
	transport.MaxIdleConns = o.maxIdleConns
	transport.MaxIdleConnsPerHost = o.maxIdleConnsPerHost
	//nolint:exhaustruct // remaining net.Dialer fields have valid zero values
	transport.DialContext = (&net.Dialer{
		Timeout:   o.dialTimeout,
		KeepAlive: o.keepAlive,
	}).DialContext
	transport.TLSHandshakeTimeout = o.tlsHandshakeTimeout
	transport.ResponseHeaderTimeout = o.responseHeaderTimeout
	transport.IdleConnTimeout = o.idleConnTimeout

	var roundTripper http.RoundTripper = transport
	if !o.otelDisabled {
		roundTripper = otelhttp.NewTransport(transport, o.otelOpts...)
	}

	//nolint:exhaustruct // Timeout is intentionally left zero; see doc comment
	return &http.Client{Transport: roundTripper}, nil
}
