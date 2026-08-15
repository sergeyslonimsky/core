package http2

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// errorBodySnippetCap bounds how many bytes of an error response body are
// captured into a span attribute — enough to see a JSON error message
// without risking an unbounded read on a misbehaving upstream.
const errorBodySnippetCap = 512

// upstreamResponseBodySnippetKey is intentionally NOT under the otel
// "http.response.*" semconv namespace — that namespace is reserved for
// semconv-defined keys, and a future semconv release could define
// "http.response.body" and collide. It groups instead with this codebase's
// existing "upstream.class" / "upstream.status_code" attributes.
const upstreamResponseBodySnippetKey = "upstream.response_body_snippet"

// NewErrorBodyRoundTripper wraps next with baseline error visibility for an
// otelhttp-instrumented client: a RecordError event on transport-level
// failures (dial/TLS/timeout), and a capped, UTF-8-safe body snippet
// attribute on HTTP-level errors (status >= 400). It is meant to be passed
// to WithInnerRoundTripper, NOT WithRoundTripper — see that option's doc
// comment for why placement matters here: this wrapper must run WHILE the
// otelhttp client span is still open, which only holds for the inner
// (pre-otel) position.
//
// Deliberately does NOT call span.SetStatus: otelhttp's own client semconv
// already marks any status >= 400 as codes.Error, and that call runs AFTER
// this RoundTripper returns control to otelhttp — a SetStatus call made here
// would just have its description blanked by otelhttp's later call (the SDK
// only refuses to downgrade FROM codes.Error, not same-code overwrites). So
// today every >=400 client span already shows codes.Error with an empty
// description; this wrapper's RecordError/body-snippet are what carry the
// actual diagnostic content, without duplicating (and losing) the status
// itself. Closing the empty-description gap would need a second, OUTER
// wrapper (WithRoundTripper) running after otelhttp's SetStatus — but that
// placement can't see transport-level errors at all (otelhttp has already
// called span.End() by the time an outer wrapper regains control on that
// path), so it is not implemented here.
//
// Domain-level error classification (which upstream, which error class) is
// deliberately NOT this RoundTripper's job — it has no notion of "provider"
// or "model", only raw HTTP. Callers that need that (e.g. a per-provider
// chat-completion client) should keep building their own richer span around
// the call and classify the returned error there; this wrapper is the
// baseline every call gets for free, not a replacement for that.
func NewErrorBodyRoundTripper(next http.RoundTripper) http.RoundTripper {
	return &errorBodyRoundTripper{next: next}
}

type errorBodyRoundTripper struct {
	next http.RoundTripper
}

func (t *errorBodyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(req)
	if err != nil {
		trace.SpanFromContext(req.Context()).RecordError(err)

		//nolint:wrapcheck // RoundTripper contract: return the underlying transport error verbatim, never wrapped
		return resp, err
	}

	if resp.StatusCode < http.StatusBadRequest {
		return resp, nil
	}

	span := trace.SpanFromContext(req.Context())
	if !span.IsRecording() {
		// Pure telemetry: skip the read entirely when nothing will consume
		// it (otel disabled, or this trace wasn't sampled).
		return resp, nil
	}

	if resp.Body == nil || resp.Body == http.NoBody {
		return resp, nil
	}

	// A compressed body would land binary/invalid-UTF-8 bytes in an OTLP
	// string attribute value, risking the whole export batch; a non-text
	// body is unreadable noise either way. Skip capture rather than guess a
	// decoder — the caller's own error path still gets the full body as
	// normal, this wrapper only decides what ALSO goes on the span.
	if resp.Header.Get("Content-Encoding") != "" || !isTextualContentType(resp.Header.Get("Content-Type")) {
		return resp, nil
	}

	snippet, wrapped := peekBody(resp.Body, errorBodySnippetCap)
	resp.Body = wrapped

	if len(snippet) > 0 {
		span.SetAttributes(attribute.String(upstreamResponseBodySnippetKey, strings.ToValidUTF8(string(snippet), "")))
	}

	return resp, nil
}

// peekBody reads up to capBytes from body and returns those bytes alongside
// a replacement io.ReadCloser that reproduces the ENTIRE original stream —
// the peeked prefix followed by whatever body has left — so a caller reading
// the returned body sees byte-identical content to what it would have
// without this wrapper. Close still closes the original body.
func peekBody(body io.ReadCloser, capBytes int) ([]byte, io.ReadCloser) {
	peeked, _ := io.ReadAll(io.LimitReader(body, int64(capBytes)))

	return peeked, struct {
		io.Reader
		io.Closer
	}{Reader: io.MultiReader(bytes.NewReader(peeked), body), Closer: body}
}

// isTextualContentType reports whether a Content-Type is safe to treat as
// text for span-attribute capture: JSON (the overwhelming common case for
// this codebase's upstream error bodies) and plain text. An empty
// Content-Type is common for small error bodies and does not imply binary
// content, so it is treated as textual rather than discarded.
func isTextualContentType(contentType string) bool {
	if contentType == "" {
		return true
	}

	mediaType, _, _ := strings.Cut(contentType, ";")
	mediaType = strings.TrimSpace(mediaType)

	return strings.HasPrefix(mediaType, "text/") ||
		strings.HasSuffix(mediaType, "/json") ||
		strings.HasSuffix(mediaType, "+json")
}
