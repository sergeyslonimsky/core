package http2_test

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/sergeyslonimsky/core/http2"
)

func newResponse(statusCode int, contentType, contentEncoding, body string) *http.Response {
	header := make(http.Header)
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}

	if contentEncoding != "" {
		header.Set("Content-Encoding", contentEncoding)
	}

	return &http.Response{
		StatusCode: statusCode,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestErrorBodyRoundTripper_NetworkError_PassesThroughUnchanged(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("dial failed")
	rt := http2.NewErrorBodyRoundTripper(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, wantErr
	}))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)

	resp, err := rt.RoundTrip(req)

	assert.Nil(t, resp)
	assert.Same(t, wantErr, err, "the wrapper must not swallow or rewrap the transport error")
}

func TestErrorBodyRoundTripper_SuccessStatus_BodyUntouched(t *testing.T) {
	t.Parallel()

	const wantBody = `{"result":"ok"}`

	rt := http2.NewErrorBodyRoundTripper(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, "application/json", "", wantBody), nil
	}))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)

	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.JSONEq(t, wantBody, string(got))
}

func TestErrorBodyRoundTripper_ErrorStatus_BodyRemainsByteIdentical(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "empty", body: ""},
		{name: "shorter than cap", body: `{"error":"bad request"}`},
		{name: "exactly at cap", body: strings.Repeat("a", 512)},
		{name: "longer than cap", body: strings.Repeat("a", 512) + strings.Repeat("b", 4096)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt := http2.NewErrorBodyRoundTripper(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return newResponse(http.StatusInternalServerError, "application/json", "", tc.body), nil
			}))

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.invalid", nil)
			require.NoError(t, err)

			resp, err := rt.RoundTrip(req)
			require.NoError(t, err)

			got, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, tc.body, string(got), "downstream must see the exact original bytes, peeked or not")
			require.NoError(t, resp.Body.Close())
		})
	}
}

func TestErrorBodyRoundTripper_ClosePropagatesToOriginalBody(t *testing.T) {
	t.Parallel()

	closed := false
	original := readCloserFunc{
		Reader: strings.NewReader(`{"error":"boom"}`),
		readFn: nil,
		closeFn: func() error {
			closed = true

			return nil
		},
	}

	rt := http2.NewErrorBodyRoundTripper(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       original,
		}, nil
	}))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)

	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)

	require.NoError(t, resp.Body.Close())
	assert.True(t, closed, "closing the returned body must close the original")
}

// newTestTracer builds an in-memory SDK tracer plus its span recorder, so a
// test can assert on real span attributes/events instead of mocking the
// trace.Span interface.
func newTestTracer() (trace.Tracer, *tracetest.SpanRecorder) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	return tp.Tracer("test"), recorder
}

func TestErrorBodyRoundTripper_NetworkError_RecordsExceptionEvent(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer()

	wantErr := errors.New("dial failed")
	rt := http2.NewErrorBodyRoundTripper(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, wantErr
	}))

	ctx, span := tracer.Start(t.Context(), "test-span")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)

	_, _ = rt.RoundTrip(req)

	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	events := spans[0].Events()
	require.Len(t, events, 1)
	assert.Equal(t, "exception", events[0].Name)
}

func TestErrorBodyRoundTripper_HTTPErrorStatus_RecordsBodySnippetAttribute(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer()

	const wantBody = `{"error":"insufficient_quota"}`

	rt := http2.NewErrorBodyRoundTripper(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return newResponse(http.StatusBadRequest, "application/json", "", wantBody), nil
	}))

	ctx, span := tracer.Start(t.Context(), "test-span")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)

	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)

	_, _ = io.ReadAll(resp.Body)

	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	attrs := spans[0].Attributes()
	found := false

	for _, a := range attrs {
		if string(a.Key) == "upstream.response_body_snippet" {
			found = true

			assert.JSONEq(t, wantBody, a.Value.AsString())
		}

		assert.NotEqual(t,
			"http.response.body_snippet", string(a.Key),
			"must not use the reserved http.response.* semconv namespace",
		)
	}

	assert.True(t, found, "expected upstream.response_body_snippet attribute")
}

func TestErrorBodyRoundTripper_SkipsBodyCapture(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		contentType     string
		contentEncoding string
		reason          string
	}{
		{
			name:            "gzip-encoded body",
			contentType:     "application/json",
			contentEncoding: "gzip",
			reason:          "gzip-encoded bodies must not be captured raw",
		},
		{
			name:            "binary content type",
			contentType:     "application/octet-stream",
			contentEncoding: "",
			reason:          "non-text content types must not be captured",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tracer, recorder := newTestTracer()

			rt := http2.NewErrorBodyRoundTripper(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return newResponse(http.StatusInternalServerError, tc.contentType, tc.contentEncoding, "payload"), nil
			}))

			ctx, span := tracer.Start(t.Context(), "test-span")
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.invalid", nil)
			require.NoError(t, err)

			resp, err := rt.RoundTrip(req)
			require.NoError(t, err)

			_, _ = io.ReadAll(resp.Body)

			span.End()

			spans := recorder.Ended()
			require.Len(t, spans, 1)

			for _, a := range spans[0].Attributes() {
				assert.NotEqual(t, "upstream.response_body_snippet", string(a.Key), tc.reason)
			}
		})
	}
}

func TestErrorBodyRoundTripper_UnsampledSpan_DoesNotReadBody(t *testing.T) {
	t.Parallel()

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	tracer := tp.Tracer("test")

	readAttempted := false
	body := readCloserFunc{
		Reader: strings.NewReader(`{"error":"boom"}`),
		readFn: func(_ []byte) (int, error) {
			readAttempted = true

			return 0, io.EOF
		},
		closeFn: func() error { return nil },
	}

	rt := http2.NewErrorBodyRoundTripper(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       body,
		}, nil
	}))

	ctx, span := tracer.Start(t.Context(), "test-span")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)

	_, err = rt.RoundTrip(req)
	require.NoError(t, err)

	span.End()

	assert.False(t, readAttempted, "must not read the body when the span is not recording")
}

// roundTripFunc is defined in client_test.go.

type readCloserFunc struct {
	io.Reader

	readFn  func([]byte) (int, error)
	closeFn func() error
}

func (r readCloserFunc) Read(p []byte) (int, error) {
	if r.readFn != nil {
		return r.readFn(p)
	}

	n, err := r.Reader.Read(p)

	//nolint:wrapcheck // test double implementing io.Reader; must return the underlying error verbatim
	return n, err
}

func (r readCloserFunc) Close() error {
	return r.closeFn()
}
