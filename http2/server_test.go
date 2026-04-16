package http2_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/http2"
	"github.com/sergeyslonimsky/core/lifecycle"
)

// helperListener allocates a random port and returns the listener + the
// resolved "http://127.0.0.1:<port>" base URL for HTTP requests.
func helperListener(t *testing.T) (net.Listener, string) {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	return l, "http://" + l.Addr().String()
}

// runServer starts srv.Run in a goroutine and returns a cancel func and an
// error channel that delivers the Run result.
func runServer(t *testing.T, srv *http2.Server) (func(), chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Run(ctx) }()

	// Wait briefly for the accept loop to start.
	time.Sleep(25 * time.Millisecond)

	return cancel, errCh
}

// doGet is a test helper that issues an HTTP GET with context and returns the
// response. Replaces bare http.Get (which the noctx linter rejects).
func doGet(ctx context.Context, t *testing.T, url string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func TestServer_Livez(t *testing.T) {
	t.Parallel()

	l, base := helperListener(t)
	srv := http2.NewServer(http2.Config{}, http2.WithListener(l))

	cancel, errCh := runServer(t, srv)

	defer func() { cancel(); <-errCh }()

	resp := doGet(context.Background(), t, base+http2.LivenessPath)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestServer_Readyz_NoHealthcheck_Returns200(t *testing.T) {
	t.Parallel()

	l, base := helperListener(t)
	srv := http2.NewServer(http2.Config{}, http2.WithListener(l))

	cancel, errCh := runServer(t, srv)

	defer func() { cancel(); <-errCh }()

	resp := doGet(context.Background(), t, base+http2.ReadinessPath)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

type fakeHC struct{ err error }

func (f *fakeHC) Healthcheck(context.Context) error { return f.err }

func TestServer_Readyz_HealthcheckFailure_Returns503(t *testing.T) {
	t.Parallel()

	l, base := helperListener(t)
	hc := &fakeHC{err: errors.New("db down")}

	srv := http2.NewServer(http2.Config{},
		http2.WithListener(l),
		http2.WithHealthcheckFrom(hc),
	)

	cancel, errCh := runServer(t, srv)

	defer func() { cancel(); <-errCh }()

	resp := doGet(context.Background(), t, base+http2.ReadinessPath)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "db down")
}

func TestServer_ExtraReadyzCheck_Aggregates(t *testing.T) {
	t.Parallel()

	l, base := helperListener(t)
	srv := http2.NewServer(http2.Config{},
		http2.WithListener(l),
		http2.WithReadyzCheck("cache", func(context.Context) error {
			return errors.New("cold")
		}),
	)

	cancel, errCh := runServer(t, srv)

	defer func() { cancel(); <-errCh }()

	resp := doGet(context.Background(), t, base+http2.ReadinessPath)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "cache: cold")
}

func TestServer_Metrics_Returns404WithoutHandler(t *testing.T) {
	t.Parallel()

	l, base := helperListener(t)
	srv := http2.NewServer(http2.Config{}, http2.WithListener(l))

	cancel, errCh := runServer(t, srv)

	defer func() { cancel(); <-errCh }()

	resp := doGet(context.Background(), t, base+http2.MetricsPath)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestServer_Metrics_WithHandler(t *testing.T) {
	t.Parallel()

	l, base := helperListener(t)
	srv := http2.NewServer(http2.Config{},
		http2.WithListener(l),
		http2.WithMetricsHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("# HELP metrics_sample dummy\n"))
		})),
	)

	cancel, errCh := runServer(t, srv)

	defer func() { cancel(); <-errCh }()

	resp := doGet(context.Background(), t, base+http2.MetricsPath)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "metrics_sample")
}

func TestServer_Recovery_CatchesPanic(t *testing.T) {
	t.Parallel()

	l, base := helperListener(t)
	srv := http2.NewServer(http2.Config{},
		http2.WithListener(l),
		http2.WithRecovery(),
	)
	srv.Mount("/panic", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	cancel, errCh := runServer(t, srv)

	defer func() { cancel(); <-errCh }()

	resp := doGet(context.Background(), t, base+"/panic")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestServer_Mount_RegistersUserHandler(t *testing.T) {
	t.Parallel()

	l, base := helperListener(t)
	srv := http2.NewServer(http2.Config{}, http2.WithListener(l))
	srv.Mount("/hello", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("world"))
	}))

	cancel, errCh := runServer(t, srv)

	defer func() { cancel(); <-errCh }()

	resp := doGet(context.Background(), t, base+"/hello")
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "world", string(body))
}

func TestServer_Shutdown_FreshContext_DoesNotShortCircuitOnCancelledRunCtx(t *testing.T) {
	t.Parallel()

	l, base := helperListener(t)

	srv := http2.NewServer(http2.Config{
		WriteTimeout:    5 * time.Second,
		ShutdownTimeout: 2 * time.Second,
	}, http2.WithListener(l))

	handlerStarted := make(chan struct{})

	srv.Mount("/slow", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(handlerStarted)
		time.Sleep(200 * time.Millisecond)

		_, _ = w.Write([]byte("done"))
	}))

	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)

	go func() { runErrCh <- srv.Run(ctx) }()

	time.Sleep(25 * time.Millisecond)

	// Fire the slow request.
	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)

	go func() {
		req, reqErr := http.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			base+"/slow",
			nil,
		)
		if reqErr != nil {
			errCh <- reqErr

			return
		}

		r, doErr := http.DefaultClient.Do(req)
		if doErr != nil {
			errCh <- doErr

			return
		}

		respCh <- r
	}()

	// Wait until the handler has started, then cancel.
	<-handlerStarted
	cancel()

	// Expect the slow response to still complete successfully.
	select {
	case resp := <-respCh:
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "done", string(body))
	case err := <-errCh:
		require.NoError(t, err, "in-flight request failed (shutdown did not respect fresh timeout)")
	case <-time.After(3 * time.Second):
		t.Fatal("request hung past ShutdownTimeout")
	}

	select {
	case err := <-runErrCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after graceful shutdown")
	}
}

func TestServer_Middleware_OrderOutermostFirst(t *testing.T) {
	t.Parallel()

	l, base := helperListener(t)

	var order strings.Builder

	mkMW := func(tag string) func(http.Handler) http.Handler {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order.WriteString(tag + ">")
				next.ServeHTTP(w, r)
				order.WriteString("<" + tag)
			})
		}
	}

	srv := http2.NewServer(http2.Config{},
		http2.WithListener(l),
		http2.WithMiddleware(mkMW("A"), mkMW("B")),
	)
	srv.Mount("/t", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		order.WriteString("handler")
		w.WriteHeader(http.StatusOK)
	}))

	cancel, errCh := runServer(t, srv)

	defer func() { cancel(); <-errCh }()

	resp := doGet(context.Background(), t, base+"/t")
	defer resp.Body.Close()

	// A is outermost: enter A, enter B, handler, exit B, exit A.
	assert.Equal(t, "A>B>handler<B<A", order.String())
}

// Compile-time assertions.
var (
	_ lifecycle.Runner   = (*http2.Server)(nil)
	_ lifecycle.Resource = (*http2.Server)(nil)
)
