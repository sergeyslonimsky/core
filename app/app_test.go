package app_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/app"
	"github.com/sergeyslonimsky/core/lifecycle"
)

// fakeRunner blocks in Run until the context is cancelled, then returns nil.
// It records the order in which its Shutdown method is invoked relative to
// other fakes that share the same *recorder.
type fakeRunner struct {
	name     string
	rec      *recorder
	runErr   error
	shutdown func(ctx context.Context) error
}

func (f *fakeRunner) Run(ctx context.Context) error {
	<-ctx.Done()

	return f.runErr
}

func (f *fakeRunner) Shutdown(ctx context.Context) error {
	f.rec.record(f.name)

	if f.shutdown != nil {
		return f.shutdown(ctx)
	}

	return nil
}

type fakeResource struct {
	name     string
	rec      *recorder
	shutdown func(ctx context.Context) error
}

func (f *fakeResource) Shutdown(ctx context.Context) error {
	f.rec.record(f.name)

	if f.shutdown != nil {
		return f.shutdown(ctx)
	}

	return nil
}

type fakeHealthchecker struct {
	err error
}

func (f *fakeHealthchecker) Shutdown(context.Context) error { return nil }

func (f *fakeHealthchecker) Healthcheck(context.Context) error { return f.err }

type recorder struct {
	mu    sync.Mutex
	order []string
}

func (r *recorder) record(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.order = append(r.order, name)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]string, len(r.order))
	copy(out, r.order)

	return out
}

func TestApp_RunnerErrorTriggersShutdown_LIFO(t *testing.T) {
	t.Parallel()

	rec := &recorder{}

	a := app.New(
		app.WithSignals(),
		app.WithShutdownTimeout(2*time.Second),
	)
	a.Add(
		&fakeResource{name: "res1", rec: rec},
		&fakeResource{name: "res2", rec: rec},
	)
	a.Add(
		&fakeRunner{name: "run1", rec: rec},
		&immediateErrorRunner{name: "run2", rec: rec},
	)

	err := a.Run()
	require.Error(t, err)

	order := rec.snapshot()
	// Shutdown order: runners LIFO first, then resources LIFO.
	// Registration: [res1, res2] resources, [run1, run2] runners.
	// Expected: run2, run1, res2, res1.
	assert.Equal(t, []string{"run2", "run1", "res2", "res1"}, order)
}

// immediateErrorRunner returns an error from Run without blocking, used to
// trigger App's shutdown phase deterministically in tests.
type immediateErrorRunner struct {
	name string
	rec  *recorder
}

func (r *immediateErrorRunner) Run(context.Context) error {
	return errors.New("immediate error from " + r.name)
}

func (r *immediateErrorRunner) Shutdown(context.Context) error {
	r.rec.record(r.name)

	return nil
}

func TestApp_HealthcheckAggregation(t *testing.T) {
	t.Parallel()

	healthy := &fakeHealthchecker{err: nil}
	failing := &fakeHealthchecker{err: errors.New("db down")}
	plain := &fakeResource{name: "plain", rec: &recorder{}}

	a := app.New(app.WithSignals())
	a.Add(healthy, failing, plain)

	err := a.Healthcheck(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db down")
}

func TestApp_HealthcheckAllHealthy(t *testing.T) {
	t.Parallel()

	a := app.New(app.WithSignals())
	a.Add(&fakeHealthchecker{err: nil})

	assert.NoError(t, a.Healthcheck(context.Background()))
}

func TestApp_Add_PanicsOnInvalidType(t *testing.T) {
	t.Parallel()

	a := app.New(app.WithSignals())

	defer func() {
		r := recover()
		require.NotNil(t, r)
		msg := fmt.Sprint(r)
		assert.Contains(t, msg, "implements neither")
	}()

	a.Add("not a runner or resource")
}

func TestApp_Add_PanicsAfterRun(t *testing.T) {
	t.Parallel()

	a := app.New(
		app.WithSignals(),
		app.WithShutdownTimeout(time.Second),
	)
	a.Add(&immediateErrorRunner{name: "r", rec: &recorder{}})
	_ = a.Run()

	defer func() {
		r := recover()
		require.NotNil(t, r)
		assert.Contains(t, fmt.Sprint(r), "called after Run")
	}()

	a.Add(&fakeResource{name: "late", rec: &recorder{}})
}

func TestApp_ShutdownTimeout_ForcesExit(t *testing.T) {
	t.Parallel()

	// Resource whose Shutdown blocks until ctx expires.
	var slowShutdownCalls atomic.Int32

	slowRes := &fakeResource{
		name: "slow",
		rec:  &recorder{},
		shutdown: func(ctx context.Context) error {
			slowShutdownCalls.Add(1)
			<-ctx.Done()

			return ctx.Err()
		},
	}

	a := app.New(
		app.WithSignals(),
		app.WithShutdownTimeout(100*time.Millisecond),
	)
	a.Add(slowRes)
	a.Add(&immediateErrorRunner{name: "r", rec: &recorder{}})

	start := time.Now()
	err := a.Run()
	elapsed := time.Since(start)

	// Shutdown should bail out at ~100ms, not hang forever.
	assert.Less(t, elapsed, time.Second)
	require.Error(t, err)
	assert.Equal(t, int32(1), slowShutdownCalls.Load())
}

func TestApp_Logger_DefaultsToSlogDefault(t *testing.T) {
	t.Parallel()

	a := app.New(app.WithSignals())
	assert.NotNil(t, a.Logger())
}

// Compile-time interface assertions to keep contracts in sync with the
// lifecycle package.
var (
	_ lifecycle.Resource      = (*fakeResource)(nil)
	_ lifecycle.Runner        = (*fakeRunner)(nil)
	_ lifecycle.Healthchecker = (*fakeHealthchecker)(nil)
	_ lifecycle.Runner        = (*immediateErrorRunner)(nil)
)
