package lifecycle_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergeyslonimsky/core/lifecycle"
)

type fakeResource struct {
	shutdownErr error
	called      bool
}

func (f *fakeResource) Shutdown(context.Context) error {
	f.called = true

	return f.shutdownErr
}

func TestGroup_ShutsDownEveryResource(t *testing.T) {
	t.Parallel()

	a := &fakeResource{}
	b := &fakeResource{}

	err := lifecycle.Group(a, b).Shutdown(t.Context())

	require.NoError(t, err)
	assert.True(t, a.called)
	assert.True(t, b.called)
}

func TestGroup_JoinsErrors(t *testing.T) {
	t.Parallel()

	errA := errors.New("a failed")
	errB := errors.New("b failed")

	a := &fakeResource{shutdownErr: errA}
	b := &fakeResource{shutdownErr: errB}

	err := lifecycle.Group(a, b).Shutdown(t.Context())

	require.Error(t, err)
	require.ErrorIs(t, err, errA)
	require.ErrorIs(t, err, errB)
}

func TestGroup_SkipsUntypedNil(t *testing.T) {
	t.Parallel()

	a := &fakeResource{}

	err := lifecycle.Group(a, nil).Shutdown(t.Context())

	require.NoError(t, err)
	assert.True(t, a.called)
}

func TestGroup_SkipsTypedNilPointer(t *testing.T) {
	t.Parallel()

	var nilResource *fakeResource

	a := &fakeResource{}

	// nilResource is a nil *fakeResource stored in the Resource interface —
	// this is the classic Go gotcha where the interface value itself is
	// non-nil even though the underlying pointer is nil. Group must not
	// call Shutdown on it (that would nil-pointer-deref inside a real
	// implementation's method body).
	err := lifecycle.Group(a, nilResource).Shutdown(t.Context())

	require.NoError(t, err)
	assert.True(t, a.called)
}

func TestGroup_Empty(t *testing.T) {
	t.Parallel()

	err := lifecycle.Group().Shutdown(t.Context())

	require.NoError(t, err)
}
