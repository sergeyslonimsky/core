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

type ctxKey struct{}

// orderedResource records its name into a shared slice on Shutdown, and
// asserts the ctx passed by Group still carries the value Shutdown was
// originally invoked with — proving Group forwards ctx unmodified rather
// than substituting its own.
type orderedResource struct {
	name  string
	order *[]string
}

func (r *orderedResource) Shutdown(ctx context.Context) error {
	if v, _ := ctx.Value(ctxKey{}).(string); v != "propagated" {
		return errors.New("ctx value not propagated to resource")
	}

	*r.order = append(*r.order, r.name)

	return nil
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

func TestGroup_ShutsDownInOrderAndForwardsCtx(t *testing.T) {
	t.Parallel()

	var order []string

	a := &orderedResource{name: "a", order: &order}
	b := &orderedResource{name: "b", order: &order}
	c := &orderedResource{name: "c", order: &order}

	ctx := context.WithValue(t.Context(), ctxKey{}, "propagated")

	err := lifecycle.Group(a, b, c).Shutdown(ctx)

	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, order)
}

func TestGroup_SkipsEmptyNestedGroup(t *testing.T) {
	t.Parallel()

	a := &fakeResource{}

	// An empty nested Group() is itself a Resource backed by a nil slice,
	// not a pointer — isNilResource must still recognize it as nil so it
	// isn't relied upon to merely no-op safely by accident.
	err := lifecycle.Group(a, lifecycle.Group()).Shutdown(t.Context())

	require.NoError(t, err)
	assert.True(t, a.called)
}

func TestGroup_ShutsDownNonEmptyNestedGroup(t *testing.T) {
	t.Parallel()

	inner := &fakeResource{}
	outer := &fakeResource{}

	err := lifecycle.Group(outer, lifecycle.Group(inner)).Shutdown(t.Context())

	require.NoError(t, err)
	assert.True(t, outer.called)
	assert.True(t, inner.called)
}
