package lifecycle

import (
	"context"
	"errors"
	"reflect"
)

// Group returns a Resource whose Shutdown calls Shutdown on every non-nil
// resource in resources, in order, joining any errors with errors.Join.
//
// Entries that are nil — including a typed nil pointer stored in the
// Resource interface (e.g. a struct field of concrete pointer type that was
// never assigned) — are silently skipped. This lets callers pass struct
// fields directly, some of which may be nil because of partial
// construction, without a hand-written nil guard per field:
//
//	func (i *Infrastructure) Shutdown(ctx context.Context) error {
//	    return lifecycle.Group(i.Redis, i.DB, i.OTel).Shutdown(ctx)
//	}
//
// Group is intended for aggregating an arbitrary subset of resources into
// one rollback/teardown unit (e.g. a DI construction-failure rollback
// path). It does not replace app.App's LIFO-ordered normal shutdown
// sequence.
func Group(resources ...Resource) Resource {
	return group(resources)
}

type group []Resource

// Shutdown implements Resource.
func (g group) Shutdown(ctx context.Context) error {
	var errs []error

	for _, r := range g {
		if isNilResource(r) {
			continue
		}

		if err := r.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// isNilResource reports whether r is nil, accounting for the classic Go
// gotcha where a nil concrete value (most commonly a nil pointer, e.g.
// (*redis.Client)(nil)) stored in an interface value compares != nil to the
// untyped nil. Covers every kind reflect.IsNil accepts so a nested
// Group() — itself a Resource backed by a slice, not a pointer — is also
// recognized correctly rather than relying on it happening to no-op safely
// on a nil slice receiver.
func isNilResource(r Resource) bool {
	if r == nil {
		return true
	}

	switch v := reflect.ValueOf(r); v.Kind() { //nolint:exhaustive // only kinds that can be nil are relevant
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return v.IsNil()
	default:
		return false
	}
}

// Compile-time check.
var _ Resource = group(nil)
