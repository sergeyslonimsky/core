package di

import (
	"strings"
	"sync/atomic"
)

// store holds the flat dotted-key snapshot of configuration values together
// with environment lookup and defaults. Reads are lock-free via
// atomic.Pointer; writers replace the pointer atomically with a freshly
// built map.
type store struct {
	snapshot  atomic.Pointer[map[string]any]
	envLookup func(string) (string, bool)
	defaults  map[string]any
}

// newStore constructs a store with the supplied env lookup and defaults.
// The defaults map is retained as-is (callers should not mutate it after
// passing in). The initial snapshot is an empty map.
func newStore(envLookup func(string) (string, bool), defaults map[string]any) *store {
	if envLookup == nil {
		envLookup = func(string) (string, bool) { return "", false }
	}
	if defaults == nil {
		defaults = map[string]any{}
	}
	s := &store{
		envLookup: envLookup,
		defaults:  defaults,
	}
	empty := map[string]any{}
	s.snapshot.Store(&empty)
	return s
}

// setSnapshot atomically replaces the current snapshot with m. The caller
// must not mutate m after calling setSnapshot.
func (s *store) setSnapshot(m map[string]any) {
	if m == nil {
		m = map[string]any{}
	}
	s.snapshot.Store(&m)
}

// lookup resolves key in priority order: env -> snapshot -> defaults.
// Env values are always returned as string. Snapshot/defaults preserve
// their original type. Returns (nil, false) when key is missing in all
// sources.
func (s *store) lookup(key string) (any, bool) {
	if v, ok := s.envLookup(envKey(key)); ok {
		return v, true
	}
	snap := s.snapshot.Load()
	if snap != nil {
		if v, ok := (*snap)[key]; ok {
			return v, true
		}
	}
	if v, ok := s.defaults[key]; ok {
		return v, true
	}
	return nil, false
}

// envKey converts a dotted config key to its env-variable form
// ("a.b.c" -> "A_B_C").
func envKey(key string) string {
	return strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
}

// flatten converts a (potentially) nested map[string]any to a flat
// map keyed by dotted paths. Slices/arrays are kept as-is and not
// descended into. Nested empty maps are dropped (they contribute no
// keys). Nil values are preserved.
func flatten(nested map[string]any) map[string]any {
	out := map[string]any{}
	flattenInto(out, "", nested)
	return out
}

func flattenInto(out map[string]any, prefix string, m map[string]any) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		switch child := v.(type) {
		case map[string]any:
			if len(child) == 0 {
				continue
			}
			flattenInto(out, key, child)
		default:
			out[key] = v
		}
	}
}

// merge deep-merges src into dst returning a new map. Inputs are not
// mutated. For overlapping keys: if both values are map[string]any,
// they are merged recursively; otherwise src wins (including the case
// where src overwrites a map with a scalar or vice versa).
func merge(dst, src map[string]any) map[string]any {
	out := make(map[string]any, len(dst)+len(src))
	for k, v := range dst {
		if m, ok := v.(map[string]any); ok {
			out[k] = cloneMap(m)
		} else {
			out[k] = v
		}
	}
	for k, v := range src {
		existing, hasExisting := out[k]
		dstMap, dstIsMap := existing.(map[string]any)
		srcMap, srcIsMap := v.(map[string]any)
		switch {
		case hasExisting && dstIsMap && srcIsMap:
			out[k] = merge(dstMap, srcMap)
		case srcIsMap:
			out[k] = cloneMap(srcMap)
		default:
			out[k] = v
		}
	}
	return out
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if child, ok := v.(map[string]any); ok {
			out[k] = cloneMap(child)
		} else {
			out[k] = v
		}
	}
	return out
}
