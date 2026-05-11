package di

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

func TestFlatten(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]any
		want map[string]any
	}{
		{
			name: "empty map",
			in:   map[string]any{},
			want: map[string]any{},
		},
		{
			name: "flat one level",
			in:   map[string]any{"a": 1, "b": "x"},
			want: map[string]any{"a": 1, "b": "x"},
		},
		{
			name: "nested three levels",
			in: map[string]any{
				"a": map[string]any{
					"b": map[string]any{
						"c": 42,
					},
					"d": "hi",
				},
				"e": true,
			},
			want: map[string]any{
				"a.b.c": 42,
				"a.d":   "hi",
				"e":     true,
			},
		},
		{
			name: "nil values preserved",
			in:   map[string]any{"a": nil, "b": map[string]any{"c": nil}},
			want: map[string]any{"a": nil, "b.c": nil},
		},
		{
			name: "arrays are not flattened",
			in: map[string]any{
				"list": []any{1, 2, 3},
				"strs": []string{"a", "b"},
				"deep": map[string]any{"arr": []any{map[string]any{"x": 1}}},
			},
			want: map[string]any{
				"list":     []any{1, 2, 3},
				"strs":     []string{"a", "b"},
				"deep.arr": []any{map[string]any{"x": 1}},
			},
		},
		{
			name: "nested empty maps are dropped",
			in: map[string]any{
				"a": map[string]any{},
				"b": map[string]any{"c": map[string]any{}},
				"d": 1,
			},
			want: map[string]any{"d": 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := flatten(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("flatten() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestMerge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dst  map[string]any
		src  map[string]any
		want map[string]any
	}{
		{
			name: "empty dst",
			dst:  map[string]any{},
			src:  map[string]any{"a": 1},
			want: map[string]any{"a": 1},
		},
		{
			name: "empty src",
			dst:  map[string]any{"a": 1},
			src:  map[string]any{},
			want: map[string]any{"a": 1},
		},
		{
			name: "src wins on same key, different types",
			dst:  map[string]any{"a": 1, "b": "x"},
			src:  map[string]any{"a": "two"},
			want: map[string]any{"a": "two", "b": "x"},
		},
		{
			name: "deep nested merge",
			dst: map[string]any{
				"a": map[string]any{"x": 1, "y": 2},
				"b": 7,
			},
			src: map[string]any{
				"a": map[string]any{"y": 99, "z": 3},
				"c": 8,
			},
			want: map[string]any{
				"a": map[string]any{"x": 1, "y": 99, "z": 3},
				"b": 7,
				"c": 8,
			},
		},
		{
			name: "src scalar overwrites dst map",
			dst:  map[string]any{"a": map[string]any{"x": 1}},
			src:  map[string]any{"a": "scalar"},
			want: map[string]any{"a": "scalar"},
		},
		{
			name: "src map overwrites dst scalar",
			dst:  map[string]any{"a": "scalar"},
			src:  map[string]any{"a": map[string]any{"x": 1}},
			want: map[string]any{"a": map[string]any{"x": 1}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dstCopy := cloneMap(tc.dst)
			srcCopy := cloneMap(tc.src)

			got := merge(tc.dst, tc.src)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("merge() = %#v, want %#v", got, tc.want)
			}
			if !reflect.DeepEqual(tc.dst, dstCopy) {
				t.Fatalf("merge() mutated dst: %#v vs %#v", tc.dst, dstCopy)
			}
			if !reflect.DeepEqual(tc.src, srcCopy) {
				t.Fatalf("merge() mutated src: %#v vs %#v", tc.src, srcCopy)
			}
		})
	}
}

func TestMerge_independenceFromInputs(t *testing.T) {
	t.Parallel()

	dst := map[string]any{"a": map[string]any{"x": 1}}
	src := map[string]any{"a": map[string]any{"y": 2}}

	got := merge(dst, src)
	// Mutate result; ensure inputs unaffected.
	got["a"].(map[string]any)["x"] = 999
	if dst["a"].(map[string]any)["x"] != 1 {
		t.Fatalf("merge result shared state with dst")
	}
}

func TestStore_Lookup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		env       map[string]string
		snapshot  map[string]any
		defaults  map[string]any
		key       string
		wantVal   any
		wantFound bool
	}{
		{
			name:      "only env",
			env:       map[string]string{"FOO_BAR": "v"},
			key:       "foo.bar",
			wantVal:   "v",
			wantFound: true,
		},
		{
			name:      "only snapshot",
			snapshot:  map[string]any{"foo.bar": 42},
			key:       "foo.bar",
			wantVal:   42,
			wantFound: true,
		},
		{
			name:      "only defaults",
			defaults:  map[string]any{"foo.bar": "d"},
			key:       "foo.bar",
			wantVal:   "d",
			wantFound: true,
		},
		{
			name:      "env wins over all",
			env:       map[string]string{"FOO_BAR": "envv"},
			snapshot:  map[string]any{"foo.bar": "snap"},
			defaults:  map[string]any{"foo.bar": "def"},
			key:       "foo.bar",
			wantVal:   "envv",
			wantFound: true,
		},
		{
			name:      "snapshot wins over defaults",
			snapshot:  map[string]any{"foo.bar": "snap"},
			defaults:  map[string]any{"foo.bar": "def"},
			key:       "foo.bar",
			wantVal:   "snap",
			wantFound: true,
		},
		{
			name:      "missing key",
			key:       "no.such",
			wantVal:   nil,
			wantFound: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := tc.env
			lookup := func(k string) (string, bool) {
				v, ok := env[k]
				return v, ok
			}
			s := newStore(lookup, tc.defaults)
			if tc.snapshot != nil {
				s.setSnapshot(tc.snapshot)
			}
			got, ok := s.lookup(tc.key)
			if ok != tc.wantFound {
				t.Fatalf("lookup found=%v want %v", ok, tc.wantFound)
			}
			if !reflect.DeepEqual(got, tc.wantVal) {
				t.Fatalf("lookup val=%#v want %#v", got, tc.wantVal)
			}
		})
	}
}

func TestStore_NilEnvLookupAndDefaults(t *testing.T) {
	t.Parallel()

	s := newStore(nil, nil)
	if v, ok := s.lookup("missing"); ok || v != nil {
		t.Fatalf("expected (nil,false), got (%v,%v)", v, ok)
	}
	s.setSnapshot(nil) // should not panic; replaces with empty map
	if v, ok := s.lookup("missing"); ok || v != nil {
		t.Fatalf("expected (nil,false), got (%v,%v)", v, ok)
	}
}

func TestStore_SetSnapshotAtomicLoad(t *testing.T) {
	t.Parallel()

	s := newStore(nil, nil)
	s.setSnapshot(map[string]any{"a": 1})
	if v, ok := s.lookup("a"); !ok || v != 1 {
		t.Fatalf("expected 1, got %v", v)
	}
	s.setSnapshot(map[string]any{"a": 2})
	if v, ok := s.lookup("a"); !ok || v != 2 {
		t.Fatalf("expected 2, got %v", v)
	}
}

func TestStore_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	s := newStore(nil, nil)
	s.setSnapshot(map[string]any{"k": 0})

	var stop atomic.Bool
	var readersWG sync.WaitGroup
	var writerWG sync.WaitGroup

	// Writer keeps replacing the snapshot until readers complete.
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for i := 0; !stop.Load(); i++ {
			s.setSnapshot(map[string]any{"k": i})
		}
	}()

	// 100 readers, each performing many lookups.
	for i := 0; i < 100; i++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for j := 0; j < 1000; j++ {
				_, _ = s.lookup("k")
			}
		}()
	}

	readersWG.Wait()
	stop.Store(true)
	writerWG.Wait()
}

func TestEnvKey(t *testing.T) {
	t.Parallel()
	if got := envKey("a.b.c"); got != "A_B_C" {
		t.Fatalf("envKey = %q", got)
	}
	if got := envKey(""); got != "" {
		t.Fatalf("envKey empty = %q", got)
	}
}
