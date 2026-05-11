package di

import (
	"reflect"
	"testing"
	"time"
)

func TestToString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     any
		want   string
		wantOk bool
	}{
		{"nil", nil, "", false},
		{"string", "hello", "hello", true},
		{"empty string", "", "", true},
		{"true", true, "true", true},
		{"false", false, "false", true},
		{"int", 42, "42", true},
		{"int32", int32(7), "7", true},
		{"int64", int64(-9), "-9", true},
		{"uint", uint(3), "3", true},
		{"uint32", uint32(4), "4", true},
		{"uint64", uint64(5), "5", true},
		{"float32", float32(1.5), "1.5", true},
		{"float64", 2.25, "2.25", true},
		{"unknown type", struct{}{}, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := toString(tc.in)
			if got != tc.want || ok != tc.wantOk {
				t.Fatalf("toString(%v) = (%q,%v), want (%q,%v)", tc.in, got, ok, tc.want, tc.wantOk)
			}
		})
	}
}

func TestToInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     any
		want   int
		wantOk bool
	}{
		{"nil", nil, 0, false},
		{"int", 5, 5, true},
		{"int32", int32(6), 6, true},
		{"int64", int64(7), 7, true},
		{"uint", uint(8), 8, true},
		{"uint32", uint32(9), 9, true},
		{"uint64", uint64(10), 10, true},
		{"float32", float32(3.7), 3, true},
		{"float64", 4.9, 4, true},
		{"true", true, 1, true},
		{"false", false, 0, true},
		{"string number", "42", 42, true},
		{"string padded", " 12 ", 12, true},
		{"string invalid", "abc", 0, false},
		{"empty string", "", 0, false},
		{"unknown type", struct{}{}, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := toInt(tc.in)
			if got != tc.want || ok != tc.wantOk {
				t.Fatalf("toInt(%v) = (%d,%v), want (%d,%v)", tc.in, got, ok, tc.want, tc.wantOk)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     any
		want   int64
		wantOk bool
	}{
		{"nil", nil, 0, false},
		{"int", 5, 5, true},
		{"int32", int32(6), 6, true},
		{"int64", int64(1 << 40), 1 << 40, true},
		{"uint", uint(8), 8, true},
		{"uint32", uint32(9), 9, true},
		{"uint64", uint64(10), 10, true},
		{"float32", float32(3.7), 3, true},
		{"float64", 4.9, 4, true},
		{"true", true, 1, true},
		{"false", false, 0, true},
		{"string", "12345", 12345, true},
		{"string invalid", "x", 0, false},
		{"unknown type", []int{1}, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := toInt64(tc.in)
			if got != tc.want || ok != tc.wantOk {
				t.Fatalf("toInt64(%v) = (%d,%v), want (%d,%v)", tc.in, got, ok, tc.want, tc.wantOk)
			}
		})
	}
}

func TestToBool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     any
		want   bool
		wantOk bool
	}{
		{"nil", nil, false, false},
		{"true", true, true, true},
		{"false", false, false, true},
		{"int 1", 1, true, true},
		{"int 0", 0, false, true},
		{"int32", int32(2), true, true},
		{"int64", int64(0), false, true},
		{"uint", uint(1), true, true},
		{"uint32", uint32(0), false, true},
		{"uint64", uint64(2), true, true},
		{"float32", float32(0), false, true},
		{"float64", 1.5, true, true},
		{"str 1", "1", true, true},
		{"str true", "true", true, true},
		{"str TRUE", "TRUE", true, true},
		{"str yes", "yes", true, true},
		{"str on", "on", true, true},
		{"str padded", "  Yes  ", true, true},
		{"str 0", "0", false, true},
		{"str false", "false", false, true},
		{"str No", "No", false, true},
		{"str off", "off", false, true},
		{"str empty", "", false, true},
		{"str invalid", "maybe", false, false},
		{"unknown type", []int{}, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := toBool(tc.in)
			if got != tc.want || ok != tc.wantOk {
				t.Fatalf("toBool(%v) = (%v,%v), want (%v,%v)", tc.in, got, ok, tc.want, tc.wantOk)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     any
		want   float64
		wantOk bool
	}{
		{"nil", nil, 0, false},
		{"float64", 1.5, 1.5, true},
		{"float32", float32(2.5), 2.5, true},
		{"int", 3, 3, true},
		{"int32", int32(4), 4, true},
		{"int64", int64(5), 5, true},
		{"uint", uint(6), 6, true},
		{"uint32", uint32(7), 7, true},
		{"uint64", uint64(8), 8, true},
		{"true", true, 1, true},
		{"false", false, 0, true},
		{"string", "3.14", 3.14, true},
		{"string invalid", "x", 0, false},
		{"unknown type", []int{1}, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := toFloat64(tc.in)
			if got != tc.want || ok != tc.wantOk {
				t.Fatalf("toFloat64(%v) = (%v,%v), want (%v,%v)", tc.in, got, ok, tc.want, tc.wantOk)
			}
		})
	}
}

func TestToDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     any
		want   time.Duration
		wantOk bool
	}{
		{"nil", nil, 0, false},
		{"duration", 5 * time.Second, 5 * time.Second, true},
		{"int (ns)", 100, 100 * time.Nanosecond, true},
		{"int32", int32(1000), 1000 * time.Nanosecond, true},
		{"int64", int64(time.Second), time.Second, true},
		{"uint", uint(500), 500 * time.Nanosecond, true},
		{"uint32", uint32(1), 1, true},
		{"uint64", uint64(2), 2, true},
		{"float32", float32(time.Second), time.Second, true},
		{"float64", float64(2 * time.Second), 2 * time.Second, true},
		{"string 5s", "5s", 5 * time.Second, true},
		{"string 250ms", "250ms", 250 * time.Millisecond, true},
		{"string padded", " 1m ", time.Minute, true},
		{"string invalid", "abc", 0, false},
		{"empty string", "", 0, false},
		{"unknown type", []int{}, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := toDuration(tc.in)
			if got != tc.want || ok != tc.wantOk {
				t.Fatalf("toDuration(%v) = (%v,%v), want (%v,%v)", tc.in, got, ok, tc.want, tc.wantOk)
			}
		})
	}
}

func TestToStringSlice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     any
		want   []string
		wantOk bool
	}{
		{"nil", nil, nil, false},
		{"[]string", []string{"a", "b"}, []string{"a", "b"}, true},
		{"[]any all strings", []any{"a", "b"}, []string{"a", "b"}, true},
		{"[]any mixed coercible", []any{"a", 1}, []string{"a", "1"}, true},
		{"[]any has uncoercible", []any{"a", struct{}{}}, nil, false},
		{"csv string", "a,b,c", []string{"a", "b", "c"}, true},
		{"csv with spaces", " a , b ", []string{"a", "b"}, true},
		{"csv empty pieces dropped", "a,,b,", []string{"a", "b"}, true},
		{"empty string", "", []string{}, true},
		{"unknown type", 42, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := toStringSlice(tc.in)
			if ok != tc.wantOk {
				t.Fatalf("toStringSlice(%v) ok=%v want %v", tc.in, ok, tc.wantOk)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("toStringSlice(%v) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestToStringSlice_DoesNotShareBacking(t *testing.T) {
	t.Parallel()

	in := []string{"a", "b"}
	got, ok := toStringSlice(in)
	if !ok {
		t.Fatal("expected ok")
	}
	got[0] = "X"
	if in[0] != "a" {
		t.Fatalf("toStringSlice shared backing array: %v", in)
	}
}

// Fuzz tests: ensure no panics on arbitrary string input.

func FuzzToInt(f *testing.F) {
	f.Add("42")
	f.Add("")
	f.Add("abc")
	f.Add("-99999999999999999999")
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = toInt(s)
	})
}

func FuzzToBool(f *testing.F) {
	f.Add("true")
	f.Add("yes")
	f.Add("0")
	f.Add("garbage")
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = toBool(s)
	})
}

func FuzzToDuration(f *testing.F) {
	f.Add("5s")
	f.Add("250ms")
	f.Add("")
	f.Add("abc")
	f.Add("1h2m3s")
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = toDuration(s)
	})
}
