package di

import (
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	strTrue  = "true"
	strFalse = "false"
)

// toString converts v to a string. Returns ok=false only for nil.
// All other types are formatted via strconv where applicable; unknown
// types fall back to fmt-less stringer-friendly cases handled here.
//
//nolint:cyclop // flat type switch is standard and efficient for coercion
func toString(v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", false
	case string:
		return x, true
	case bool:
		return strconv.FormatBool(x), true
	case int:
		return strconv.FormatInt(int64(x), 10), true
	case int32:
		return strconv.FormatInt(int64(x), 10), true
	case int64:
		return strconv.FormatInt(x, 10), true
	case uint:
		return strconv.FormatUint(uint64(x), 10), true
	case uint32:
		return strconv.FormatUint(uint64(x), 10), true
	case uint64:
		return strconv.FormatUint(x, 10), true
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32), true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	default:
		return "", false
	}
}

// toInt converts v to int. Strings are parsed via strconv.Atoi.
//
//nolint:cyclop // flat type switch is standard and efficient for coercion
func toInt(v any) (int, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case int:
		return x, true
	case int32:
		return int(x), true
	case int64:
		return int(x), true
	case uint:
		if uint64(x) > uint64(math.MaxInt) {
			return 0, false
		}

		return int(x), true
	case uint32:
		if uint64(x) > uint64(math.MaxInt) {
			return 0, false
		}

		return int(x), true
	case uint64:
		if x > uint64(math.MaxInt) {
			return 0, false
		}

		return int(x), true
	case float32:
		return int(x), true
	case float64:
		return int(x), true
	case bool:
		if x {
			return 1, true
		}

		return 0, true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil {
			return 0, false
		}

		return n, true
	default:
		return 0, false
	}
}

// toInt64 converts v to int64.
//
//nolint:cyclop // flat type switch is standard and efficient for coercion
func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		if uint64(x) > uint64(math.MaxInt64) {
			return 0, false
		}

		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		if x > uint64(math.MaxInt64) {
			return 0, false
		}

		return int64(x), true
	case float32:
		return int64(x), true
	case float64:
		return int64(x), true
	case bool:
		if x {
			return 1, true
		}

		return 0, true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			return 0, false
		}

		return n, true
	default:
		return 0, false
	}
}

// toBool converts v to bool. String form accepts "1"/"true"/"yes"/"on"
// (case-insensitive) as true; "0"/"false"/"no"/"off"/"" as false.
// Anything else returns (false, false).
//
//nolint:cyclop // flat type switch is standard and efficient for coercion
func toBool(v any) (bool, bool) {
	switch x := v.(type) {
	case nil:
		return false, false
	case bool:
		return x, true
	case int:
		return x != 0, true
	case int32:
		return x != 0, true
	case int64:
		return x != 0, true
	case uint:
		return x != 0, true
	case uint32:
		return x != 0, true
	case uint64:
		return x != 0, true
	case float32:
		return x != 0, true
	case float64:
		return x != 0, true
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "1", strTrue, "yes", "on":
			return true, true
		case "0", strFalse, "no", "off", "":
			return false, true
		default:
			return false, false
		}
	default:
		return false, false
	}
}

// toFloat64 converts v to float64.
//
//nolint:cyclop // flat type switch is standard and efficient for coercion
func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case bool:
		if x {
			return 1, true
		}

		return 0, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return 0, false
		}

		return f, true
	default:
		return 0, false
	}
}

// toDuration converts v to time.Duration. Strings go through
// time.ParseDuration. Numeric values are interpreted as nanoseconds
// (matching viper's GetDuration semantics).
//
//nolint:cyclop // flat type switch is standard and efficient for coercion
func toDuration(v any) (time.Duration, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case time.Duration:
		return x, true
	case int:
		return time.Duration(x), true
	case int32:
		return time.Duration(x), true
	case int64:
		return time.Duration(x), true
	case uint:
		if uint64(x) > uint64(math.MaxInt64) {
			return 0, false
		}

		return time.Duration(x), true
	case uint32:
		return time.Duration(x), true
	case uint64:
		if x > uint64(math.MaxInt64) {
			return 0, false
		}

		return time.Duration(x), true
	case float32:
		return time.Duration(x), true
	case float64:
		return time.Duration(x), true
	case string:
		d, err := time.ParseDuration(strings.TrimSpace(x))
		if err != nil {
			return 0, false
		}

		return d, true
	default:
		return 0, false
	}
}

// toStringSlice converts v to []string. Accepts:
//   - []string: returned as-is
//   - []any: each element coerced via toString; if any element is not
//     coercible, returns (nil, false)
//   - string: split by ',', trimmed of whitespace, empty pieces dropped
//
// Anything else returns (nil, false). Nil returns (nil, false).
//
//nolint:cyclop // flat type switch is standard and efficient for coercion
func toStringSlice(v any) ([]string, bool) {
	switch x := v.(type) {
	case nil:
		return nil, false
	case []string:
		out := make([]string, len(x))
		copy(out, x)

		return out, true
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			s, ok := toString(item)
			if !ok {
				return nil, false
			}

			out = append(out, s)
		}

		return out, true
	case string:
		if x == "" {
			return []string{}, true
		}

		parts := strings.Split(x, ",")

		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}

			out = append(out, p)
		}

		return out, true
	default:
		return nil, false
	}
}
