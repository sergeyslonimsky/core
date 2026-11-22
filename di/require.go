package di

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrRequiredKeyMissing is returned by Require (and collected by Reader)
	// when a key is absent from every source, or is present with a null
	// value (e.g. `key:` with nothing after it in yaml).
	ErrRequiredKeyMissing = errors.New("required config key is missing")
	// ErrRequiredKeyType is returned by Require (and collected by Reader)
	// when a key is present but its value cannot be coerced to T.
	ErrRequiredKeyType = errors.New("config value cannot be coerced to the requested type")
)

// Require returns the value for key as T, or an error when the key is
// missing (ErrRequiredKeyMissing) or its value cannot be coerced to T
// (ErrRequiredKeyType). The error names the key.
//
// Require checks presence, not content: an empty string, 0 or an empty
// slice is a present value, and so is an env var set to "". Validate such
// values where they are used (e.g. in the constructor of the client that
// needs them).
//
// To read many required keys at once and report every problem together,
// use a Reader.
func Require[T ValueType](cfg *Config, key string) (T, error) {
	var zero T

	raw, ok := cfg.store.lookup(key)
	if !ok || raw == nil {
		return zero, fmt.Errorf("%w: %s", ErrRequiredKeyMissing, key)
	}

	value, ok := coerce[T](raw)
	if !ok {
		return zero, fmt.Errorf("%w: %s: want %T, got %T", ErrRequiredKeyType, key, zero, raw)
	}

	return value, nil
}

// Reader reads required keys and collects their errors, so a config
// struct can be built in one literal and checked once:
//
//	r := di.NewReader(cfg)
//	smtpCfg := smtp.Config{
//		Host: r.Required[string]("smtp.host"),
//		Port: r.Required[int]("smtp.port"),
//	}
//	if err := r.Err(); err != nil {
//		return err // lists every missing or mistyped key
//	}
//
// Optional keys keep using Get / GetOrDefault on the same *Config.
// A Reader is safe for concurrent use.
type Reader struct {
	cfg *Config

	mu   sync.Mutex
	errs []error
}

// NewReader returns a Reader over cfg.
func NewReader(cfg *Config) *Reader {
	return &Reader{cfg: cfg, mu: sync.Mutex{}, errs: nil}
}

// Required returns the value for key as T, like Require. On failure it
// returns the zero value of T and records the error in r; see Reader.Err.
func (r *Reader) Required[T ValueType](key string) T {
	value, err := Require[T](r.cfg, key)
	if err != nil {
		r.mu.Lock()
		r.errs = append(r.errs, err)
		r.mu.Unlock()
	}

	return value
}

// Err returns every error recorded by Required, joined with errors.Join
// in the order the keys were read, or nil if all of them succeeded.
// errors.Is works with ErrRequiredKeyMissing and ErrRequiredKeyType.
func (r *Reader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return errors.Join(r.errs...)
}
