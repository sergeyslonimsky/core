package di

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrRequiredKeyMissing is returned by Require (and collected by Reader)
	// when a key is not set in any source. A null value (`key:` with nothing
	// after it in yaml) and an env var set to "" count as not set, and the
	// lower-priority sources are consulted instead.
	ErrRequiredKeyMissing = errors.New("required config key is missing")
	// ErrRequiredKeyType is returned by Require (and collected by Reader)
	// when a key is present but its value cannot be coerced to T.
	ErrRequiredKeyType = errors.New("config value cannot be coerced to the requested type")
	// ErrNilConfig is returned by Require and Reader.Err when they are
	// given a nil *Config (e.g. NewConfig failed and its error was dropped).
	ErrNilConfig = errors.New("config is nil")
)

// Require returns the value for key as T, or an error when the key is
// missing (ErrRequiredKeyMissing), its value cannot be coerced to T
// (ErrRequiredKeyType) or cfg is nil (ErrNilConfig). The error names the
// key.
//
// Require uses the same lookup as Get: an env var set to "" and a null
// value do not count as set, so a key whose only value is one of those is
// missing, and a lower-priority source (yaml, defaults) is used when it
// has one. Beyond that, Require checks presence, not content: an empty
// yaml string, 0 or an empty slice is a present value. Validate such
// values where they are used (e.g. in the constructor of the client that
// needs them).
//
// To read many required keys at once and report every problem together,
// use a Reader.
func Require[T ValueType](cfg *Config, key string) (T, error) {
	value, raw, res := lookupAs[T](cfg, key)

	switch res {
	case lookupFound:
		return value, nil
	case lookupMissing:
		return value, fmt.Errorf("%w: %s", ErrRequiredKeyMissing, key)
	case lookupMistyped:
		return value, fmt.Errorf("%w: %s: want %T, got %T", ErrRequiredKeyType, key, value, raw)
	case lookupNoConfig:
		return value, fmt.Errorf("%w: reading %s", ErrNilConfig, key)
	}

	return value, fmt.Errorf("%w: %s", ErrRequiredKeyMissing, key)
}

// Reader reads a config in one pass: required keys collect their errors,
// optional keys read as usual, so a config struct can be built in one
// literal and checked once:
//
//	r := di.NewReader(cfg)
//	smtpCfg := smtp.Config{
//		Host:     r.Required[string]("smtp.host"),
//		Port:     r.Required[int]("smtp.port"),
//		Username: r.Get[string]("smtp.username"),
//		Timeout:  r.GetOrDefault[time.Duration]("smtp.timeout", 10*time.Second),
//	}
//	if err := r.Err(); err != nil {
//		return err // lists every missing or mistyped required key
//	}
//
// Hot-reloadable values keep using Live / LiveOrDefault on the *Config.
// A Reader is safe for concurrent use. A Reader over a nil *Config (or a
// zero Reader) does not panic: every read returns the zero value or the
// default, and Err reports ErrNilConfig.
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
	if !r.hasConfig() {
		// Reported once by Err instead of once per key.
		var zero T

		return zero
	}

	value, err := Require[T](r.cfg, key)
	if err != nil {
		r.mu.Lock()
		r.errs = append(r.errs, err)
		r.mu.Unlock()
	}

	return value
}

// Get reads an optional key, like the package-level Get: the zero value
// of T when the key is absent or not coercible. Never records an error.
func (r *Reader) Get[T ValueType](key string) T {
	return Get[T](r.cfg, key)
}

// GetOrDefault reads an optional key, like the package-level
// GetOrDefault: def when the key is absent or not coercible. Never
// records an error.
func (r *Reader) GetOrDefault[T ValueType](key string, def T) T {
	return GetOrDefault[T](r.cfg, key, def)
}

// Err returns every error recorded by Required, joined with errors.Join,
// or nil if all of them succeeded. Errors appear in the order they were
// recorded: the read order for sequential reads, unspecified for reads
// made from several goroutines at once. errors.Is works with
// ErrRequiredKeyMissing and ErrRequiredKeyType. When the Reader has no
// *Config, Err returns ErrNilConfig.
func (r *Reader) Err() error {
	if !r.hasConfig() {
		return fmt.Errorf("di.Reader: %w", ErrNilConfig)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return errors.Join(r.errs...)
}

// hasConfig reports whether r has a usable *Config (a zero Config{} has
// no store and is treated like a nil one).
func (r *Reader) hasConfig() bool {
	return r.cfg != nil && r.cfg.store != nil
}
