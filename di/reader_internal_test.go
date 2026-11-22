package di

import (
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireTestConfig builds a Config from a yaml file plus env overrides.
func requireTestConfig(t *testing.T, env map[string]string) *Config {
	t.Helper()

	yaml := []byte(`
smtp:
  host: localhost
  port: 1025
  tls: ""
  timeout: 5s
  enabled: true
  ratio: 0.5
  big: 9223372036854775000
  hosts:
    - a
    - b
  nullvalue:
  badport: not-a-number
`)

	lookup := map[string]string{"APP_CONFIG_FILE_PATHS": "/c.yaml"}
	maps.Copy(lookup, env)

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup:   envFn(lookup),
		openFile:    fileFn(map[string][]byte{"/c.yaml": yaml}),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)

	return cfg
}

func TestRequire_TypeMatrix(t *testing.T) {
	t.Parallel()

	cfg := requireTestConfig(t, nil)

	host, err := Require[string](cfg, "smtp.host")
	require.NoError(t, err)
	assert.Equal(t, "localhost", host)

	port, err := Require[int](cfg, "smtp.port")
	require.NoError(t, err)
	assert.Equal(t, 1025, port)

	big, err := Require[int64](cfg, "smtp.big")
	require.NoError(t, err)
	assert.Equal(t, int64(9223372036854775000), big)

	enabled, err := Require[bool](cfg, "smtp.enabled")
	require.NoError(t, err)
	assert.True(t, enabled)

	ratio, err := Require[float64](cfg, "smtp.ratio")
	require.NoError(t, err)
	assert.InDelta(t, 0.5, ratio, 0.0001)

	timeout, err := Require[time.Duration](cfg, "smtp.timeout")
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, timeout)

	hosts, err := Require[[]string](cfg, "smtp.hosts")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, hosts)
}

func TestRequire_Errors(t *testing.T) {
	t.Parallel()

	cfg := requireTestConfig(t, nil)

	tests := []struct {
		name    string
		read    func() error
		wantErr error
	}{
		{
			name: "absent key",
			read: func() error {
				_, err := Require[string](cfg, "smtp.username")

				return err
			},
			wantErr: ErrRequiredKeyMissing,
		},
		{
			name: "null value counts as missing",
			read: func() error {
				_, err := Require[string](cfg, "smtp.nullvalue")

				return err
			},
			wantErr: ErrRequiredKeyMissing,
		},
		{
			name: "value of the wrong type",
			read: func() error {
				_, err := Require[int](cfg, "smtp.badport")

				return err
			},
			wantErr: ErrRequiredKeyType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.read()
			require.ErrorIs(t, err, tt.wantErr)
			assert.Contains(t, err.Error(), "smtp.")
		})
	}
}

func TestRequire_ZeroValueOnError(t *testing.T) {
	t.Parallel()

	cfg := requireTestConfig(t, nil)

	port, err := Require[int](cfg, "smtp.badport")
	require.ErrorIs(t, err, ErrRequiredKeyType)
	assert.Equal(t, 0, port)
}

func TestRequire_PresenceNotContent(t *testing.T) {
	t.Parallel()

	// An empty yaml string is a present value.
	cfg := requireTestConfig(t, nil)

	tls, err := Require[string](cfg, "smtp.tls")
	require.NoError(t, err)
	assert.Empty(t, tls)
}

func TestRequire_EmptyEnvIsUnset(t *testing.T) {
	t.Parallel()

	// SMTP_PASSWORD="" is what `SMTP_PASSWORD=${SMTP_PASSWORD}` produces
	// when the variable is not set on the host: it must not satisfy
	// Require, and it must not hide the value from the file.
	cfg := requireTestConfig(t, map[string]string{
		"SMTP_PASSWORD": "",
		"SMTP_HOST":     "",
		"SMTP_ENABLED":  "",
	})

	_, err := Require[string](cfg, "smtp.password")
	require.ErrorIs(t, err, ErrRequiredKeyMissing)

	host, err := Require[string](cfg, "smtp.host")
	require.NoError(t, err)
	assert.Equal(t, "localhost", host)

	enabled, err := Require[bool](cfg, "smtp.enabled")
	require.NoError(t, err)
	assert.True(t, enabled)
}

func TestRequire_NullFallsBackToDefaults(t *testing.T) {
	t.Parallel()

	s := newStore(nil, map[string]any{"smtp.port": 587})
	s.setSnapshot(map[string]any{"smtp.port": nil})

	cfg := &Config{store: s, serviceName: "", appEnv: AppEnvDev}

	port, err := Require[int](cfg, "smtp.port")
	require.NoError(t, err)
	assert.Equal(t, 587, port)
	assert.Equal(t, 587, Get[int](cfg, "smtp.port"))
}

func TestRequire_NilConfig(t *testing.T) {
	t.Parallel()

	v, err := Require[string](nil, "smtp.host")
	require.ErrorIs(t, err, ErrNilConfig)
	assert.Contains(t, err.Error(), "smtp.host")
	assert.Empty(t, v)
}

func TestReader_NilConfig(t *testing.T) {
	t.Parallel()

	for name, r := range map[string]*Reader{
		"NewReader(nil)": NewReader(nil),
		"zero Reader":    {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Empty(t, r.Required[string]("smtp.host"))
			assert.Equal(t, 0, r.Required[int]("smtp.port"))
			assert.Empty(t, r.Get[string]("smtp.username"))
			assert.Equal(t, 7, r.GetOrDefault[int]("smtp.retries", 7))

			err := r.Err()
			require.ErrorIs(t, err, ErrNilConfig)
			assert.NotErrorIs(t, err, ErrRequiredKeyMissing)
		})
	}
}

func TestRequire_EnvOverridesFile(t *testing.T) {
	t.Parallel()

	cfg := requireTestConfig(t, map[string]string{"SMTP_PORT": "587", "SMTP_USERNAME": "resend"})

	port, err := Require[int](cfg, "smtp.port")
	require.NoError(t, err)
	assert.Equal(t, 587, port)

	// Present only in env.
	username, err := Require[string](cfg, "smtp.username")
	require.NoError(t, err)
	assert.Equal(t, "resend", username)
}

func TestReader_AllPresent(t *testing.T) {
	t.Parallel()

	r := NewReader(requireTestConfig(t, nil))

	host := r.Required[string]("smtp.host")
	port := r.Required[int]("smtp.port")

	require.NoError(t, r.Err())
	assert.Equal(t, "localhost", host)
	assert.Equal(t, 1025, port)
}

func TestReader_CollectsEveryError(t *testing.T) {
	t.Parallel()

	r := NewReader(requireTestConfig(t, nil))

	host := r.Required[string]("smtp.host")
	username := r.Required[string]("smtp.username")
	port := r.Required[int]("smtp.badport")
	password := r.Required[string]("smtp.password")

	// Successful reads still return their values; failed ones return zero.
	assert.Equal(t, "localhost", host)
	assert.Empty(t, username)
	assert.Equal(t, 0, port)
	assert.Empty(t, password)

	err := r.Err()
	require.ErrorIs(t, err, ErrRequiredKeyMissing)
	require.ErrorIs(t, err, ErrRequiredKeyType)

	// Every failed key is reported, in read order; the good one is not.
	msg := err.Error()
	assert.NotContains(t, msg, "smtp.host")

	usernameAt := strings.Index(msg, "smtp.username")
	badportAt := strings.Index(msg, "smtp.badport")
	passwordAt := strings.Index(msg, "smtp.password")

	require.NotEqual(t, -1, usernameAt)
	require.NotEqual(t, -1, badportAt)
	require.NotEqual(t, -1, passwordAt)
	assert.Less(t, usernameAt, badportAt)
	assert.Less(t, badportAt, passwordAt)
}

func TestReader_ConcurrentUse(t *testing.T) {
	t.Parallel()

	const readers = 32

	r := NewReader(requireTestConfig(t, nil))

	var wg sync.WaitGroup

	for range readers {
		wg.Go(func() {
			_ = r.Required[string]("smtp.host")
			_ = r.Required[string]("smtp.username")
		})
	}

	wg.Wait()

	err := r.Err()
	require.ErrorIs(t, err, ErrRequiredKeyMissing)
	assert.Equal(t, readers, strings.Count(err.Error(), "smtp.username"))
}

func TestReader_OptionalKeys(t *testing.T) {
	t.Parallel()

	r := NewReader(requireTestConfig(t, map[string]string{"SMTP_USERNAME": "resend"}))

	// Present values, from yaml and env.
	assert.Equal(t, 1025, r.Get[int]("smtp.port"))
	assert.Equal(t, "resend", r.Get[string]("smtp.username"))
	assert.Equal(t, 5*time.Second, r.GetOrDefault[time.Duration]("smtp.timeout", time.Minute))

	// Absent, null or not coercible: zero value / default, as the package
	// functions do.
	assert.Empty(t, r.Get[string]("smtp.password"))
	assert.Empty(t, r.Get[string]("smtp.nullvalue"))
	assert.Equal(t, 0, r.Get[int]("smtp.badport"))
	assert.Equal(t, 587, r.GetOrDefault[int]("smtp.missingport", 587))
	assert.Equal(t, 587, r.GetOrDefault[int]("smtp.badport", 587))

	// Optional reads never make the Reader fail.
	require.NoError(t, r.Err())
}

func TestReader_OptionalReadsDoNotHideRequiredErrors(t *testing.T) {
	t.Parallel()

	r := NewReader(requireTestConfig(t, nil))

	_ = r.Get[string]("smtp.password")
	_ = r.Required[string]("smtp.username")
	_ = r.GetOrDefault[int]("smtp.badport", 1)

	err := r.Err()
	require.ErrorIs(t, err, ErrRequiredKeyMissing)
	require.NotErrorIs(t, err, ErrRequiredKeyType)
	assert.NotContains(t, err.Error(), "smtp.password")
}

func TestRequire_NullInLaterFileKeepsEarlierValue(t *testing.T) {
	t.Parallel()

	cfg, err := newConfigWithDeps(t.Context(), configDeps{
		envLookup: envFn(map[string]string{"APP_CONFIG_FILE_PATHS": "/a.yaml,/b.yaml"}),
		openFile: fileFn(map[string][]byte{
			"/a.yaml": []byte("smtp:\n  host: base-host\n"),
			"/b.yaml": []byte("smtp:\n  host:\n"),
		}),
		etcdFactory: noFactory(),
	})
	require.NoError(t, err)

	host, err := Require[string](cfg, "smtp.host")
	require.NoError(t, err)
	assert.Equal(t, "base-host", host)
}

func TestOptionalReaders_EmptyEnvFallsThrough(t *testing.T) {
	t.Parallel()

	cfg := requireTestConfig(t, map[string]string{"SMTP_HOST": "", "SMTP_PORT": "", "SMTP_USERNAME": ""})

	assert.Equal(t, "localhost", Get[string](cfg, "smtp.host"))
	assert.Equal(t, 1025, GetOrDefault[int](cfg, "smtp.port", 1))
	assert.Equal(t, "localhost", Live[string](cfg, "smtp.host")())
	assert.Equal(t, 1025, LiveOrDefault[int](cfg, "smtp.port", 1)())
	assert.Equal(t, "anon", GetOrDefault[string](cfg, "smtp.username", "anon"))
}

func TestReader_ZeroConfig(t *testing.T) {
	t.Parallel()

	r := NewReader(&Config{})

	assert.Empty(t, r.Required[string]("a"))
	assert.Empty(t, r.Required[string]("b"))
	assert.Empty(t, r.Get[string]("c"))
	assert.Equal(t, 3, r.GetOrDefault[int]("d", 3))

	err := r.Err()
	require.ErrorIs(t, err, ErrNilConfig)
	assert.NotContains(t, err.Error(), "reading", "nil config must be reported once, not per key")
}
