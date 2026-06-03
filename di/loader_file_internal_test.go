//go:build !windows

package di

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// stringOpener returns an openFileFn closure backed by an in-memory map
// of path -> yaml content. It also records the paths it was called with.
func stringOpener(contents map[string]string, calls *[]string) openFileFn {
	return func(path string) (io.ReadCloser, error) {
		if calls != nil {
			*calls = append(*calls, path)
		}

		body, ok := contents[path]
		if !ok {
			return nil, fmt.Errorf("not found: %s", path)
		}

		return io.NopCloser(strings.NewReader(body)), nil
	}
}

// ----- Set A: unit tests of loadFiles via in-memory openFileFn -----

func TestLoadFiles_SingleValidFile(t *testing.T) {
	t.Parallel()

	open := stringOpener(map[string]string{
		"a.yaml": "foo: 1\nbar:\n  baz: hello\n",
	}, nil)
	got, err := loadFiles(open, []string{"a.yaml"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"foo":     1,
		"bar.baz": "hello",
	}, got)
}

func TestLoadFiles_TwoFilesSecondOverridesKey(t *testing.T) {
	t.Parallel()

	open := stringOpener(map[string]string{
		"first.yaml":  "shared: from-first\nonlyA: 1\n",
		"second.yaml": "shared: from-second\nonlyB: 2\n",
	}, nil)
	got, err := loadFiles(open, []string{"first.yaml", "second.yaml"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"shared": "from-second",
		"onlyA":  1,
		"onlyB":  2,
	}, got)
}

func TestLoadFiles_ThreeFilesDisjointKeys(t *testing.T) {
	t.Parallel()

	open := stringOpener(map[string]string{
		"a.yaml": "a: 1\n",
		"b.yaml": "b: 2\n",
		"c.yaml": "c: 3\n",
	}, nil)
	got, err := loadFiles(open, []string{"a.yaml", "b.yaml", "c.yaml"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": 1, "b": 2, "c": 3}, got)
}

func TestLoadFiles_OpenErrorIsWrappedWithPath(t *testing.T) {
	t.Parallel()

	open := stringOpener(map[string]string{}, nil)
	_, err := loadFiles(open, []string{"missing.yaml"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing.yaml")
	require.Contains(t, err.Error(), "read config from")
}

func TestLoadFiles_InvalidYAMLIsWrappedWithPath(t *testing.T) {
	t.Parallel()

	open := stringOpener(map[string]string{
		"bad.yaml": "this: : : not yaml\n  - bad\n",
	}, nil)
	_, err := loadFiles(open, []string{"bad.yaml"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad.yaml")
	require.Contains(t, err.Error(), "read config from")
}

func TestLoadFiles_DirectoryPathAppendsConfigYaml(t *testing.T) {
	t.Parallel()

	want := filepath.Clean(filepath.Join("etc/conf.d", "config.yaml"))
	open := stringOpener(map[string]string{
		want: "k: v\n",
	}, nil)
	got, err := loadFiles(open, []string{"etc/conf.d"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"k": "v"}, got)
}

func TestLoadFiles_UppercaseYAMLExtensionRecognised(t *testing.T) {
	t.Parallel()

	var calls []string

	open := stringOpener(map[string]string{
		"app.YAML": "k: v\n",
	}, &calls)
	got, err := loadFiles(open, []string{"app.YAML"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"k": "v"}, got)
	require.Equal(t, []string{"app.YAML"}, calls)
}

func TestLoadFiles_YmlExtensionRecognised(t *testing.T) {
	t.Parallel()

	var calls []string

	open := stringOpener(map[string]string{
		"app.yml": "k: v\n",
	}, &calls)
	_, err := loadFiles(open, []string{"app.yml"})
	require.NoError(t, err)
	require.Equal(t, []string{"app.yml"}, calls)
}

func TestLoadFiles_FilepathCleanIsApplied(t *testing.T) {
	t.Parallel()

	var calls []string
	// "./a/../b//c.yaml" cleans to "b/c.yaml".
	open := stringOpener(map[string]string{
		"b/c.yaml": "k: v\n",
	}, &calls)
	got, err := loadFiles(open, []string{"./a/../b//c.yaml"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"k": "v"}, got)
	require.Equal(t, []string{"b/c.yaml"}, calls)
}

func TestLoadFiles_EmptyPathsReturnsEmptyMap(t *testing.T) {
	t.Parallel()

	open := stringOpener(map[string]string{}, nil)
	got, err := loadFiles(open, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]any{}, got)

	got, err = loadFiles(open, []string{})
	require.NoError(t, err)
	require.Equal(t, map[string]any{}, got)
}

func TestLoadFiles_EmptyFileContentYieldsEmptyMap(t *testing.T) {
	t.Parallel()

	open := stringOpener(map[string]string{
		"empty.yaml":   "",
		"nullish.yaml": "~\n",
	}, nil)
	got, err := loadFiles(open, []string{"empty.yaml", "nullish.yaml"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{}, got)
}

func TestLoadFiles_MergeNestedMaps(t *testing.T) {
	t.Parallel()

	open := stringOpener(map[string]string{
		"a.yaml": "db:\n  host: a\n  port: 1\n",
		"b.yaml": "db:\n  host: b\n",
	}, nil)
	got, err := loadFiles(open, []string{"a.yaml", "b.yaml"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"db.host": "b",
		"db.port": 1,
	}, got)
}

// ----- Set B: integration tests of osOpenFile via t.TempDir() -----

func TestOsOpenFile_RegularFileReadsCorrectly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(p, []byte("foo: bar\nnested:\n  k: v\n"), 0o644))

	got, err := loadFiles(osOpenFile, []string{p})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"foo":      "bar",
		"nested.k": "v",
	}, got)
}

func TestOsOpenFile_FileTooLarge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "big.yaml")
	f, err := os.Create(p)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(11*1024*1024))
	require.NoError(t, f.Close())

	_, err = loadFiles(osOpenFile, []string{p})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrConfigFileTooLarge)
	require.Contains(t, err.Error(), p)
}

func TestOsOpenFile_NonExistentPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "does-not-exist.yaml")
	_, err := loadFiles(osOpenFile, []string{p})
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Contains(t, err.Error(), p)
}

func TestOsOpenFile_SymlinkToRegularFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "real.yaml")
	require.NoError(t, os.WriteFile(target, []byte("k: v\n"), 0o644))

	link := filepath.Join(dir, "link.yaml")
	require.NoError(t, os.Symlink(target, link))

	got, err := loadFiles(osOpenFile, []string{link})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"k": "v"}, got)
}

func TestOsOpenFile_FifoNotRegular(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	p := filepath.Join(dir, "fifo.yaml")
	if err := syscall.Mkfifo(p, 0o644); err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP) {
			t.Skip("requires mkfifo")
		}

		t.Fatalf("mkfifo: %v", err)
	}

	// open() on a fifo without a writer would block; run osOpenFile in a
	// goroutine with a writer so the open returns and Stat sees the fifo.
	// Easier: open writer side first (non-blocking via O_RDWR isn't portable),
	// just call osOpenFile in a goroutine.
	type result struct {
		rc  io.ReadCloser
		err error
	}

	done := make(chan result, 1)

	go func() {
		rc, err := osOpenFile(p)
		done <- result{rc, err}
	}()

	// Open writer side so the reader open() can return.
	w, err := os.OpenFile(p, os.O_WRONLY, 0)
	require.NoError(t, err)

	defer w.Close()

	res := <-done
	if res.rc != nil {
		_ = res.rc.Close()
	}

	require.Error(t, res.err)
	require.ErrorIs(t, res.err, ErrNotRegularFile)
	require.Contains(t, res.err.Error(), p)
}

func TestOsOpenFile_DirectoryIsNotRegular(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Pass dir directly to osOpenFile (not via loadFiles which would append config.yaml).
	rc, err := osOpenFile(dir)
	if rc != nil {
		_ = rc.Close()
	}

	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotRegularFile)
	require.Contains(t, err.Error(), dir)
}

func TestOsOpenFile_LimitReaderCapsRead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "ok.yaml")
	require.NoError(t, os.WriteFile(p, []byte("k: v\n"), 0o644))

	rc, err := osOpenFile(p)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Close() })

	// Reading should yield exactly the file bytes.
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "k: v\n", string(data))
}
