package di

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// maxConfigBytes caps the size of a YAML config file. The limit is enforced
// both via Stat (fast fail) and via io.LimitReader (defense in depth).
const maxConfigBytes = 10 * 1024 * 1024 // 10 MB

// Sentinel errors exposed by the file loader.
var (
	ErrNotRegularFile     = errors.New("config path is not a regular file")
	ErrConfigFileTooLarge = errors.New("config file exceeds size limit")
)

// openFileFn opens a config source for reading. Production code uses
// osOpenFile; tests inject closures over in-memory buffers.
type openFileFn func(path string) (io.ReadCloser, error)

// loadFiles reads each path in order, parses it as YAML, flattens the
// resulting map to dotted keys and merges into an accumulator. Later
// paths override earlier ones on key conflicts.
//
// Path resolution: if a path ends in .yaml or .yml (case-insensitive on
// the extension), it is treated as a file path. Otherwise the path is
// treated as a directory and "config.yaml" is appended. The resulting
// path is normalised via filepath.Clean before being passed to open.
func loadFiles(open openFileFn, paths []string) (map[string]any, error) {
	acc := map[string]any{}
	for _, p := range paths {
		resolved := filepath.Clean(resolvePath(p))
		flat, err := readOneFile(open, resolved)
		if err != nil {
			return nil, err
		}
		acc = merge(acc, flat)
	}
	return acc, nil
}

func resolvePath(path string) string {
	lower := strings.ToLower(path)
	if strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml") {
		return path
	}
	return filepath.Join(path, "config.yaml")
}

func readOneFile(open openFileFn, path string) (map[string]any, error) {
	rc, err := open(path)
	if err != nil {
		return nil, fmt.Errorf("read config from %s: %w", path, err)
	}
	defer func() { _ = rc.Close() }()

	var parsed map[string]any
	dec := yaml.NewDecoder(rc)
	if err := dec.Decode(&parsed); err != nil {
		if errors.Is(err, io.EOF) {
			// empty file → no keys, no error.
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("read config from %s: %w", path, err)
	}
	if parsed == nil {
		return map[string]any{}, nil
	}
	return flatten(parsed), nil
}

// limitedReadCloser is an io.ReadCloser that caps reads at maxConfigBytes
// while still propagating Close to the underlying file.
type limitedReadCloser struct {
	r io.Reader
	c io.Closer
}

func (l *limitedReadCloser) Read(p []byte) (int, error) { return l.r.Read(p) }
func (l *limitedReadCloser) Close() error               { return l.c.Close() }

// osOpenFile is the production openFileFn. It enforces:
//   - file must be a regular file (not FIFO/device/socket) → ErrNotRegularFile
//   - file size <= maxConfigBytes → ErrConfigFileTooLarge (checked via Stat
//     before reading; also uses io.LimitReader as defense in depth)
func osOpenFile(path string) (io.ReadCloser, error) {
	clean := filepath.Clean(path)
	f, err := os.Open(clean)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("%s: %w", clean, ErrNotRegularFile)
	}
	if info.Size() > maxConfigBytes {
		_ = f.Close()
		return nil, fmt.Errorf("%s: %w", clean, ErrConfigFileTooLarge)
	}
	return &limitedReadCloser{
		r: io.LimitReader(f, maxConfigBytes),
		c: f,
	}, nil
}
