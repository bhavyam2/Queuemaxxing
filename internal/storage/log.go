package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	logExt = ".log"
	tmpExt = ".log.tmp"
)

var (
	// ErrExists reports an attempt to create a log that is already on disk.
	ErrExists = errors.New("queue log already exists")

	// ErrClosed reports an append to a log that has been closed.
	ErrClosed = errors.New("log is closed")

	// ErrLogBroken reports a log that could not be rolled back after a failed append and
	// must not be extended further. The queue holding it refuses writes from then on.
	ErrLogBroken = errors.New("log is unwritable")
)

// Log is an append-only file of framed records. It has exactly one writer: the owning
// queue holds its mutex across every call, which is also what fsync-before-2xx costs.
type Log struct {
	f       *os.File
	path    string
	dir     string
	size    int64
	records int
	broken  error

	// Test seams for exercising partial writes and sync failures against a real file.
	writeAt func(f *os.File, p []byte, off int64) (int, error)
	sync    func(f *os.File) error
}

func EnsureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return syncDir(dir)
}

// Create writes a new log containing the file header and hdr. The data directory is fsynced
// afterwards: without it the file's directory entry is not durable, so a newly created queue
// can vanish entirely even though its contents were flushed.
func Create(dir, name string, hdr Header) (*Log, error) {
	path := filepath.Join(dir, name+logExt)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("%s: %w", path, ErrExists)
		}
		return nil, err
	}

	l := &Log{f: f, path: path, dir: dir}
	if _, err := f.WriteAt(encodeFileHeader(), 0); err != nil {
		l.discard()
		return nil, err
	}
	l.size = fileHeaderSize
	if err := l.Append(hdr); err != nil {
		l.discard()
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		l.discard()
		return nil, err
	}
	return l, nil
}

func (l *Log) discard() {
	l.f.Close()
	os.Remove(l.path)
}

// Append encodes rec, writes it, and fsyncs before returning. A failed write or sync is
// rolled back to the pre-append offset so the log never carries a half-record in its middle;
// the caller has not returned 2xx for the operation, so discarding it is correct.
func (l *Log) Append(rec Record) error {
	if l.broken != nil {
		return l.broken
	}
	if l.f == nil {
		return ErrClosed
	}
	frame, err := encodeFrame(rec)
	if err != nil {
		return err
	}

	at := l.size
	write := l.writeAt
	if write == nil {
		write = func(f *os.File, p []byte, off int64) (int, error) { return f.WriteAt(p, off) }
	}
	sync := l.sync
	if sync == nil {
		sync = func(f *os.File) error { return f.Sync() }
	}

	n, err := write(l.f, frame, at)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return l.rollback(at, err)
	}
	if err := sync(l.f); err != nil {
		return l.rollback(at, err)
	}

	l.size = at + int64(len(frame))
	l.records++
	return nil
}

func (l *Log) rollback(to int64, cause error) error {
	if err := l.f.Truncate(to); err != nil {
		l.broken = fmt.Errorf("%w: %s: rollback to offset %d failed: %v (after %v)", ErrLogBroken, l.path, to, err, cause)
		return l.broken
	}
	if err := l.f.Sync(); err != nil {
		l.broken = fmt.Errorf("%w: %s: sync after rollback to offset %d failed: %v (after %v)", ErrLogBroken, l.path, to, err, cause)
		return l.broken
	}
	return cause
}

func (l *Log) Records() int { return l.records }
func (l *Log) Size() int64  { return l.size }
func (l *Log) Path() string { return l.path }

func (l *Log) Close() error {
	if l.f == nil {
		return nil
	}
	err := l.f.Sync()
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f = nil
	return err
}

// Remove deletes a queue's log and makes the deletion durable.
func Remove(dir, name string) error {
	if err := os.Remove(filepath.Join(dir, name+logExt)); err != nil {
		return err
	}
	return syncDir(dir)
}

// ListLogs returns the queue names present in dir.
func ListLogs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), logExt) {
			continue
		}
		names = append(names, strings.TrimSuffix(e.Name(), logExt))
	}
	return names, nil
}

// CleanTemps removes compaction temporaries left by a crash. The rename is the commit point,
// so a surviving temporary is by definition uncommitted and can never be adopted.
func CleanTemps(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	removed := false
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), tmpExt) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
		removed = true
	}
	if removed {
		return syncDir(dir)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
