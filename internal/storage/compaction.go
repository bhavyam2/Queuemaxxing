package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// Compact rewrites the log with only the records still needed: a header carrying the counters
// and sequence floor, followed by one ENQUEUE per live message.
//
// Crash safety rests on rename being the single commit point. A crash before step 4 leaves
// the old log untouched plus a stray temporary that startup deletes; a crash after it leaves
// the complete new log. There is no instant at which a reader could observe a mixture of the
// two, because the replacement is fully written and fsynced before the rename, and the rename
// is durable once the directory is fsynced.
func (l *Log) Compact(hdr Header, live []Enqueue) error {
	if l.broken != nil {
		return l.broken
	}
	if l.f == nil {
		return ErrClosed
	}

	tmpPath := filepath.Join(l.dir, hdr.Name+tmpExt)
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	cleanup := func() {
		tmp.Close()
		os.Remove(tmpPath)
	}

	// 1. Build the replacement in full.
	buf := encodeFileHeader()
	hdrFrame, err := encodeFrame(hdr)
	if err != nil {
		cleanup()
		return err
	}
	buf = append(buf, hdrFrame...)
	for _, e := range live {
		frame, err := encodeFrame(e)
		if err != nil {
			cleanup()
			return fmt.Errorf("compact %s: %w", hdr.Name, err)
		}
		buf = append(buf, frame...)
	}
	if _, err := tmp.WriteAt(buf, 0); err != nil {
		cleanup()
		return err
	}

	// 2. Flush its contents, then 3. close it before the rename.
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// 4. Commit.
	if err := os.Rename(tmpPath, l.path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	// 5. Make the rename itself durable; without this the directory entry can revert.
	if err := syncDir(l.dir); err != nil {
		return err
	}

	// 6. Adopt the new file. The old descriptor still points at the replaced inode, so it
	// is closed only after the swap and no writer ever targets the unlinked file.
	replacement, err := os.OpenFile(l.path, os.O_RDWR, 0o644)
	if err != nil {
		l.broken = fmt.Errorf("%w: %s: compacted log could not be reopened: %v", ErrLogBroken, l.path, err)
		return l.broken
	}
	old := l.f
	l.f = replacement
	l.size = int64(len(buf))
	l.records = 1 + len(live)
	old.Close()
	return nil
}
