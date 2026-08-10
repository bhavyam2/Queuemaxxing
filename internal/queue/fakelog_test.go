package queue

import (
	"sync"

	"queuemaxxing/internal/storage"
)

// fakeLog stands in for a real log in tests that are about queue semantics rather than
// durability. It skips the fsync that bounds the real log at a few hundred appends per
// second, which keeps ordering and concurrency tests fast. Tests that are about persistence
// use a real storage.Log on a temporary directory instead.
type fakeLog struct {
	mu          sync.Mutex
	records     []storage.Record
	failWith    error
	closed      bool
	compactions int
}

func (f *fakeLog) Append(rec storage.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return f.failWith
	}
	f.records = append(f.records, rec)
	return nil
}

func (f *fakeLog) Records() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

func (f *fakeLog) Compact(hdr storage.Header, live []storage.Enqueue) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return f.failWith
	}
	f.records = f.records[:0]
	f.records = append(f.records, hdr)
	for _, e := range live {
		f.records = append(f.records, e)
	}
	f.compactions++
	return nil
}

func (f *fakeLog) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeLog) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failWith = err
}

func (f *fakeLog) snapshot() []storage.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]storage.Record, len(f.records))
	copy(out, f.records)
	return out
}
