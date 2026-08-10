package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testHeader(name string) Header {
	return Header{Name: name, Ordering: "fifo", CreatedAt: 1_700_000_000_000, NextSeq: 1}
}

func enq(id string, seq uint64) Enqueue {
	return Enqueue{ID: id, Seq: seq, CreatedAt: 1, AvailableAt: 1, Body: json.RawMessage(`{"n":` + fmt.Sprint(seq) + `}`)}
}

// replay opens a log and collects every record handed to apply.
func replay(t *testing.T, dir, name string) (*Log, []Record) {
	t.Helper()
	var got []Record
	l, err := Open(dir, name, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return l, got
}

func TestCreateAppendReopen(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir, "q", testHeader("q"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	for i := 1; i <= 5; i++ {
		if err := l.Append(enq(fmt.Sprintf("id-%d", i), uint64(i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Append(Ack{ID: "id-2"}); err != nil {
		t.Fatalf("Append ack: %v", err)
	}
	if err := l.Append(Nack{ID: "id-3", AvailableAt: 555}); err != nil {
		t.Fatalf("Append nack: %v", err)
	}
	if l.Records() != 8 {
		t.Fatalf("Records() = %d, want 8", l.Records())
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2, got := replay(t, dir, "q")
	defer l2.Close()
	if len(got) != 8 {
		t.Fatalf("replayed %d records, want 8", len(got))
	}
	if _, ok := got[0].(Header); !ok {
		t.Fatalf("first record is %T, want Header", got[0])
	}
	if ack, ok := got[6].(Ack); !ok || ack.ID != "id-2" {
		t.Fatalf("record 6 = %+v, want Ack id-2", got[6])
	}
	nack, ok := got[7].(Nack)
	if !ok || nack.ID != "id-3" || nack.AvailableAt != 555 {
		t.Fatalf("record 7 = %+v, want Nack id-3 at 555", got[7])
	}
	if l2.Records() != 8 {
		t.Fatalf("reopened Records() = %d, want 8", l2.Records())
	}
}

func TestCreateIsExclusive(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir, "q", testHeader("q"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	l.Close()
	if _, err := Create(dir, "q", testHeader("q")); !errors.Is(err, ErrExists) {
		t.Fatalf("second Create returned %v, want ErrExists", err)
	}
}

func TestAppendAfterCloseFails(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir, "q", testHeader("q"))
	l.Close()
	if err := l.Append(Ack{ID: "x"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close returned %v, want ErrClosed", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestReopenIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir, "q", testHeader("q"))
	l.Append(enq("a", 1))
	l.Close()

	l1, first := replay(t, dir, "q")
	size1 := l1.Size()
	l1.Close()
	l2, second := replay(t, dir, "q")
	defer l2.Close()

	if len(first) != len(second) || l2.Size() != size1 {
		t.Fatalf("second open differs: %d records/%d bytes vs %d records/%d bytes",
			len(second), l2.Size(), len(first), size1)
	}
}

func TestEmptyAndShortFileHeader(t *testing.T) {
	dir := t.TempDir()
	for _, size := range []int{0, 3, fileHeaderSize - 1} {
		name := fmt.Sprintf("q%d", size)
		path := filepath.Join(dir, name+logExt)
		if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := Open(dir, name, func(Record) error { return nil })
		if !errors.Is(err, ErrIncompleteLog) {
			t.Fatalf("size %d: Open returned %v, want ErrIncompleteLog", size, err)
		}
	}
}

func TestHeaderOnlyFileIsIncomplete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "q"+logExt)
	if err := os.WriteFile(path, encodeFileHeader(), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, "q", func(Record) error { return nil }); !errors.Is(err, ErrIncompleteLog) {
		t.Fatalf("Open returned %v, want ErrIncompleteLog", err)
	}
}

func TestOpenRejectsForeignFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "q"+logExt)
	if err := os.WriteFile(path, []byte("this is definitely not a queuemaxxing log file"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, "q", func(Record) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "magic") {
		t.Fatalf("Open returned %v, want a magic-number error", err)
	}
}

// buildLog writes n enqueues after the header and returns the file path plus the byte
// offset at which the final record's frame begins.
func buildLog(t *testing.T, dir, name string, n int) (string, int64) {
	t.Helper()
	l, err := Create(dir, name, testHeader(name))
	if err != nil {
		t.Fatal(err)
	}
	var lastStart int64
	for i := 1; i <= n; i++ {
		lastStart = l.Size()
		if err := l.Append(enq(fmt.Sprintf("id-%d", i), uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, name+logExt), lastStart
}

func TestTornTailShortFrameIsTruncated(t *testing.T) {
	dir := t.TempDir()
	path, lastStart := buildLog(t, dir, "q", 4)

	// Cut the final frame in half: the file now ends part way through a record.
	info, _ := os.Stat(path)
	cut := lastStart + (info.Size()-lastStart)/2
	if err := os.Truncate(path, cut); err != nil {
		t.Fatal(err)
	}

	l, got := replay(t, dir, "q")
	defer l.Close()
	if len(got) != 4 {
		t.Fatalf("replayed %d records, want 4 (header + 3 intact enqueues)", len(got))
	}
	if l.Size() != lastStart {
		t.Fatalf("log size = %d, want %d (truncated to the last valid frame)", l.Size(), lastStart)
	}
	info, _ = os.Stat(path)
	if info.Size() != lastStart {
		t.Fatalf("file size = %d, want %d", info.Size(), lastStart)
	}

	// The truncated log must accept new appends and read back cleanly.
	if err := l.Append(enq("id-new", 99)); err != nil {
		t.Fatalf("Append after truncation: %v", err)
	}
	l.Close()
	l2, got2 := replay(t, dir, "q")
	defer l2.Close()
	if len(got2) != 5 {
		t.Fatalf("after re-append, replayed %d records, want 5", len(got2))
	}
}

func TestTornTailBadChecksumIsTruncated(t *testing.T) {
	dir := t.TempDir()
	path, lastStart := buildLog(t, dir, "q", 4)

	// Flip a payload byte in the final, complete frame. Nothing follows it, so this is
	// indistinguishable from a partial write and must be treated as a torn tail.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	at := lastStart + lengthSize + typeSize
	f.ReadAt(b[:], at)
	b[0] ^= 0xff
	f.WriteAt(b[:], at)
	f.Close()

	l, got := replay(t, dir, "q")
	defer l.Close()
	if len(got) != 4 {
		t.Fatalf("replayed %d records, want 4", len(got))
	}
	if l.Size() != lastStart {
		t.Fatalf("log size = %d, want %d", l.Size(), lastStart)
	}
}

func TestZeroFilledTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	path, _ := buildLog(t, dir, "q", 3)
	info, _ := os.Stat(path)
	end := info.Size()

	f, _ := os.OpenFile(path, os.O_RDWR, 0o644)
	f.WriteAt(make([]byte, 512), end)
	f.Close()

	l, got := replay(t, dir, "q")
	defer l.Close()
	if len(got) != 4 {
		t.Fatalf("replayed %d records, want 4", len(got))
	}
	if l.Size() != end {
		t.Fatalf("log size = %d, want %d", l.Size(), end)
	}
}

func TestNonZeroGarbageTailAborts(t *testing.T) {
	dir := t.TempDir()
	path, _ := buildLog(t, dir, "q", 3)
	info, _ := os.Stat(path)
	end := info.Size()

	// Garbage that is not a zero fill cannot be explained by a partial write, so recovery
	// must refuse rather than discard whatever it cannot parse.
	f, _ := os.OpenFile(path, os.O_RDWR, 0o644)
	junk := make([]byte, 64)
	for i := range junk {
		junk[i] = byte(i + 1)
	}
	f.WriteAt(junk, end)
	f.Close()

	_, err := Open(dir, "q", func(Record) error { return nil })
	var corrupt *CorruptError
	if !errors.As(err, &corrupt) {
		t.Fatalf("Open returned %v, want CorruptError", err)
	}
	if corrupt.Offset != end {
		t.Fatalf("corruption reported at offset %d, want %d", corrupt.Offset, end)
	}
}

func TestMidLogCorruptionAbortsRecovery(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir, "q", testHeader("q"))
	if err != nil {
		t.Fatal(err)
	}
	var secondStart int64
	for i := 1; i <= 5; i++ {
		if i == 2 {
			secondStart = l.Size()
		}
		l.Append(enq(fmt.Sprintf("id-%d", i), uint64(i)))
	}
	path := l.Path()
	l.Close()

	f, _ := os.OpenFile(path, os.O_RDWR, 0o644)
	var b [1]byte
	at := secondStart + lengthSize + typeSize
	f.ReadAt(b[:], at)
	b[0] ^= 0xff
	f.WriteAt(b[:], at)
	f.Close()

	_, err = Open(dir, "q", func(Record) error { return nil })
	var corrupt *CorruptError
	if !errors.As(err, &corrupt) {
		t.Fatalf("Open returned %v, want CorruptError", err)
	}
	if corrupt.Offset != secondStart {
		t.Fatalf("corruption reported at offset %d, want %d", corrupt.Offset, secondStart)
	}
	if !strings.Contains(corrupt.Error(), "checksum") {
		t.Fatalf("error %q does not mention the checksum", corrupt.Error())
	}

	// Recovery must not have modified the file when it aborts.
	info, _ := os.Stat(path)
	if info.Size() == secondStart {
		t.Fatal("aborting recovery truncated the log; it must be left untouched")
	}
}

func TestCorruptLengthInMiddleAborts(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir, "q", testHeader("q"))
	var secondStart int64
	for i := 1; i <= 5; i++ {
		if i == 2 {
			secondStart = l.Size()
		}
		l.Append(enq(fmt.Sprintf("id-%d", i), uint64(i)))
	}
	path := l.Path()
	l.Close()

	f, _ := os.OpenFile(path, os.O_RDWR, 0o644)
	f.WriteAt([]byte{0xff, 0xff, 0xff, 0xff}, secondStart) // length far past MaxRecordSize
	f.Close()

	_, err := Open(dir, "q", func(Record) error { return nil })
	var corrupt *CorruptError
	if !errors.As(err, &corrupt) {
		t.Fatalf("Open returned %v, want CorruptError", err)
	}
	if corrupt.Offset != secondStart {
		t.Fatalf("corruption reported at offset %d, want %d", corrupt.Offset, secondStart)
	}
}

func TestFirstRecordMustBeHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "q"+logExt)
	frame, _ := encodeFrame(Ack{ID: "x"})
	if err := os.WriteFile(path, append(encodeFileHeader(), frame...), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, "q", func(Record) error { return nil })
	var corrupt *CorruptError
	if !errors.As(err, &corrupt) || !strings.Contains(corrupt.Reason, "want HEADER") {
		t.Fatalf("Open returned %v, want a missing-header CorruptError", err)
	}
}

func TestApplyErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir, "q", testHeader("q"))
	l.Append(enq("a", 1))
	l.Close()

	sentinel := errors.New("apply failed")
	_, err := Open(dir, "q", func(r Record) error {
		if _, ok := r.(Enqueue); ok {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Open returned %v, want the apply error", err)
	}
}

func TestPartialWriteIsRolledBack(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir, "q", testHeader("q"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(enq("good", 1)); err != nil {
		t.Fatal(err)
	}
	before := l.Size()
	records := l.Records()

	// Write half the frame to the real file, then fail: the file now holds torn bytes
	// that the rollback must remove.
	failure := errors.New("simulated device failure")
	l.writeAt = func(f *os.File, p []byte, off int64) (int, error) {
		n, _ := f.WriteAt(p[:len(p)/2], off)
		return n, failure
	}
	if err := l.Append(enq("torn", 2)); !errors.Is(err, failure) {
		t.Fatalf("Append returned %v, want the injected failure", err)
	}
	if l.Size() != before || l.Records() != records {
		t.Fatalf("after rollback size=%d records=%d, want %d and %d", l.Size(), l.Records(), before, records)
	}
	info, _ := os.Stat(l.Path())
	if info.Size() != before {
		t.Fatalf("file is %d bytes after rollback, want %d", info.Size(), before)
	}

	// The queue must remain usable, and the log must still replay cleanly.
	l.writeAt = nil
	if err := l.Append(enq("after", 3)); err != nil {
		t.Fatalf("Append after rollback: %v", err)
	}
	l.Close()
	l2, got := replay(t, dir, "q")
	defer l2.Close()
	if len(got) != 3 {
		t.Fatalf("replayed %d records, want 3 (header, good, after)", len(got))
	}
	if e, ok := got[2].(Enqueue); !ok || e.ID != "after" {
		t.Fatalf("last record = %+v, want enqueue of \"after\"", got[2])
	}
}

func TestSyncFailureIsRolledBack(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir, "q", testHeader("q"))
	l.Append(enq("good", 1))
	before := l.Size()

	failure := errors.New("simulated sync failure")
	l.sync = func(*os.File) error { return failure }
	if err := l.Append(enq("unsynced", 2)); !errors.Is(err, failure) {
		t.Fatalf("Append returned %v, want the injected failure", err)
	}
	if l.Size() != before {
		t.Fatalf("size = %d after rollback, want %d", l.Size(), before)
	}
	info, _ := os.Stat(l.Path())
	if info.Size() != before {
		t.Fatalf("file is %d bytes, want %d", info.Size(), before)
	}
	l.sync = nil
	l.Close()
}

func TestFailedRollbackBreaksTheLog(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir, "q", testHeader("q"))
	l.Append(enq("good", 1))

	// Closing the descriptor makes both the write and the rollback truncate fail, which is
	// the case where the log may hold torn bytes we could not remove.
	l.f.Close()
	err := l.Append(enq("doomed", 2))
	if !errors.Is(err, ErrLogBroken) {
		t.Fatalf("Append returned %v, want ErrLogBroken", err)
	}
	if err := l.Append(enq("later", 3)); !errors.Is(err, ErrLogBroken) {
		t.Fatalf("a broken log accepted a later append: %v", err)
	}
}

func TestLargeLogRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir, "q", testHeader("q"))
	if err != nil {
		t.Fatal(err)
	}
	// What this test exercises is recovery across many frames, not fsync throughput, which
	// TestFsyncedAppendThroughput covers separately. Suppressing the per-append sync keeps
	// this from costing 20000 disk flushes; Close performs a real sync at the end.
	const n = 20000
	l.sync = func(*os.File) error { return nil }
	for i := 1; i <= n; i++ {
		if err := l.Append(enq(fmt.Sprintf("id-%d", i), uint64(i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	l.sync = nil
	l.Close()

	count := 0
	seq := uint64(0)
	l2, err := Open(dir, "q", func(r Record) error {
		if e, ok := r.(Enqueue); ok {
			seq++
			if e.Seq != seq {
				return fmt.Errorf("record %d out of order: seq %d", count, e.Seq)
			}
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l2.Close()
	if count != n+1 {
		t.Fatalf("replayed %d records, want %d", count, n+1)
	}
}

// TestFsyncedAppendThroughput pays for real fsyncs on a modest number of records. It asserts
// only correctness; the timing it reports is what bounds per-queue write throughput, since
// every append flushes before the queue returns success to its caller.
func TestFsyncedAppendThroughput(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir, "q", testHeader("q"))
	if err != nil {
		t.Fatal(err)
	}
	const n = 200
	start := time.Now()
	for i := 1; i <= n; i++ {
		if err := l.Append(enq(fmt.Sprintf("id-%d", i), uint64(i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	l.Close()
	t.Logf("%d fsynced appends in %v (%.0f/s)", n, elapsed, float64(n)/elapsed.Seconds())

	l2, got := replay(t, dir, "q")
	defer l2.Close()
	if len(got) != n+1 {
		t.Fatalf("replayed %d records, want %d", len(got), n+1)
	}
}

func TestLargePayloadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir, "q", testHeader("q"))
	body, _ := json.Marshal(strings.Repeat("p", 1<<20))
	if err := l.Append(Enqueue{ID: "big", Seq: 1, CreatedAt: 1, AvailableAt: 1, Body: body}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	l.Close()

	l2, got := replay(t, dir, "q")
	defer l2.Close()
	e, ok := got[1].(Enqueue)
	if !ok || len(e.Body) != len(body) {
		t.Fatalf("large body did not round trip: %d bytes back, want %d", len(e.Body), len(body))
	}
}

func TestListLogsAndRemove(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"alpha", "beta"} {
		l, err := Create(dir, name, testHeader(name))
		if err != nil {
			t.Fatal(err)
		}
		l.Close()
	}
	os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignored"), 0o644)

	names, err := ListLogs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Fatalf("ListLogs returned %v, want two queue names", names)
	}
	if err := Remove(dir, "alpha"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	names, _ = ListLogs(dir)
	if len(names) != 1 || names[0] != "beta" {
		t.Fatalf("after Remove, ListLogs returned %v, want [beta]", names)
	}
}

func TestCleanTemps(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir, "q", testHeader("q"))
	l.Close()
	tmp := filepath.Join(dir, "q"+tmpExt)
	if err := os.WriteFile(tmp, []byte("uncommitted compaction"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := CleanTemps(dir); err != nil {
		t.Fatalf("CleanTemps: %v", err)
	}
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("temporary file survived CleanTemps")
	}
	if _, err := os.Stat(filepath.Join(dir, "q"+logExt)); err != nil {
		t.Fatalf("CleanTemps removed the real log: %v", err)
	}
}

func TestOpenMissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := Open(dir, "nope", func(Record) error { return nil })
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open returned %v, want os.ErrNotExist", err)
	}
}

func TestScannerReportsCleanEOF(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir, "q", testHeader("q"))
	l.Close()
	l2, err := Open(dir, "q", func(Record) error { return nil })
	if err != nil {
		t.Fatalf("Open on a header-only-plus-record log: %v", err)
	}
	defer l2.Close()
	if l2.Records() != 1 {
		t.Fatalf("Records() = %d, want 1", l2.Records())
	}
	_ = io.EOF
}
