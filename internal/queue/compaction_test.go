package queue

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func logSize(t *testing.T, dir string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, "q.log"))
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func newCompactingQueue(t *testing.T, threshold int) (*Queue, string) {
	t.Helper()
	dir := t.TempDir()
	q, err := createQueue(dir, Config{Name: "q", Ordering: FIFO, CreatedAt: time.Now()}, threshold)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	return q, dir
}

// TestCompactionShrinksLogAndPreservesState drives a queue past the threshold with messages
// that are all acked, so the compacted log should end up nearly empty while the counters and
// sequence floor survive.
func TestCompactionShrinksLogAndPreservesState(t *testing.T) {
	q, dir := newCompactingQueue(t, 20)

	for i := 0; i < 40; i++ {
		send(t, q, fmt.Sprintf("m%d", i), 0)
		d, ok := receiveNow(t, q)
		if !ok {
			t.Fatalf("no message on iteration %d", i)
		}
		if err := q.Ack(d.Receipt); err != nil {
			t.Fatal(err)
		}
	}

	before := q.Stats()
	if before.TotalEnqueued != 40 || before.TotalAcked != 40 {
		t.Fatalf("stats = %+v, want 40 enqueued and 40 acked", before)
	}
	// Without compaction the log would hold 81 records: a header plus an enqueue and an ack
	// for every message. Compaction keeps it bounded near the threshold instead.
	if got := q.log.Records(); got > 25 {
		t.Fatalf("log holds %d records, want it bounded near the threshold of 20", got)
	}

	// Send one more so the sequence floor is observable after a restart.
	send(t, q, "after", 0)
	crash(q)

	r := reopen(t, dir)
	after := r.Stats()
	if after.TotalEnqueued != 41 || after.TotalAcked != 40 {
		t.Fatalf("post-restart stats = %+v, want 41 enqueued and 40 acked; compaction lost the counters", after)
	}
	if after.Ready != 1 {
		t.Fatalf("post-restart ready = %d, want 1", after.Ready)
	}
	d, ok := receiveNow(t, r)
	if !ok {
		t.Fatal("no message after restart")
	}
	// Sequence numbers must not restart at 1 just because the acked records were dropped.
	if d.Seq != 41 {
		t.Fatalf("recovered sequence = %d, want 41; the sequence floor was not carried through compaction", d.Seq)
	}
}

func TestCompactionPreservesReadyDelayedAndInFlight(t *testing.T) {
	q, dir := newCompactingQueue(t, 8)

	send(t, q, "ready", 0)
	if _, err := q.Enqueue(body("delayed"), 3, time.Hour); err != nil {
		t.Fatal(err)
	}
	send(t, q, "inflight", 0)

	// Under FIFO the two undelayed messages come out in sequence order. Hold the second in
	// flight and nack the first straight back, leaving one of each state behind.
	first, ok := receiveNow(t, q)
	if !ok || label(t, first) != "ready" {
		t.Fatalf("first receive = %q, want ready", label(t, first))
	}
	second, ok := receiveNow(t, q)
	if !ok || label(t, second) != "inflight" {
		t.Fatalf("second receive = %q, want inflight", label(t, second))
	}
	if err := q.Nack(first.Receipt, 0); err != nil {
		t.Fatal(err)
	}

	// Churn at a higher priority so this traffic is always delivered ahead of the message
	// parked in the ready heap, and trips the compaction threshold without disturbing it.
	for i := 0; i < 12; i++ {
		if _, err := q.Enqueue(body(fmt.Sprintf("churn%d", i)), 100, 0); err != nil {
			t.Fatal(err)
		}
		d, ok := receiveNow(t, q)
		if !ok || label(t, d) != fmt.Sprintf("churn%d", i) {
			t.Fatalf("churn %d received %q", i, label(t, d))
		}
		if err := q.Ack(d.Receipt); err != nil {
			t.Fatal(err)
		}
	}

	before := q.Stats()
	crash(q)

	r := reopen(t, dir)
	after := r.Stats()
	if after.Delayed != 1 {
		t.Fatalf("post-restart stats = %+v, want the delayed message preserved through compaction", after)
	}
	// The in-flight message and the ready one both come back available.
	if after.Ready != before.Ready+before.InFlight {
		t.Fatalf("post-restart ready = %d, want %d (ready %d plus in-flight %d)",
			after.Ready, before.Ready+before.InFlight, before.Ready, before.InFlight)
	}
	if after.TotalEnqueued != before.TotalEnqueued || after.TotalAcked != before.TotalAcked {
		t.Fatalf("counters drifted across compaction: %+v then %+v", before, after)
	}
}

// TestCompactionDoesNotThrash guards the halving condition. A queue whose live set is larger
// than the threshold would otherwise recompact on every single append, because the freshly
// written log is already over the limit.
func TestCompactionDoesNotThrash(t *testing.T) {
	log := &fakeLog{}
	q := newQueue(Config{Name: "q", Ordering: FIFO, CreatedAt: time.Now()}, log, 1, 0, 0)
	q.compactThreshold = 10
	q.start()
	defer q.Close()

	// Never acked, so every message stays live and compaction can never shrink the log.
	for i := 0; i < 200; i++ {
		send(t, q, fmt.Sprintf("m%d", i), 0)
	}
	log.mu.Lock()
	compactions := log.compactions
	log.mu.Unlock()

	if compactions > 5 {
		t.Fatalf("compacted %d times over 200 appends with no acks; the halving guard is not working", compactions)
	}
}

func TestCompactionOnCleanShutdown(t *testing.T) {
	dir := t.TempDir()
	q, err := createQueue(dir, Config{Name: "q", Ordering: FIFO, CreatedAt: time.Now()}, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		send(t, q, fmt.Sprintf("m%d", i), 0)
		d, _ := receiveNow(t, q)
		q.Ack(d.Receipt)
	}
	sizeBefore := logSize(t, dir)

	// The threshold was never reached, so only the clean shutdown can shrink this.
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	sizeAfter := logSize(t, dir)
	if sizeAfter >= sizeBefore {
		t.Fatalf("log is %d bytes after a clean shutdown, was %d; shutdown did not compact", sizeAfter, sizeBefore)
	}

	r := reopen(t, dir)
	st := r.Stats()
	if st.TotalEnqueued != 30 || st.TotalAcked != 30 || st.Ready != 0 {
		t.Fatalf("stats after reopening a shutdown-compacted log = %+v, want 30/30 and nothing ready", st)
	}
}

// TestCrashDuringCompactionLeavesOldLogIntact simulates a crash before the rename by leaving a
// temporary behind. The commit point is the rename, so the original log must still be the one
// that recovers and the temporary must be discarded.
func TestCrashDuringCompactionLeavesOldLogIntact(t *testing.T) {
	q, dir := newCompactingQueue(t, 0)
	send(t, q, "a", 0)
	send(t, q, "b", 0)
	crash(q)

	original := logSize(t, dir)
	tmp := filepath.Join(dir, "q.log.tmp")
	if err := os.WriteFile(tmp, []byte("half-written replacement"), 0o644); err != nil {
		t.Fatal(err)
	}

	m, err := NewManager(dir, 0)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer m.Close()

	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("uncommitted compaction temporary was not discarded")
	}
	if logSize(t, dir) != original {
		t.Fatal("the original log was modified by an uncommitted compaction")
	}
	r, err := m.Get("q")
	if err != nil {
		t.Fatal(err)
	}
	assertOrder(t, drainLabels(t, r), []string{"a", "b"})
}

func TestCompactedLogIsAppendableAndRecoverable(t *testing.T) {
	q, dir := newCompactingQueue(t, 10)
	for i := 0; i < 25; i++ {
		send(t, q, fmt.Sprintf("m%d", i), 0)
		d, _ := receiveNow(t, q)
		q.Ack(d.Receipt)
	}
	// Writing after a compaction must land in the new file, not the replaced inode.
	send(t, q, "post-compaction", 0)
	crash(q)

	r := reopen(t, dir)
	assertOrder(t, drainLabels(t, r), []string{"post-compaction"})
}
