package queue

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"queuemaxxing/internal/storage"
)

// crash abandons the queue object without a clean shutdown, which is what a kill -9 leaves
// behind: the log file is whatever was fsynced, and no shutdown work ran.
func crash(q *Queue) {
	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
	if q.done != nil {
		close(q.done)
	}
	q.wg.Wait()
	// Deliberately no log.Close(): a crashed process does not flush or compact.
}

func reopen(t *testing.T, dir string) *Queue {
	t.Helper()
	q, err := openQueue(dir, "q", 0)
	if err != nil {
		t.Fatalf("openQueue: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

func TestRecoverEnqueuedMessages(t *testing.T) {
	q, dir := newDiskQueue(t, FIFO)
	send(t, q, "a", 0)
	send(t, q, "b", 0)
	crash(q)

	r := reopen(t, dir)
	if st := r.Stats(); st.Ready != 2 || st.TotalEnqueued != 2 {
		t.Fatalf("stats = %+v, want ready 2 enqueued 2", st)
	}
	assertOrder(t, drainLabels(t, r), []string{"a", "b"})
}

func TestAckedMessageIsNotRedelivered(t *testing.T) {
	q, dir := newDiskQueue(t, FIFO)
	send(t, q, "keep", 0)
	send(t, q, "drop", 0)

	first, _ := receiveNow(t, q)
	if label(t, first) != "keep" {
		t.Fatalf("received %q first, want keep", label(t, first))
	}
	if err := q.Ack(first.Receipt); err != nil {
		t.Fatal(err)
	}
	crash(q)

	r := reopen(t, dir)
	st := r.Stats()
	if st.TotalAcked != 1 {
		t.Fatalf("stats = %+v, want one ack carried across the restart", st)
	}
	assertOrder(t, drainLabels(t, r), []string{"drop"})
}

// TestRestartBehaviourTable is the case the specification calls out: a ready message stays
// available, an in-flight message comes back for redelivery, a delayed message stays delayed,
// and an acked message is gone for good.
func TestRestartBehaviourTable(t *testing.T) {
	q, dir := newDiskQueue(t, FIFO)

	send(t, q, "A", 0) // stays ready
	send(t, q, "B", 0) // will be in flight at crash time
	if _, err := q.Enqueue(body("C"), 0, 10*time.Second); err != nil {
		t.Fatal(err) // delayed well past the test
	}
	send(t, q, "D", 0) // will be acked

	// Draw A, B and D in FIFO order, then put A back so it is ready at crash time, ack D so
	// it is terminal, and leave B in flight.
	receipts := make(map[string]string)
	for i := 0; i < 3; i++ {
		d, ok := receiveNow(t, q)
		if !ok {
			t.Fatalf("expected a message on receive %d", i)
		}
		receipts[label(t, d)] = d.Receipt
	}
	for _, l := range []string{"A", "B", "D"} {
		if receipts[l] == "" {
			t.Fatalf("did not receive %s", l)
		}
	}
	if err := q.Nack(receipts["A"], 0); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(receipts["D"]); err != nil {
		t.Fatal(err)
	}
	bReceipt := receipts["B"]

	before := q.Stats()
	if before.Ready != 1 || before.InFlight != 1 || before.Delayed != 1 {
		t.Fatalf("pre-crash stats = %+v, want ready 1 (A) in-flight 1 (B) delayed 1 (C)", before)
	}
	crash(q)

	r := reopen(t, dir)
	after := r.Stats()
	if after.Ready != 2 || after.Delayed != 1 || after.InFlight != 0 {
		t.Fatalf("post-restart stats = %+v, want ready 2 (A and B) delayed 1 (C) in-flight 0", after)
	}
	if after.TotalAcked != 1 {
		t.Fatalf("post-restart acked = %d, want 1", after.TotalAcked)
	}

	// A was never delivered and B is being redelivered; both are available, and D is gone.
	assertOrder(t, drainLabels(t, r), []string{"A", "B"})

	// The stale receipt from before the crash cannot ack the redelivery.
	if err := r.Ack(bReceipt); !errors.Is(err, ErrReceiptNotFound) {
		t.Fatalf("pre-crash receipt returned %v after restart, want ErrReceiptNotFound", err)
	}
}

// TestReceiveCountResetsOnRestart pins the documented cost of not persisting in-flight state.
func TestReceiveCountResetsOnRestart(t *testing.T) {
	q, dir := newDiskQueue(t, FIFO)
	send(t, q, "a", 0)

	d, _ := receiveNow(t, q)
	if d.ReceiveCount != 1 {
		t.Fatalf("ReceiveCount = %d, want 1", d.ReceiveCount)
	}
	crash(q)

	r := reopen(t, dir)
	again, ok := receiveNow(t, r)
	if !ok {
		t.Fatal("in-flight message was not redelivered after restart")
	}
	if again.ReceiveCount != 1 {
		t.Fatalf("ReceiveCount after restart = %d, want 1; delivery attempts are not persisted", again.ReceiveCount)
	}
}

func TestDelayedMessageStaysDelayedThenBecomesAvailable(t *testing.T) {
	q, dir := newDiskQueue(t, FIFO)
	if _, err := q.Enqueue(body("later"), 0, 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	crash(q)

	r := reopen(t, dir)
	if st := r.Stats(); st.Delayed != 1 || st.Ready != 0 {
		t.Fatalf("stats = %+v, want the message still delayed after restart", st)
	}
	if _, ok := receiveNow(t, r); ok {
		t.Fatal("a delayed message was delivered immediately after restart")
	}
	waitFor(t, "the recovered delay to elapse", func() bool { return r.Stats().Ready == 1 })
	if _, ok := receiveNow(t, r); !ok {
		t.Fatal("message never became available after restart")
	}
}

// TestElapsedDelayRecoversAsReady covers a delay that expired while the process was down.
func TestElapsedDelayRecoversAsReady(t *testing.T) {
	q, dir := newDiskQueue(t, FIFO)
	if _, err := q.Enqueue(body("short"), 0, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	crash(q)
	time.Sleep(40 * time.Millisecond)

	r := reopen(t, dir)
	if st := r.Stats(); st.Ready != 1 || st.Delayed != 0 {
		t.Fatalf("stats = %+v, want the elapsed delay to recover as ready", st)
	}
}

func TestNackWithDelaySurvivesRestart(t *testing.T) {
	q, dir := newDiskQueue(t, FIFO)
	send(t, q, "retry", 0)
	d, _ := receiveNow(t, q)
	if err := q.Nack(d.Receipt, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	crash(q)

	r := reopen(t, dir)
	if st := r.Stats(); st.Delayed != 1 || st.Ready != 0 {
		t.Fatalf("stats = %+v, want the nack delay to survive the restart", st)
	}
}

func TestImmediateNackSurvivesRestart(t *testing.T) {
	q, dir := newDiskQueue(t, FIFO)
	send(t, q, "retry", 0)
	d, _ := receiveNow(t, q)
	if err := q.Nack(d.Receipt, 0); err != nil {
		t.Fatal(err)
	}
	crash(q)

	r := reopen(t, dir)
	if st := r.Stats(); st.Ready != 1 {
		t.Fatalf("stats = %+v, want the message ready after restart", st)
	}
}

// TestSequenceNumbersSurviveRestart checks that ordering is stable across a restart, which is
// the reason sequence numbers are persisted rather than re-derived.
func TestSequenceNumbersSurviveRestart(t *testing.T) {
	q, dir := newDiskQueue(t, LIFO)
	send(t, q, "first", 0)
	send(t, q, "second", 0)
	crash(q)

	r := reopen(t, dir)
	send(t, r, "third", 0)
	assertOrder(t, drainLabels(t, r), []string{"third", "second", "first"})
}

func TestRecoveryPreservesPriorityAndBody(t *testing.T) {
	q, dir := newDiskQueue(t, FIFO)
	send(t, q, "low", 1)
	send(t, q, "high", 9)
	crash(q)

	r := reopen(t, dir)
	d, ok := receiveNow(t, r)
	if !ok {
		t.Fatal("no message")
	}
	if label(t, d) != "high" || d.Priority != 9 {
		t.Fatalf("recovered %q with priority %d, want high with 9", label(t, d), d.Priority)
	}
}

func TestRecoveryOfEmptyQueue(t *testing.T) {
	q, dir := newDiskQueue(t, FIFO)
	crash(q)

	r := reopen(t, dir)
	if st := r.Stats(); st.Ready != 0 || st.TotalEnqueued != 0 {
		t.Fatalf("stats = %+v, want an empty queue", st)
	}
	if _, err := r.Enqueue(body("after"), 0, 0); err != nil {
		t.Fatalf("recovered empty queue rejected an enqueue: %v", err)
	}
}

func TestRecoveryPreservesOrderingConfig(t *testing.T) {
	q, dir := newDiskQueue(t, LIFO)
	crash(q)

	r := reopen(t, dir)
	if r.Config().Ordering != LIFO {
		t.Fatalf("recovered ordering = %q, want lifo", r.Config().Ordering)
	}
}

// TestTornTailAfterCrashIsDiscarded simulates a kill -9 partway through the final append by
// truncating the log mid-frame, which is exactly what an interrupted write leaves.
func TestTornTailAfterCrashIsDiscarded(t *testing.T) {
	q, dir := newDiskQueue(t, FIFO)
	send(t, q, "durable", 0)
	send(t, q, "interrupted", 0)
	crash(q)

	path := filepath.Join(dir, "q.log")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-20); err != nil {
		t.Fatal(err)
	}

	r := reopen(t, dir)
	if st := r.Stats(); st.Ready != 1 {
		t.Fatalf("stats = %+v, want only the fully written message", st)
	}
	assertOrder(t, drainLabels(t, r), []string{"durable"})

	// The truncated log must still accept writes.
	if _, err := r.Enqueue(body("after"), 0, 0); err != nil {
		t.Fatalf("enqueue after tail truncation: %v", err)
	}
}

func TestManagerRecoversAllQueues(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	orders, err := m.Create("orders", FIFO)
	if err != nil {
		t.Fatal(err)
	}
	events, err := m.Create("events", LIFO)
	if err != nil {
		t.Fatal(err)
	}
	send(t, orders, "o1", 0)
	send(t, events, "e1", 0)
	send(t, events, "e2", 0)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	m2, err := NewManager(dir, 0)
	if err != nil {
		t.Fatalf("NewManager after restart: %v", err)
	}
	defer m2.Close()

	if got := len(m2.List()); got != 2 {
		t.Fatalf("recovered %d queues, want 2", got)
	}
	ev, err := m2.Get("events")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Config().Ordering != LIFO {
		t.Fatalf("events ordering = %q, want lifo", ev.Config().Ordering)
	}
	assertOrder(t, drainLabels(t, ev), []string{"e2", "e1"})
}

func TestManagerCreateGetDelete(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if _, err := m.Create("orders", FIFO); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create("orders", FIFO); !errors.Is(err, ErrQueueExists) {
		t.Fatalf("duplicate Create returned %v, want ErrQueueExists", err)
	}
	if _, err := m.Create("../escape", FIFO); err == nil {
		t.Fatal("Create accepted a name that escapes the data directory")
	}
	if _, err := m.Get("missing"); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("Get returned %v, want ErrQueueNotFound", err)
	}

	if err := m.Delete("orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "orders.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Delete left the log file behind")
	}
	if err := m.Delete("orders"); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("second Delete returned %v, want ErrQueueNotFound", err)
	}

	// The name is free again once the queue is gone.
	if _, err := m.Create("orders", LIFO); err != nil {
		t.Fatalf("recreating a deleted queue: %v", err)
	}
}

// TestDeleteReleasesLongPollers checks that removing a queue does not strand its consumers.
func TestDeleteReleasesLongPollers(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	q, err := m.Create("orders", FIFO)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := q.Receive(context.Background(), testVisibility, 5*time.Second)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)

	if err := m.Delete("orders"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrQueueDeleted) {
			t.Fatalf("Receive returned %v, want ErrQueueDeleted", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deleting the queue left a long poller blocked")
	}
}

func TestManagerRefusesToStartOnCorruptLog(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	q, _ := m.Create("orders", FIFO)
	for i := 0; i < 5; i++ {
		send(t, q, "m", 0)
	}
	m.Close()

	// Corrupt a record in the middle, which represents acknowledged data going missing.
	path := filepath.Join(dir, "orders.log")
	f, _ := os.OpenFile(path, os.O_RDWR, 0o644)
	var b [1]byte
	f.ReadAt(b[:], 64)
	b[0] ^= 0xff
	f.WriteAt(b[:], 64)
	f.Close()

	if _, err := NewManager(dir, 0); err == nil {
		t.Fatal("NewManager started despite corruption in the middle of a log")
	}
}

func TestManagerDiscardsIncompleteLog(t *testing.T) {
	dir := t.TempDir()
	// A file too short to hold the file header is a queue whose creation never completed.
	if err := os.WriteFile(filepath.Join(dir, "half.log"), []byte("QMX"), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(dir, 0)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer m.Close()
	if got := len(m.List()); got != 0 {
		t.Fatalf("recovered %d queues, want 0", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "half.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("incomplete log was not discarded")
	}
}

func TestManagerCleansCompactionTemporaries(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	m.Create("orders", FIFO)
	m.Close()

	tmp := filepath.Join(dir, "orders.log.tmp")
	if err := os.WriteFile(tmp, []byte("uncommitted"), 0o644); err != nil {
		t.Fatal(err)
	}
	m2, err := NewManager(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("compaction temporary survived startup")
	}
}

func TestRecoveryRejectsUnknownOrdering(t *testing.T) {
	dir := t.TempDir()
	hdr := storage.Header{Name: "q", Ordering: "sideways", CreatedAt: 1, NextSeq: 1}
	l, err := storage.Create(dir, "q", hdr)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	if _, err := openQueue(dir, "q", 0); err == nil {
		t.Fatal("openQueue accepted an unknown ordering")
	}
}
