package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Tests use millisecond-scale durations because the engine takes time.Duration directly; the
// HTTP layer is the only place that converts from the API's integer seconds. That keeps the
// scheduler's real timer behaviour under test without a slow suite.
const (
	tick  = 40 * time.Millisecond
	grace = 400 * time.Millisecond
)

// waitFor polls a predicate up to grace. It is used only to observe an asynchronous
// promotion; the code under test is timer driven, not polled.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func receiveNow(t *testing.T, q *Queue) (Delivery, bool) {
	t.Helper()
	d, ok, err := q.Receive(context.Background(), testVisibility, 0)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	return d, ok
}

func TestZeroDelayIsImmediatelyReady(t *testing.T) {
	q := newTestQueue(t, FIFO)
	if _, err := q.Enqueue(body("a"), 0, 0); err != nil {
		t.Fatal(err)
	}
	if st := q.Stats(); st.Ready != 1 || st.Delayed != 0 {
		t.Fatalf("stats = %+v, want ready 1 delayed 0", st)
	}
}

func TestDelayedMessageIsInvisibleThenPromoted(t *testing.T) {
	q := newTestQueue(t, FIFO)
	if _, err := q.Enqueue(body("later"), 0, tick); err != nil {
		t.Fatal(err)
	}
	if st := q.Stats(); st.Delayed != 1 || st.Ready != 0 {
		t.Fatalf("stats = %+v, want delayed 1 ready 0", st)
	}
	if _, ok := receiveNow(t, q); ok {
		t.Fatal("a delayed message was delivered before its available_at")
	}

	waitFor(t, "promotion", func() bool { return q.Stats().Ready == 1 })
	if _, ok := receiveNow(t, q); !ok {
		t.Fatal("message was not delivered after promotion")
	}
}

// TestDelayedMessagesPromoteInAvailableAtOrder consumes each message as it becomes
// available, which is what isolates the delayed heap's ordering. The messages are enqueued
// in the reverse of the order they come due, so sequence order cannot produce this result.
func TestDelayedMessagesPromoteInAvailableAtOrder(t *testing.T) {
	q := newTestQueue(t, FIFO)
	q.Enqueue(body("third"), 0, 3*tick)  // seq 1
	q.Enqueue(body("second"), 0, 2*tick) // seq 2
	q.Enqueue(body("first"), 0, tick)    // seq 3

	var got []string
	for i := 0; i < 3; i++ {
		d, ok, err := q.Receive(context.Background(), testVisibility, 2*time.Second)
		if err != nil || !ok {
			t.Fatalf("receive %d = (%v, %v)", i, ok, err)
		}
		got = append(got, label(t, d))
	}
	assertOrder(t, got, []string{"first", "second", "third"})
}

// TestPromotedMessagesThenFollowQueueOrdering documents the flip side: once several delayed
// messages have all become available, the ready heap governs, and it sorts by the sequence
// number assigned at enqueue rather than by when each was promoted.
func TestPromotedMessagesThenFollowQueueOrdering(t *testing.T) {
	q := newTestQueue(t, FIFO)
	q.Enqueue(body("enqueued-first"), 0, 3*tick)  // seq 1, available last
	q.Enqueue(body("enqueued-second"), 0, 2*tick) // seq 2
	q.Enqueue(body("enqueued-third"), 0, tick)    // seq 3, available first

	waitFor(t, "all three promotions", func() bool { return q.Stats().Ready == 3 })
	assertOrder(t, drainLabels(t, q), []string{"enqueued-first", "enqueued-second", "enqueued-third"})
}

// TestSchedulerWakesEarlyForNearerDeadline is the observable proof that promotion is timer
// driven rather than polled: the scheduler is armed for a far deadline, a much nearer message
// arrives, and it must fire for the new one instead of sleeping out the original.
func TestSchedulerWakesEarlyForNearerDeadline(t *testing.T) {
	q := newTestQueue(t, FIFO)
	if _, err := q.Enqueue(body("far"), 0, time.Hour); err != nil {
		t.Fatal(err)
	}
	// Let the scheduler arm itself for the distant message before the near one lands.
	time.Sleep(10 * time.Millisecond)

	start := time.Now()
	if _, err := q.Enqueue(body("near"), 0, tick); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the nearer message to be promoted", func() bool { return q.Stats().Ready == 1 })

	if elapsed := time.Since(start); elapsed > grace {
		t.Fatalf("promotion took %v; the scheduler did not re-arm for the nearer deadline", elapsed)
	}
	if st := q.Stats(); st.Delayed != 1 {
		t.Fatalf("stats = %+v, want the distant message still delayed", st)
	}
}

func TestDelayInteractsWithPriority(t *testing.T) {
	q := newTestQueue(t, FIFO)
	q.Enqueue(body("low-now"), 1, 0)
	q.Enqueue(body("high-later"), 10, tick)

	// Before promotion only the ready message exists, regardless of the other's priority.
	if d, ok := receiveNow(t, q); !ok || label(t, d) != "low-now" {
		t.Fatalf("first receive = %q, want low-now", label(t, d))
	}
	waitFor(t, "promotion", func() bool { return q.Stats().Ready == 1 })
	if d, ok := receiveNow(t, q); !ok || label(t, d) != "high-later" {
		t.Fatalf("second receive = %q, want high-later", label(t, d))
	}
}

// TestDelayKeepsOriginalSequenceUnderLIFO pins the documented consequence of assigning a
// sequence number once at enqueue: a delayed message sorts by when it was sent, not by when
// it became available.
func TestDelayKeepsOriginalSequenceUnderLIFO(t *testing.T) {
	q := newTestQueue(t, LIFO)
	q.Enqueue(body("early-delayed"), 0, tick) // seq 1
	q.Enqueue(body("late-instant"), 0, 0)     // seq 2

	waitFor(t, "promotion", func() bool { return q.Stats().Ready == 2 })
	// Under LIFO the higher sequence wins, and the delayed message kept the lower one.
	assertOrder(t, drainLabels(t, q), []string{"late-instant", "early-delayed"})
}

func TestVisibilityTimeoutRedelivers(t *testing.T) {
	q := newTestQueue(t, FIFO)
	send(t, q, "a", 0)

	first, ok, err := q.Receive(context.Background(), tick, 0)
	if err != nil || !ok {
		t.Fatalf("Receive = (%v, %v)", ok, err)
	}
	if first.ReceiveCount != 1 {
		t.Fatalf("first delivery ReceiveCount = %d, want 1", first.ReceiveCount)
	}
	if st := q.Stats(); st.InFlight != 1 {
		t.Fatalf("stats = %+v, want in flight 1", st)
	}

	waitFor(t, "the visibility window to expire", func() bool { return q.Stats().Ready == 1 })

	second, ok := receiveNow(t, q)
	if !ok {
		t.Fatal("message was not redelivered after the visibility timeout")
	}
	if second.ReceiveCount != 2 {
		t.Fatalf("second delivery ReceiveCount = %d, want 2", second.ReceiveCount)
	}
	if second.Receipt == first.Receipt {
		t.Fatal("redelivery reused the original receipt")
	}
	if second.ID != first.ID {
		t.Fatal("redelivery changed the message id")
	}
}

// TestStaleReceiptCannotAck is the case the receipt design exists for: consumer one times
// out, consumer two picks the message up, and consumer one must not be able to ack it.
func TestStaleReceiptCannotAck(t *testing.T) {
	q := newTestQueue(t, FIFO)
	send(t, q, "a", 0)

	stale, _, _ := q.Receive(context.Background(), tick, 0)
	waitFor(t, "the visibility window to expire", func() bool { return q.Stats().Ready == 1 })
	fresh, ok := receiveNow(t, q)
	if !ok {
		t.Fatal("no redelivery")
	}

	if err := q.Ack(stale.Receipt); !errors.Is(err, ErrReceiptNotFound) {
		t.Fatalf("ack with the stale receipt returned %v, want ErrReceiptNotFound", err)
	}
	if err := q.Nack(stale.Receipt, 0); !errors.Is(err, ErrReceiptNotFound) {
		t.Fatalf("nack with the stale receipt returned %v, want ErrReceiptNotFound", err)
	}
	if err := q.Ack(fresh.Receipt); err != nil {
		t.Fatalf("ack with the current receipt failed: %v", err)
	}
	if st := q.Stats(); st.TotalAcked != 1 || st.InFlight != 0 || st.Ready != 0 {
		t.Fatalf("stats = %+v, want one ack and an empty queue", st)
	}
}

func TestAckRemovesPermanently(t *testing.T) {
	q := newTestQueue(t, FIFO)
	send(t, q, "a", 0)
	d, _ := receiveNow(t, q)
	if err := q.Ack(d.Receipt); err != nil {
		t.Fatal(err)
	}
	if _, ok := receiveNow(t, q); ok {
		t.Fatal("an acked message was delivered again")
	}
	if err := q.Ack(d.Receipt); !errors.Is(err, ErrReceiptNotFound) {
		t.Fatalf("second ack returned %v, want ErrReceiptNotFound", err)
	}
}

func TestAckUnknownReceipt(t *testing.T) {
	q := newTestQueue(t, FIFO)
	if err := q.Ack("not-a-real-receipt"); !errors.Is(err, ErrReceiptNotFound) {
		t.Fatalf("Ack returned %v, want ErrReceiptNotFound", err)
	}
}

func TestNackMakesMessageImmediatelyAvailable(t *testing.T) {
	q := newTestQueue(t, FIFO)
	send(t, q, "a", 0)
	d, _ := receiveNow(t, q)

	if err := q.Nack(d.Receipt, 0); err != nil {
		t.Fatal(err)
	}
	st := q.Stats()
	if st.Ready != 1 || st.InFlight != 0 || st.Delayed != 0 {
		t.Fatalf("stats = %+v, want ready 1", st)
	}
	again, ok := receiveNow(t, q)
	if !ok {
		t.Fatal("nacked message was not redelivered")
	}
	if again.ReceiveCount != 2 {
		t.Fatalf("ReceiveCount = %d after nack, want 2", again.ReceiveCount)
	}
}

func TestNackWithDelayMovesToDelayed(t *testing.T) {
	q := newTestQueue(t, FIFO)
	send(t, q, "a", 0)
	d, _ := receiveNow(t, q)

	if err := q.Nack(d.Receipt, tick); err != nil {
		t.Fatal(err)
	}
	st := q.Stats()
	if st.Delayed != 1 || st.Ready != 0 || st.InFlight != 0 {
		t.Fatalf("stats = %+v, want delayed 1", st)
	}
	if _, ok := receiveNow(t, q); ok {
		t.Fatal("a nacked-with-delay message was delivered before its retry delay elapsed")
	}
	waitFor(t, "the retry delay to elapse", func() bool { return q.Stats().Ready == 1 })
}

func TestNackRejectsNegativeDelay(t *testing.T) {
	q := newTestQueue(t, FIFO)
	send(t, q, "a", 0)
	d, _ := receiveNow(t, q)
	if err := q.Nack(d.Receipt, -time.Second); err == nil {
		t.Fatal("Nack with a negative delay should fail")
	}
}

func TestLongPollReturnsEmptyAfterWait(t *testing.T) {
	q := newTestQueue(t, FIFO)
	start := time.Now()
	_, ok, err := q.Receive(context.Background(), testVisibility, tick)
	elapsed := time.Since(start)
	if err != nil || ok {
		t.Fatalf("Receive = (%v, %v), want no message", ok, err)
	}
	if elapsed < tick {
		t.Fatalf("long poll returned after %v, want at least %v", elapsed, tick)
	}
	if elapsed > tick+grace {
		t.Fatalf("long poll returned after %v, far past its wait window", elapsed)
	}
}

// TestLongPollWakesOnEnqueue checks the cond signalling path: a consumer parked in Wait must
// be woken by a producer rather than sitting out its whole window.
func TestLongPollWakesOnEnqueue(t *testing.T) {
	q := newTestQueue(t, FIFO)

	type result struct {
		d   Delivery
		ok  bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		d, ok, err := q.Receive(context.Background(), testVisibility, 5*time.Second)
		done <- result{d, ok, err}
	}()

	time.Sleep(20 * time.Millisecond) // let the consumer reach Wait
	start := time.Now()
	send(t, q, "wakeup", 0)

	select {
	case r := <-done:
		if r.err != nil || !r.ok {
			t.Fatalf("long poll returned (%v, %v), want a message", r.ok, r.err)
		}
		if elapsed := time.Since(start); elapsed > grace {
			t.Fatalf("consumer woke %v after the enqueue, far too slow for a broadcast", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long poll did not wake on enqueue")
	}
}

func TestLongPollWakesOnDelayedPromotion(t *testing.T) {
	q := newTestQueue(t, FIFO)
	if _, err := q.Enqueue(body("later"), 0, tick); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	d, ok, err := q.Receive(context.Background(), testVisibility, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("Receive = (%v, %v), want the promoted message", ok, err)
	}
	if label(t, d) != "later" {
		t.Fatalf("received %q, want later", label(t, d))
	}
	if elapsed := time.Since(start); elapsed > tick+grace {
		t.Fatalf("consumer woke %v after the delay; the scheduler did not signal it", elapsed)
	}
}

func TestLongPollHonoursContextCancellation(t *testing.T) {
	q := newTestQueue(t, FIFO)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, _, err := q.Receive(ctx, testVisibility, 5*time.Second)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Receive returned %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > grace {
			t.Fatalf("consumer took %v to notice cancellation", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelling the context did not release the long poll")
	}
}

// TestOnlyOneLongPollerGetsEachMessage guards the choice of Broadcast over Signal: every
// parked consumer wakes, but exactly one may claim the message.
func TestOnlyOneLongPollerGetsEachMessage(t *testing.T) {
	q := newTestQueue(t, FIFO)
	const consumers = 8

	var wg sync.WaitGroup
	var mu sync.Mutex
	received := 0
	empty := 0

	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := q.Receive(context.Background(), testVisibility, 300*time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Errorf("Receive: %v", err)
				return
			}
			if ok {
				received++
			} else {
				empty++
			}
		}()
	}

	time.Sleep(30 * time.Millisecond)
	send(t, q, "only-one", 0)
	wg.Wait()

	if received != 1 || empty != consumers-1 {
		t.Fatalf("%d consumers got the message and %d timed out, want exactly 1 and %d", received, empty, consumers-1)
	}
}

func TestReceiveOnClosedQueue(t *testing.T) {
	q := newQueue(Config{Name: "q", Ordering: FIFO, CreatedAt: time.Now()}, &fakeLog{}, 1, 0, 0)
	q.start()
	q.Close()

	if _, _, err := q.Receive(context.Background(), testVisibility, 0); !errors.Is(err, ErrQueueDeleted) {
		t.Fatalf("Receive returned %v, want ErrQueueDeleted", err)
	}
	if _, err := q.Enqueue(body("x"), 0, 0); !errors.Is(err, ErrQueueDeleted) {
		t.Fatalf("Enqueue returned %v, want ErrQueueDeleted", err)
	}
	if err := q.Ack("anything"); !errors.Is(err, ErrQueueDeleted) {
		t.Fatalf("Ack returned %v, want ErrQueueDeleted", err)
	}
}

// TestCloseReleasesLongPollers makes sure deleting a queue does not strand its consumers.
func TestCloseReleasesLongPollers(t *testing.T) {
	q := newQueue(Config{Name: "q", Ordering: FIFO, CreatedAt: time.Now()}, &fakeLog{}, 1, 0, 0)
	q.start()

	done := make(chan error, 1)
	go func() {
		_, _, err := q.Receive(context.Background(), testVisibility, 5*time.Second)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	q.Close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrQueueDeleted) {
			t.Fatalf("Receive returned %v, want ErrQueueDeleted", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closing the queue left a long poller blocked")
	}
}

func TestEnqueueFailureLeavesQueueUnchanged(t *testing.T) {
	log := &fakeLog{}
	q := newQueue(Config{Name: "q", Ordering: FIFO, CreatedAt: time.Now()}, log, 1, 0, 0)
	q.start()
	defer q.Close()

	failure := errors.New("log unavailable")
	log.fail(failure)

	if _, err := q.Enqueue(body("doomed"), 0, 0); !errors.Is(err, failure) {
		t.Fatalf("Enqueue returned %v, want the log failure", err)
	}
	st := q.Stats()
	if st.Ready != 0 || st.TotalEnqueued != 0 {
		t.Fatalf("stats = %+v, want nothing recorded after a failed append", st)
	}

	// A failed append must not consume a sequence number, so the log stays contiguous.
	log.fail(nil)
	if _, err := q.Enqueue(body("ok"), 0, 0); err != nil {
		t.Fatal(err)
	}
	recs := log.snapshot()
	if len(recs) != 1 {
		t.Fatalf("log holds %d records, want 1", len(recs))
	}
}

func TestAckFailureKeepsMessageInFlight(t *testing.T) {
	log := &fakeLog{}
	q := newQueue(Config{Name: "q", Ordering: FIFO, CreatedAt: time.Now()}, log, 1, 0, 0)
	q.start()
	defer q.Close()

	q.Enqueue(body("a"), 0, 0)
	d, _ := receiveNow(t, q)

	failure := errors.New("log unavailable")
	log.fail(failure)
	if err := q.Ack(d.Receipt); !errors.Is(err, failure) {
		t.Fatalf("Ack returned %v, want the log failure", err)
	}
	st := q.Stats()
	if st.InFlight != 1 || st.TotalAcked != 0 {
		t.Fatalf("stats = %+v, want the message still in flight and nothing acked", st)
	}

	// The receipt is still valid, so the consumer can retry once the log recovers.
	log.fail(nil)
	if err := q.Ack(d.Receipt); err != nil {
		t.Fatalf("retried ack failed: %v", err)
	}
}
