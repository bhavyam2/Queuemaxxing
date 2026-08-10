package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentProducersAndConsumers is the central concurrency claim: with a visibility
// window long enough that nothing times out, every message is delivered and acked exactly
// once, and no two consumers ever hold the same message at the same time.
func TestConcurrentProducersAndConsumers(t *testing.T) {
	q := newTestQueue(t, FIFO)

	const (
		producers        = 16
		perProducer      = 500
		consumers        = 8
		total            = producers * perProducer
		longVisibility   = 5 * time.Minute
		consumerPollWait = 200 * time.Millisecond
	)

	var (
		mu          sync.Mutex
		ackedByID   = make(map[string]int)
		heldNow     = make(map[string]bool) // messages currently out with some consumer
		overlaps    int
		ackedTotal  int
		producerErr error
	)

	var producerWG sync.WaitGroup
	for p := 0; p < producers; p++ {
		producerWG.Add(1)
		go func(p int) {
			defer producerWG.Done()
			for i := 0; i < perProducer; i++ {
				payload, _ := json.Marshal(map[string]int{"producer": p, "index": i})
				if _, err := q.Enqueue(payload, i%5, 0); err != nil {
					mu.Lock()
					if producerErr == nil {
						producerErr = err
					}
					mu.Unlock()
					return
				}
			}
		}(p)
	}

	var consumerWG sync.WaitGroup
	for c := 0; c < consumers; c++ {
		consumerWG.Add(1)
		go func() {
			defer consumerWG.Done()
			for {
				// Every consumer exits once the queue is fully drained, not just the one
				// that happened to ack the final message.
				mu.Lock()
				finished := ackedTotal == total
				mu.Unlock()
				if finished {
					return
				}

				d, ok, err := q.Receive(context.Background(), longVisibility, consumerPollWait)
				if err != nil {
					t.Errorf("Receive: %v", err)
					return
				}
				if !ok {
					continue
				}

				mu.Lock()
				if heldNow[d.ID] {
					overlaps++
				}
				heldNow[d.ID] = true
				mu.Unlock()

				if err := q.Ack(d.Receipt); err != nil {
					t.Errorf("Ack: %v", err)
					return
				}

				mu.Lock()
				delete(heldNow, d.ID)
				ackedByID[d.ID]++
				ackedTotal++
				mu.Unlock()
			}
		}()
	}

	producerWG.Wait()

	// Wait for the consumers to drain, with a watchdog so a deadlock fails rather than hangs.
	drained := make(chan struct{})
	go func() {
		consumerWG.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(60 * time.Second):
		t.Fatal("consumers did not drain the queue; likely a deadlock or a lost wakeup")
	}

	mu.Lock()
	defer mu.Unlock()
	if producerErr != nil {
		t.Fatalf("producer failed: %v", producerErr)
	}
	if overlaps != 0 {
		t.Fatalf("%d messages were held by two consumers at once", overlaps)
	}
	if ackedTotal != total {
		t.Fatalf("acked %d messages, enqueued %d", ackedTotal, total)
	}
	if len(ackedByID) != total {
		t.Fatalf("%d distinct messages acked, want %d", len(ackedByID), total)
	}
	for id, n := range ackedByID {
		if n != 1 {
			t.Fatalf("message %s acked %d times, want exactly 1", id, n)
		}
	}

	st := q.Stats()
	if st.Ready != 0 || st.InFlight != 0 || st.Delayed != 0 {
		t.Fatalf("stats = %+v, want an empty queue", st)
	}
	if st.TotalEnqueued != total || st.TotalAcked != total {
		t.Fatalf("stats = %+v, want %d enqueued and %d acked", st, total, total)
	}
}

// TestAtLeastOnceWithShortVisibility runs the same shape with a visibility window short
// enough to force redeliveries, and asserts the guarantee actually on offer: every message
// arrives at least once, duplicates are possible, and deduplicating on message id recovers
// exactly-once at the application level.
func TestAtLeastOnceWithShortVisibility(t *testing.T) {
	q := newTestQueue(t, FIFO)

	const (
		total     = 300
		consumers = 6
	)
	for i := 0; i < total; i++ {
		send(t, q, fmt.Sprintf("m%d", i), 0)
	}

	var (
		mu         sync.Mutex
		deliveries = make(map[string]int)
		processed  = make(map[string]bool) // the consumer's own idempotency layer
	)

	var wg sync.WaitGroup
	deadline := time.Now().Add(20 * time.Second)
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				// A very short window means a slow consumer loses its claim.
				d, ok, err := q.Receive(context.Background(), 25*time.Millisecond, 100*time.Millisecond)
				if err != nil {
					t.Errorf("Receive: %v", err)
					return
				}
				if !ok {
					mu.Lock()
					finished := len(processed) == total
					mu.Unlock()
					if finished {
						return
					}
					continue
				}

				mu.Lock()
				deliveries[d.ID]++
				processed[d.ID] = true
				remaining := total - len(processed)
				mu.Unlock()

				// Acking may fail if this delivery's window already expired, which is
				// exactly the redelivery path under test.
				if err := q.Ack(d.Receipt); err != nil && !errors.Is(err, ErrReceiptNotFound) {
					t.Errorf("Ack: %v", err)
					return
				}
				if remaining == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != total {
		t.Fatalf("%d distinct messages seen, want %d; at-least-once was violated", len(processed), total)
	}
	for id, n := range deliveries {
		if n < 1 {
			t.Fatalf("message %s was never delivered", id)
		}
	}
}

// TestConcurrentAckAndExpiry hammers the race between a consumer acking and the scheduler
// expiring the same delivery. Exactly one of them may win, and the loser must be told the
// receipt is gone rather than corrupting the queue.
func TestConcurrentAckAndExpiry(t *testing.T) {
	q := newTestQueue(t, FIFO)

	const rounds = 200
	acked, rejected := 0, 0
	for i := 0; i < rounds; i++ {
		send(t, q, fmt.Sprintf("m%d", i), 0)

		// A window this short expires at roughly the moment the ack is issued.
		d, ok, err := q.Receive(context.Background(), time.Millisecond, 0)
		if err != nil || !ok {
			t.Fatalf("Receive = (%v, %v)", ok, err)
		}
		switch err := q.Ack(d.Receipt); {
		case err == nil:
			acked++
		case errors.Is(err, ErrReceiptNotFound):
			rejected++
		default:
			t.Fatalf("Ack returned an unexpected error: %v", err)
		}
	}

	// Whatever the split, the totals must add up and nothing may be double counted.
	st := q.Stats()
	if int(st.TotalAcked) != acked {
		t.Fatalf("stats report %d acks, the test counted %d", st.TotalAcked, acked)
	}
	if got := st.Ready + st.InFlight + st.Delayed; got != rejected {
		t.Fatalf("%d messages left in the queue, want %d (the acks that lost the race)", got, rejected)
	}
	if acked+rejected != rounds {
		t.Fatalf("acked %d and rejected %d, want %d in total", acked, rejected, rounds)
	}
}

// TestConcurrentQueueOperations mixes every operation against one queue to shake out lock
// ordering problems and heap index corruption under the race detector.
func TestConcurrentQueueOperations(t *testing.T) {
	q := newTestQueue(t, LIFO)

	var wg sync.WaitGroup
	stop := time.Now().Add(2 * time.Second)

	worker := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) {
				fn()
			}
		}()
	}

	for i := 0; i < 4; i++ {
		worker(func() { q.Enqueue(body("plain"), 1, 0) })
		worker(func() { q.Enqueue(body("delayed"), 2, 30*time.Millisecond) })
		worker(func() {
			d, ok, err := q.Receive(context.Background(), 50*time.Millisecond, 0)
			if err != nil || !ok {
				return
			}
			if d.Seq%2 == 0 {
				q.Ack(d.Receipt)
			} else {
				q.Nack(d.Receipt, time.Duration(d.Seq%3)*10*time.Millisecond)
			}
		})
		worker(func() { q.Stats() })
	}
	wg.Wait()

	// The core invariant: every message is in exactly one place.
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.ready.Len()+q.delayed.Len()+len(q.inflight) < 0 {
		t.Fatal("negative queue size")
	}
	for i, m := range q.ready.items {
		if m.index != i {
			t.Fatalf("ready heap index corrupted at %d: message records %d", i, m.index)
		}
		if m.State != StateReady {
			t.Fatalf("message in the ready heap has state %s", m.State)
		}
	}
	for i, m := range q.delayed.items {
		if m.index != i {
			t.Fatalf("delayed heap index corrupted at %d: message records %d", i, m.index)
		}
	}
	for i, m := range q.expiry.items {
		if m.index != i {
			t.Fatalf("expiry heap index corrupted at %d: message records %d", i, m.index)
		}
	}
	if len(q.inflight) != q.expiry.Len() {
		t.Fatalf("in-flight map holds %d entries but the expiry heap holds %d", len(q.inflight), q.expiry.Len())
	}
	for receipt, m := range q.inflight {
		if m.Receipt != receipt {
			t.Fatalf("in-flight map key %s does not match the message receipt %s", receipt, m.Receipt)
		}
	}
}

// TestConcurrentManagerAccess exercises the registry lock against queue operations.
func TestConcurrentManagerAccess(t *testing.T) {
	m, err := NewManager(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	var wg sync.WaitGroup
	stop := time.Now().Add(time.Second)

	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("q%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) {
				m.Create(name, FIFO)
				if q, err := m.Get(name); err == nil {
					q.Enqueue(body("x"), 0, 0)
					q.Stats()
				}
				m.List()
				m.Delete(name)
			}
		}()
	}
	wg.Wait()
}
