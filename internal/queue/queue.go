package queue

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"

	"queuemaxxing/internal/storage"
)

var (
	ErrEmptyBody       = errors.New("message body is required")
	ErrQueueDeleted    = errors.New("queue was deleted")
	ErrReceiptNotFound = errors.New("receipt is unknown or no longer valid")
)

// namePattern keeps queue names usable as file names. Without it a name like "../../etc/x"
// would escape the data directory.
var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

func ValidateName(name string) error {
	if !namePattern.MatchString(name) {
		return fmt.Errorf("queue name %q must be 1 to 64 characters of letters, digits, underscore or hyphen, starting with a letter or digit", name)
	}
	return nil
}

// logStore is the queue's view of its log. Declared here rather than in the storage package
// so tests can substitute a log that fails on demand.
type logStore interface {
	Append(rec storage.Record) error
	Records() int
	Compact(hdr storage.Header, live []storage.Enqueue) error
	Close() error
}

type Stats struct {
	Ready         int    `json:"ready"`
	Delayed       int    `json:"delayed"`
	InFlight      int    `json:"in_flight"`
	TotalEnqueued uint64 `json:"total_enqueued"`
	TotalAcked    uint64 `json:"total_acked"`
}

type Config struct {
	Name      string    `json:"name"`
	Ordering  Ordering  `json:"ordering"`
	CreatedAt time.Time `json:"created_at"`
}

// Queue owns everything about one named queue. Every mutable field below is guarded by mu,
// including the log handle: appends happen with the lock held, which is what makes the log's
// byte order match operation order and bounds write throughput at one fsync per operation.
type Queue struct {
	mu   sync.Mutex
	cond *sync.Cond

	name      string
	ordering  Ordering
	createdAt time.Time

	ready    *msgHeap
	delayed  *msgHeap
	expiry   *msgHeap
	inflight map[string]*Message

	nextSeq  uint64
	enqueued uint64
	acked    uint64

	log              logStore
	closed           bool
	compactThreshold int

	// kick wakes the scheduler when a new delayed message or in-flight deadline lands
	// earlier than whatever it is currently armed for. Capacity 1 so concurrent kicks
	// coalesce and a sender holding mu never blocks.
	kick chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
}

func newQueue(cfg Config, store logStore, nextSeq, enqueued, acked uint64) *Queue {
	q := &Queue{
		name:      cfg.Name,
		ordering:  cfg.Ordering,
		createdAt: cfg.CreatedAt,
		ready:     newReadyHeap(cfg.Ordering),
		delayed:   newDelayedHeap(),
		expiry:    newExpiryHeap(),
		inflight:  make(map[string]*Message),
		nextSeq:   nextSeq,
		enqueued:  enqueued,
		acked:     acked,
		log:       store,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func createQueue(dir string, cfg Config, compactThreshold int) (*Queue, error) {
	if err := ValidateName(cfg.Name); err != nil {
		return nil, err
	}
	hdr := storage.Header{
		Name:      cfg.Name,
		Ordering:  string(cfg.Ordering),
		CreatedAt: toMillis(cfg.CreatedAt),
		NextSeq:   1,
	}
	store, err := storage.Create(dir, cfg.Name, hdr)
	if err != nil {
		return nil, err
	}
	q := newQueue(cfg, store, 1, 0, 0)
	q.compactThreshold = compactThreshold
	q.start()
	return q, nil
}

// openQueue replays a log and rebuilds the queue. In-flight state was deliberately never
// written, so every unacked message comes back ready, or delayed if a persisted nack pushed
// its available_at into the future. That single rule is the whole at-least-once story.
func openQueue(dir, name string, compactThreshold int) (*Queue, error) {
	var (
		cfg      Config
		live            = make(map[string]*Message)
		nextSeq  uint64 = 1
		enqueued uint64
		acked    uint64
		seenHdr  bool
	)

	store, err := storage.Open(dir, name, func(rec storage.Record) error {
		switch r := rec.(type) {
		case storage.Header:
			ordering, err := ParseOrdering(r.Ordering)
			if err != nil {
				return err
			}
			cfg = Config{Name: r.Name, Ordering: ordering, CreatedAt: fromMillis(r.CreatedAt)}
			// A compacted log starts from the counters recorded at compaction time, since
			// the enqueue and ack frames they were derived from are gone.
			if r.NextSeq > nextSeq {
				nextSeq = r.NextSeq
			}
			enqueued = r.TotalEnqueued
			acked = r.TotalAcked
			seenHdr = true

		case storage.Enqueue:
			body := make(json.RawMessage, len(r.Body))
			copy(body, r.Body)
			live[r.ID] = &Message{
				ID:          r.ID,
				Seq:         r.Seq,
				Priority:    r.Priority,
				Body:        body,
				CreatedAt:   fromMillis(r.CreatedAt),
				AvailableAt: fromMillis(r.AvailableAt),
			}
			if r.Seq >= nextSeq {
				nextSeq = r.Seq + 1
			}
			enqueued++

		case storage.Ack:
			delete(live, r.ID)
			acked++

		case storage.Nack:
			// An ack for the same id may already have removed it after a compaction
			// boundary; replaying a nack for an absent message is a no-op by design.
			if m, ok := live[r.ID]; ok {
				m.AvailableAt = fromMillis(r.AvailableAt)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !seenHdr {
		store.Close()
		return nil, fmt.Errorf("%s: log has no header record", name)
	}
	if cfg.Name != name {
		store.Close()
		return nil, fmt.Errorf("%s: log header names queue %q", name, cfg.Name)
	}

	q := newQueue(cfg, store, nextSeq, enqueued, acked)
	q.compactThreshold = compactThreshold
	now := time.Now()
	for _, m := range live {
		if m.AvailableAt.After(now) {
			m.State = StateDelayed
			heap.Push(q.delayed, m)
		} else {
			m.State = StateReady
			heap.Push(q.ready, m)
		}
	}
	q.start()
	return q, nil
}

func (q *Queue) Name() string { return q.name }

func (q *Queue) Config() Config {
	return Config{Name: q.name, Ordering: q.ordering, CreatedAt: q.createdAt}
}

// Enqueue durably records the message before making it visible. The append fsyncs, so a
// caller that sees no error can rely on the message surviving a crash.
func (q *Queue) Enqueue(body json.RawMessage, priority int, delay time.Duration) (string, error) {
	if len(body) == 0 {
		return "", ErrEmptyBody
	}
	if delay < 0 {
		return "", errors.New("delay must not be negative")
	}
	id, err := newID()
	if err != nil {
		return "", err
	}

	// The caller's bytes may alias a request buffer that is reused after the handler
	// returns, so the queue keeps its own copy.
	owned := make(json.RawMessage, len(body))
	copy(owned, body)

	now := time.Now()
	available := now.Add(delay)

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return "", ErrQueueDeleted
	}

	seq := q.nextSeq
	rec := storage.Enqueue{
		ID:          id,
		Seq:         seq,
		Priority:    priority,
		CreatedAt:   toMillis(now),
		AvailableAt: toMillis(available),
		Body:        owned,
	}
	if err := q.log.Append(rec); err != nil {
		return "", err
	}
	q.nextSeq++
	q.enqueued++
	defer q.maybeCompact()

	m := &Message{
		ID:          id,
		Seq:         seq,
		Priority:    priority,
		Body:        owned,
		CreatedAt:   now,
		AvailableAt: available,
	}
	if delay > 0 {
		m.State = StateDelayed
		heap.Push(q.delayed, m)
		if q.delayed.peek() == m {
			q.wakeScheduler()
		}
	} else {
		m.State = StateReady
		heap.Push(q.ready, m)
	}
	q.cond.Broadcast()
	return id, nil
}

// popReady moves the next eligible message into the in-flight set. The caller holds mu, so
// selecting, marking, and recording the delivery are one atomic step and two consumers can
// never be handed the same message.
func (q *Queue) popReady(visibility time.Duration) (Delivery, bool, error) {
	if q.ready.Len() == 0 {
		return Delivery{}, false, nil
	}
	receipt, err := newReceipt()
	if err != nil {
		return Delivery{}, false, err
	}
	m := heap.Pop(q.ready).(*Message)
	m.State = StateInFlight
	m.ReceiveCount++
	m.Receipt = receipt
	m.Deadline = time.Now().Add(visibility)
	q.inflight[receipt] = m
	heap.Push(q.expiry, m)
	// Only disturb the scheduler when this deadline is the new earliest one; otherwise it is
	// already armed for something sooner.
	if q.expiry.peek() == m {
		q.wakeScheduler()
	}
	return m.delivery(), true, nil
}

// Receive hands out the next eligible message. With wait above zero it blocks on the queue's
// condition variable until a message arrives, the wait elapses, or ctx is cancelled.
//
// sync.Cond has no timed wait and cannot observe a cancelled context, so both are turned into
// broadcasts: a timer fires at the deadline and a watcher goroutine fires on cancellation.
// Both take the queue mutex before broadcasting, because a broadcast issued without it could
// land between a waiter's predicate check and its Wait and strand that waiter.
func (q *Queue) Receive(ctx context.Context, visibility, wait time.Duration) (Delivery, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var deadline time.Time
	if wait > 0 {
		deadline = time.Now().Add(wait)

		timer := time.AfterFunc(wait, func() {
			q.mu.Lock()
			defer q.mu.Unlock()
			q.cond.Broadcast()
		})
		// Stop runs while mu is held, but it never waits for an already-running callback,
		// so a callback blocked on mu cannot deadlock against it.
		defer timer.Stop()

		if done := ctx.Done(); done != nil {
			finished := make(chan struct{})
			defer close(finished)
			go func() {
				select {
				case <-done:
					q.mu.Lock()
					defer q.mu.Unlock()
					q.cond.Broadcast()
				case <-finished:
				}
			}()
		}
	}

	for {
		if q.closed {
			return Delivery{}, false, ErrQueueDeleted
		}
		d, ok, err := q.popReady(visibility)
		if err != nil || ok {
			return d, ok, err
		}
		if wait <= 0 || !time.Now().Before(deadline) {
			return Delivery{}, false, nil
		}
		if err := ctx.Err(); err != nil {
			return Delivery{}, false, err
		}
		q.cond.Wait()
	}
}

// Ack removes a message permanently. The receipt, not the message id, is the credential: a
// consumer whose delivery already expired holds a receipt that is no longer in the map and
// therefore cannot acknowledge the redelivery someone else is now working on.
func (q *Queue) Ack(receipt string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueDeleted
	}
	m, ok := q.inflight[receipt]
	if !ok {
		return ErrReceiptNotFound
	}
	if err := q.log.Append(storage.Ack{ID: m.ID}); err != nil {
		return err
	}
	delete(q.inflight, receipt)
	heap.Remove(q.expiry, m.index)
	q.acked++
	q.maybeCompact()
	return nil
}

// Nack returns a message to the queue, immediately or after a retry delay.
func (q *Queue) Nack(receipt string, delay time.Duration) error {
	if delay < 0 {
		return errors.New("delay must not be negative")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueDeleted
	}
	m, ok := q.inflight[receipt]
	if !ok {
		return ErrReceiptNotFound
	}

	available := time.Now().Add(delay)
	if err := q.log.Append(storage.Nack{ID: m.ID, AvailableAt: toMillis(available)}); err != nil {
		return err
	}
	delete(q.inflight, receipt)
	heap.Remove(q.expiry, m.index)
	m.Receipt = ""
	m.Deadline = time.Time{}
	m.AvailableAt = available

	if delay > 0 {
		m.State = StateDelayed
		heap.Push(q.delayed, m)
		if q.delayed.peek() == m {
			q.wakeScheduler()
		}
	} else {
		m.State = StateReady
		heap.Push(q.ready, m)
	}
	q.cond.Broadcast()
	q.maybeCompact()
	return nil
}

// wakeScheduler is a non-blocking send, so a caller holding mu can never stall behind a busy
// scheduler. Before the scheduler is started the channel is nil and this is a no-op.
func (q *Queue) wakeScheduler() {
	select {
	case q.kick <- struct{}{}:
	default:
	}
}

func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()

	if q.done != nil {
		close(q.done)
	}
	q.wg.Wait()

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.compactThreshold > 0 {
		if err := q.compactLocked(); err != nil {
			log.Printf("queuemaxxing: compact %s on shutdown: %v", q.name, err)
		}
	}
	return q.log.Close()
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Stats{
		Ready:         q.ready.Len(),
		Delayed:       q.delayed.Len(),
		InFlight:      len(q.inflight),
		TotalEnqueued: q.enqueued,
		TotalAcked:    q.acked,
	}
}

func toMillis(t time.Time) int64    { return t.UnixMilli() }
func fromMillis(ms int64) time.Time { return time.UnixMilli(ms) }
