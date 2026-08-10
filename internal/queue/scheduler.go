package queue

import (
	"container/heap"
	"time"
)

// start launches the queue's scheduler. One goroutine covers both delay promotion and
// visibility expiry, because they are the same problem: act at time T, and wake earlier if
// an earlier T appears.
func (q *Queue) start() {
	q.kick = make(chan struct{}, 1)
	q.done = make(chan struct{})
	q.wg.Add(1)
	go q.runScheduler()
}

func (q *Queue) runScheduler() {
	defer q.wg.Done()

	// The timer belongs to this goroutine alone. Producers and consumers signal through
	// kick rather than calling Reset themselves, which is what keeps this free of the
	// reset-an-undrained-timer race.
	timer := time.NewTimer(time.Hour)
	stopTimer(timer)
	armed := false

	for {
		q.mu.Lock()
		next, ok := q.processDue(time.Now())
		q.mu.Unlock()

		if ok {
			timer.Reset(time.Until(next))
			armed = true
		}

		select {
		case <-timer.C:
			armed = false
		case <-q.kick:
			if armed {
				stopTimer(timer)
				armed = false
			}
		case <-q.done:
			if armed {
				stopTimer(timer)
			}
			return
		}
	}
}

// stopTimer drains the channel only if a value is actually waiting. The bare
// `if !t.Stop() { <-t.C }` idiom blocks under the timer semantics introduced in Go 1.23,
// which apply once this module's go directive moves past 1.22.
func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

// processDue promotes everything that has come due and reports the next moment the queue
// needs attention. The caller holds mu, and the broadcast happens under it so a waiting
// consumer cannot miss the wakeup between checking its predicate and calling Wait.
func (q *Queue) processDue(now time.Time) (time.Time, bool) {
	changed := false

	for q.delayed.Len() > 0 {
		m := q.delayed.peek()
		if m.AvailableAt.After(now) {
			break
		}
		heap.Pop(q.delayed)
		m.State = StateReady
		heap.Push(q.ready, m)
		changed = true
	}

	for q.expiry.Len() > 0 {
		m := q.expiry.peek()
		if m.Deadline.After(now) {
			break
		}
		heap.Pop(q.expiry)
		// Dropping the receipt is what makes a late ack from the original consumer fail:
		// the lookup misses, so it cannot acknowledge a delivery that has moved on.
		delete(q.inflight, m.Receipt)
		m.Receipt = ""
		m.Deadline = time.Time{}
		m.State = StateReady
		heap.Push(q.ready, m)
		changed = true
	}

	if changed {
		q.cond.Broadcast()
	}

	var next time.Time
	ok := false
	if d := q.delayed.peek(); d != nil {
		next, ok = d.AvailableAt, true
	}
	if e := q.expiry.peek(); e != nil && (!ok || e.Deadline.Before(next)) {
		next, ok = e.Deadline, true
	}
	return next, ok
}
