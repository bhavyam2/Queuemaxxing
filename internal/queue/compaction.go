package queue

import (
	"log"

	"queuemaxxing/internal/storage"
)

// liveCount is the number of messages compaction would have to write out. The caller holds mu.
func (q *Queue) liveCount() int {
	return q.ready.Len() + q.delayed.Len() + len(q.inflight)
}

// maybeCompact rewrites the log once it has grown past the threshold, provided the rewrite
// would at least halve it. Without that second condition a queue holding more live messages
// than the threshold would recompact on every append, since the fresh log would already be
// over the limit. Requiring the log to double before the next attempt keeps compaction
// amortised. The caller holds mu.
func (q *Queue) maybeCompact() {
	if q.compactThreshold <= 0 {
		return
	}
	records := q.log.Records()
	if records < q.compactThreshold || records < 2*(q.liveCount()+1) {
		return
	}
	if err := q.compactLocked(); err != nil {
		// The old log is still intact and correct, so a failed compaction is not fatal;
		// it only means the file stays larger than intended.
		log.Printf("queuemaxxing: compact %s: %v", q.name, err)
	}
}

// compactLocked writes a snapshot of every live message. In-flight messages are live and
// unacked, so they are written with their original available_at and come back ready after a
// restart, which is the same recovery policy that applies to any other unacked message.
// The caller holds mu.
func (q *Queue) compactLocked() error {
	live := make([]storage.Enqueue, 0, q.liveCount())
	appendAll := func(items []*Message) {
		for _, m := range items {
			live = append(live, storage.Enqueue{
				ID:          m.ID,
				Seq:         m.Seq,
				Priority:    m.Priority,
				CreatedAt:   toMillis(m.CreatedAt),
				AvailableAt: toMillis(m.AvailableAt),
				Body:        m.Body,
			})
		}
	}
	appendAll(q.ready.items)
	appendAll(q.delayed.items)
	for _, m := range q.inflight {
		live = append(live, storage.Enqueue{
			ID:          m.ID,
			Seq:         m.Seq,
			Priority:    m.Priority,
			CreatedAt:   toMillis(m.CreatedAt),
			AvailableAt: toMillis(m.AvailableAt),
			Body:        m.Body,
		})
	}

	// The header accounts only for history the following records no longer describe.
	// Recovery derives total_enqueued as the header value plus the ENQUEUE records it
	// replays, and a compacted log replays one per live message, so those must be excluded
	// here or every compaction would inflate the counter by the size of the live set.
	// Acks need no such adjustment: a compacted log contains no ACK records at all.
	hdr := storage.Header{
		Name:          q.name,
		Ordering:      string(q.ordering),
		CreatedAt:     toMillis(q.createdAt),
		NextSeq:       q.nextSeq,
		TotalEnqueued: q.enqueued - uint64(len(live)),
		TotalAcked:    q.acked,
	}
	return q.log.Compact(hdr, live)
}
