package queue

// msgHeap is the shared container behind all three per-queue heaps. Only the comparator
// differs between them, and Message.index is maintained here so heap.Remove can pull a
// message out of the middle in O(log n) when it is acked.
type msgHeap struct {
	items []*Message
	less  func(a, b *Message) bool
}

func (h *msgHeap) Len() int           { return len(h.items) }
func (h *msgHeap) Less(i, j int) bool { return h.less(h.items[i], h.items[j]) }

func (h *msgHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}

func (h *msgHeap) Push(x any) {
	m := x.(*Message)
	m.index = len(h.items)
	h.items = append(h.items, m)
}

func (h *msgHeap) Pop() any {
	old := h.items
	n := len(old)
	m := old[n-1]
	old[n-1] = nil
	m.index = -1
	h.items = old[:n-1]
	return m
}

func (h *msgHeap) peek() *Message {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

// newReadyHeap orders by priority first, then breaks ties by sequence number in the
// direction the queue was configured with. Sequence numbers rather than timestamps because
// messages enqueued in the same nanosecond still need a total order, and that order has to
// survive a restart.
func newReadyHeap(ordering Ordering) *msgHeap {
	return &msgHeap{less: func(a, b *Message) bool {
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if ordering == FIFO {
			return a.Seq < b.Seq
		}
		return a.Seq > b.Seq
	}}
}
