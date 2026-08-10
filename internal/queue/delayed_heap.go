package queue

// newDelayedHeap is a min-heap on AvailableAt. Delayed messages never sit in the ready heap,
// so the scheduler only ever has to look at one element to know when it next has work.
func newDelayedHeap() *msgHeap {
	return &msgHeap{less: func(a, b *Message) bool {
		return a.AvailableAt.Before(b.AvailableAt)
	}}
}
