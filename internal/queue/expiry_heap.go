package queue

// newExpiryHeap is a min-heap on the visibility deadline of in-flight messages. Without it,
// finding which deliveries have timed out would mean scanning the whole in-flight map on
// every scheduler wakeup.
func newExpiryHeap() *msgHeap {
	return &msgHeap{less: func(a, b *Message) bool {
		return a.Deadline.Before(b.Deadline)
	}}
}
