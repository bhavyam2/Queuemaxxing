package queue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Ordering is the tie-breaker applied to messages of equal priority.
type Ordering string

const (
	FIFO Ordering = "fifo"
	LIFO Ordering = "lifo"
)

func ParseOrdering(s string) (Ordering, error) {
	switch Ordering(s) {
	case FIFO:
		return FIFO, nil
	case LIFO:
		return LIFO, nil
	default:
		return "", fmt.Errorf("ordering must be %q or %q, got %q", FIFO, LIFO, s)
	}
}

type State uint8

const (
	StateReady State = iota + 1
	StateDelayed
	StateInFlight
)

func (s State) String() string {
	switch s {
	case StateReady:
		return "ready"
	case StateDelayed:
		return "delayed"
	case StateInFlight:
		return "in_flight"
	default:
		return "unknown"
	}
}

// Message is queue-owned state. Every field is guarded by the owning queue's mutex,
// so a Message pointer must never escape the engine; callers get a Delivery instead.
type Message struct {
	ID          string
	Seq         uint64
	Priority    int
	Body        json.RawMessage
	CreatedAt   time.Time
	AvailableAt time.Time

	State        State
	ReceiveCount int

	Receipt  string
	Deadline time.Time

	// Position in whichever heap currently holds this message. A message is in at most
	// one heap at a time, so a single field serves all three and keeps heap.Remove O(log n).
	index int
}

// Delivery is an immutable snapshot handed to a consumer. Receive returns a value rather
// than the live *Message because the scheduler may expire and re-deliver that message the
// instant the queue lock is released, which would otherwise race the caller's use of it.
// Body aliases the message's bytes, which are never mutated after enqueue.
type Delivery struct {
	ID           string
	Seq          uint64
	Priority     int
	Body         json.RawMessage
	CreatedAt    time.Time
	AvailableAt  time.Time
	ReceiveCount int
	Receipt      string
	Deadline     time.Time
}

func (m *Message) delivery() Delivery {
	return Delivery{
		ID:           m.ID,
		Seq:          m.Seq,
		Priority:     m.Priority,
		Body:         m.Body,
		CreatedAt:    m.CreatedAt,
		AvailableAt:  m.AvailableAt,
		ReceiveCount: m.ReceiveCount,
		Receipt:      m.Receipt,
		Deadline:     m.Deadline,
	}
}

// newID returns a random UUIDv4 in 8-4-4-4-12 form.
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate message id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:]), nil
}

// newReceipt returns an opaque token identifying one specific delivery. A fresh token per
// delivery is what makes a stale consumer's ack fail rather than acknowledge someone else's
// redelivery of the same message.
func newReceipt() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate receipt: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
