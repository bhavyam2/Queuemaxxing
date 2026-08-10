package queue

import (
	"regexp"
	"testing"
)

var uuidV4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewIDFormatAndUniqueness(t *testing.T) {
	seen := make(map[string]bool, 10000)
	for i := 0; i < 10000; i++ {
		id, err := newID()
		if err != nil {
			t.Fatalf("newID: %v", err)
		}
		if !uuidV4Pattern.MatchString(id) {
			t.Fatalf("id %q is not a UUIDv4", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q at iteration %d", id, i)
		}
		seen[id] = true
	}
}

func TestNewReceiptFormatAndUniqueness(t *testing.T) {
	pattern := regexp.MustCompile(`^[0-9a-f]{32}$`)
	seen := make(map[string]bool, 10000)
	for i := 0; i < 10000; i++ {
		r, err := newReceipt()
		if err != nil {
			t.Fatalf("newReceipt: %v", err)
		}
		if !pattern.MatchString(r) {
			t.Fatalf("receipt %q is not 32 hex characters", r)
		}
		if seen[r] {
			t.Fatalf("duplicate receipt %q at iteration %d", r, i)
		}
		seen[r] = true
	}
}

func TestParseOrdering(t *testing.T) {
	for _, in := range []string{"fifo", "lifo"} {
		if _, err := ParseOrdering(in); err != nil {
			t.Fatalf("ParseOrdering(%q): %v", in, err)
		}
	}
	for _, in := range []string{"", "FIFO", "priority", "fif0"} {
		if _, err := ParseOrdering(in); err == nil {
			t.Fatalf("ParseOrdering(%q) should have failed", in)
		}
	}
}

func TestStateString(t *testing.T) {
	cases := map[State]string{StateReady: "ready", StateDelayed: "delayed", StateInFlight: "in_flight", State(0): "unknown"}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Fatalf("State(%d).String() = %q, want %q", s, got, want)
		}
	}
}
