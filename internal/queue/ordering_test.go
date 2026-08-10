package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

const testVisibility = time.Minute

// newTestQueue builds a queue over an in-memory log. Persistence behaviour is covered by the
// recovery tests, which use a real log on disk.
func newTestQueue(t *testing.T, ordering Ordering) *Queue {
	t.Helper()
	q := newQueue(Config{Name: "q", Ordering: ordering, CreatedAt: time.Now()}, &fakeLog{}, 1, 0, 0)
	q.start()
	t.Cleanup(func() { q.Close() })
	return q
}

// newDiskQueue builds a queue over a real log, paying real fsyncs.
func newDiskQueue(t *testing.T, ordering Ordering) (*Queue, string) {
	t.Helper()
	dir := t.TempDir()
	q, err := createQueue(dir, Config{Name: "q", Ordering: ordering, CreatedAt: time.Now()}, 0)
	if err != nil {
		t.Fatalf("createQueue: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q, dir
}

func body(label string) json.RawMessage {
	b, _ := json.Marshal(map[string]string{"label": label})
	return b
}

// send enqueues and returns the label-to-id mapping so tests can assert on order by label.
func send(t *testing.T, q *Queue, label string, priority int) string {
	t.Helper()
	id, err := q.Enqueue(body(label), priority, 0)
	if err != nil {
		t.Fatalf("Enqueue(%s): %v", label, err)
	}
	return id
}

func drainLabels(t *testing.T, q *Queue) []string {
	t.Helper()
	var out []string
	for {
		d, ok, err := q.Receive(context.Background(), testVisibility, 0)
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if !ok {
			return out
		}
		var payload struct {
			Label string `json:"label"`
		}
		if err := json.Unmarshal(d.Body, &payload); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		out = append(out, payload.Label)
	}
}

func label(t *testing.T, d Delivery) string {
	t.Helper()
	var payload struct {
		Label string `json:"label"`
	}
	if err := json.Unmarshal(d.Body, &payload); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return payload.Label
}

func assertOrder(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("received %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("received %v, want %v", got, want)
		}
	}
}

func TestFIFOOrder(t *testing.T) {
	q := newTestQueue(t, FIFO)
	for _, l := range []string{"a", "b", "c", "d"} {
		send(t, q, l, 0)
	}
	assertOrder(t, drainLabels(t, q), []string{"a", "b", "c", "d"})
}

func TestLIFOOrder(t *testing.T) {
	q := newTestQueue(t, LIFO)
	for _, l := range []string{"a", "b", "c", "d"} {
		send(t, q, l, 0)
	}
	assertOrder(t, drainLabels(t, q), []string{"d", "c", "b", "a"})
}

// TestWorkedExample is the case named in the specification. Priority decides first and the
// queue ordering breaks ties within a priority.
func TestWorkedExample(t *testing.T) {
	cases := []struct {
		ordering Ordering
		want     []string
	}{
		{FIFO, []string{"A", "B", "D", "C"}},
		{LIFO, []string{"D", "B", "A", "C"}},
	}
	for _, tc := range cases {
		t.Run(string(tc.ordering), func(t *testing.T) {
			q := newTestQueue(t, tc.ordering)
			send(t, q, "A", 10) // seq 1
			send(t, q, "B", 10) // seq 2
			send(t, q, "C", 5)  // seq 3
			send(t, q, "D", 10) // seq 4
			assertOrder(t, drainLabels(t, q), tc.want)
		})
	}
}

func TestPriorityBeatsOrdering(t *testing.T) {
	for _, ordering := range []Ordering{FIFO, LIFO} {
		t.Run(string(ordering), func(t *testing.T) {
			q := newTestQueue(t, ordering)
			send(t, q, "low", 0)
			send(t, q, "high", 100)
			got := drainLabels(t, q)
			if got[0] != "high" {
				t.Fatalf("received %v, want the higher priority first", got)
			}
		})
	}
}

func TestNegativePriorities(t *testing.T) {
	q := newTestQueue(t, FIFO)
	send(t, q, "neg", -5)
	send(t, q, "zero", 0)
	send(t, q, "pos", 5)
	assertOrder(t, drainLabels(t, q), []string{"pos", "zero", "neg"})
}

func TestEqualPriorityRuns(t *testing.T) {
	for _, tc := range []struct {
		ordering Ordering
		want     []string
	}{
		{FIFO, []string{"p2a", "p2b", "p2c", "p1a", "p1b", "p1c"}},
		{LIFO, []string{"p2c", "p2b", "p2a", "p1c", "p1b", "p1a"}},
	} {
		t.Run(string(tc.ordering), func(t *testing.T) {
			q := newTestQueue(t, tc.ordering)
			send(t, q, "p1a", 1)
			send(t, q, "p1b", 1)
			send(t, q, "p1c", 1)
			send(t, q, "p2a", 2)
			send(t, q, "p2b", 2)
			send(t, q, "p2c", 2)
			assertOrder(t, drainLabels(t, q), tc.want)
		})
	}
}

// TestLargeNOrdering checks the heap against an independently sorted reference so a
// comparator bug that only shows up past a few elements cannot hide.
func TestLargeNOrdering(t *testing.T) {
	const n = 5000
	for _, ordering := range []Ordering{FIFO, LIFO} {
		t.Run(string(ordering), func(t *testing.T) {
			q := newTestQueue(t, ordering)
			rng := rand.New(rand.NewSource(42))

			type entry struct {
				label    string
				priority int
				seq      uint64
			}
			var want []entry
			for i := 0; i < n; i++ {
				p := rng.Intn(11) - 5
				label := fmt.Sprintf("m%d", i)
				send(t, q, label, p)
				want = append(want, entry{label, p, uint64(i + 1)})
			}

			sort.SliceStable(want, func(i, j int) bool {
				if want[i].priority != want[j].priority {
					return want[i].priority > want[j].priority
				}
				if ordering == FIFO {
					return want[i].seq < want[j].seq
				}
				return want[i].seq > want[j].seq
			})

			got := drainLabels(t, q)
			if len(got) != n {
				t.Fatalf("received %d messages, want %d", len(got), n)
			}
			for i := range want {
				if got[i] != want[i].label {
					t.Fatalf("position %d: received %s, want %s", i, got[i], want[i].label)
				}
			}
		})
	}
}

// TestOrderingOverRealLog repeats the worked example against a real fsynced log, so the
// ordering guarantee is not an artifact of the in-memory test log.
func TestOrderingOverRealLog(t *testing.T) {
	q, _ := newDiskQueue(t, LIFO)
	send(t, q, "A", 10)
	send(t, q, "B", 10)
	send(t, q, "C", 5)
	send(t, q, "D", 10)
	assertOrder(t, drainLabels(t, q), []string{"D", "B", "A", "C"})
}

func TestReceiveOnEmptyQueue(t *testing.T) {
	q := newTestQueue(t, FIFO)
	if _, ok, err := q.Receive(context.Background(), testVisibility, 0); err != nil || ok {
		t.Fatalf("Receive on empty queue = (%v, %v), want (false, nil)", ok, err)
	}
}

func TestReceiveMarksInFlight(t *testing.T) {
	q := newTestQueue(t, FIFO)
	send(t, q, "a", 0)

	d, ok, err := q.Receive(context.Background(), testVisibility, 0)
	if err != nil || !ok {
		t.Fatalf("Receive = (%v, %v)", ok, err)
	}
	if d.Receipt == "" {
		t.Fatal("delivery has no receipt")
	}
	if d.ReceiveCount != 1 {
		t.Fatalf("ReceiveCount = %d on first delivery, want 1", d.ReceiveCount)
	}
	if d.Deadline.Before(time.Now()) {
		t.Fatal("visibility deadline is already in the past")
	}

	st := q.Stats()
	if st.Ready != 0 || st.InFlight != 1 || st.TotalEnqueued != 1 {
		t.Fatalf("stats = %+v, want ready 0, in flight 1, enqueued 1", st)
	}

	// The message must not be handed out twice while it is in flight.
	if _, ok, _ := q.Receive(context.Background(), testVisibility, 0); ok {
		t.Fatal("an in-flight message was delivered a second time")
	}
}

func TestDistinctReceiptsPerDelivery(t *testing.T) {
	q := newTestQueue(t, FIFO)
	send(t, q, "a", 0)
	send(t, q, "b", 0)
	first, _, _ := q.Receive(context.Background(), testVisibility, 0)
	second, _, _ := q.Receive(context.Background(), testVisibility, 0)
	if first.Receipt == second.Receipt {
		t.Fatal("two deliveries share a receipt")
	}
}

func TestEnqueueRejectsEmptyBody(t *testing.T) {
	q := newTestQueue(t, FIFO)
	if _, err := q.Enqueue(nil, 0, 0); err != ErrEmptyBody {
		t.Fatalf("Enqueue(nil) = %v, want ErrEmptyBody", err)
	}
}

func TestEnqueueRejectsNegativeDelay(t *testing.T) {
	q := newTestQueue(t, FIFO)
	if _, err := q.Enqueue(body("x"), 0, -time.Second); err == nil {
		t.Fatal("Enqueue with a negative delay should fail")
	}
}

func TestEnqueueCopiesBody(t *testing.T) {
	q := newTestQueue(t, FIFO)
	raw := []byte(`{"label":"original"}`)
	if _, err := q.Enqueue(raw, 0, 0); err != nil {
		t.Fatal(err)
	}
	// Simulate the HTTP layer reusing its read buffer once the handler returns.
	copy(raw, []byte(`{"label":"OVERWRIT"}`))

	d, ok, _ := q.Receive(context.Background(), testVisibility, 0)
	if !ok {
		t.Fatal("no message")
	}
	var payload struct {
		Label string `json:"label"`
	}
	json.Unmarshal(d.Body, &payload)
	if payload.Label != "original" {
		t.Fatalf("body = %q, want %q; the queue did not copy the caller's bytes", payload.Label, "original")
	}
}

func TestValidateName(t *testing.T) {
	valid := []string{"a", "orders", "Q1", "my-queue_2", "0"}
	for _, n := range valid {
		if err := ValidateName(n); err != nil {
			t.Fatalf("ValidateName(%q): %v", n, err)
		}
	}
	invalid := []string{"", "-leading", "_leading", "../escape", "a/b", "has space", "tab\t", "q.log", string(make([]byte, 65))}
	for _, n := range invalid {
		if err := ValidateName(n); err == nil {
			t.Fatalf("ValidateName(%q) should have failed", n)
		}
	}
}
