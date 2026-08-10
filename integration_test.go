package queuemaxxing_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"queuemaxxing/internal/api"
	"queuemaxxing/internal/queue"
)

// stack is a full HTTP-to-disk instance over one data directory. Restarting it exercises the
// same path the server takes on boot: recover every log, then start serving.
type stack struct {
	t       *testing.T
	dir     string
	manager *queue.Manager
	server  *httptest.Server
}

func newStack(t *testing.T) *stack {
	t.Helper()
	s := &stack{t: t, dir: t.TempDir()}
	s.start()
	t.Cleanup(func() { s.stop() })
	return s
}

func (s *stack) start() {
	s.t.Helper()
	m, err := queue.NewManager(s.dir, 10000)
	if err != nil {
		s.t.Fatalf("NewManager: %v", err)
	}
	s.manager = m
	s.server = httptest.NewServer(api.NewRouter(m, nil))
}

func (s *stack) stop() {
	if s.server != nil {
		s.server.Close()
		s.server = nil
	}
	if s.manager != nil {
		s.manager.Close()
		s.manager = nil
	}
}

// crash drops the server without a clean shutdown: no compaction, no log close. Only records
// already fsynced survive, which is what a kill -9 leaves behind.
func (s *stack) crash() {
	s.t.Helper()
	if s.server != nil {
		s.server.Close()
		s.server = nil
	}
	s.manager = nil
}

func (s *stack) restart() {
	s.t.Helper()
	s.stop()
	s.start()
}

func (s *stack) do(method, path string, body any) (int, map[string]any) {
	s.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			s.t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, s.server.URL+path, reader)
	if err != nil {
		s.t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var decoded map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		json.Unmarshal(raw, &decoded)
	}
	return res.StatusCode, decoded
}

func (s *stack) mustStatus(want int, method, path string, body any) map[string]any {
	s.t.Helper()
	status, decoded := s.do(method, path, body)
	if status != want {
		s.t.Fatalf("%s %s: status %d, want %d (body %v)", method, path, status, want, decoded)
	}
	return decoded
}

func (s *stack) stats(name string) map[string]float64 {
	s.t.Helper()
	raw := s.mustStatus(http.StatusOK, "GET", "/queues/"+name+"/stats", nil)
	out := make(map[string]float64, len(raw))
	for k, v := range raw {
		out[k], _ = v.(float64)
	}
	return out
}

func (s *stack) assertStats(name string, want map[string]float64) {
	s.t.Helper()
	got := s.stats(name)
	for k, v := range want {
		if got[k] != v {
			s.t.Fatalf("queue %s: stats[%s] = %v, want %v (full: %v)", name, k, got[k], v, got)
		}
	}
}

// TestRestartBehaviourOverHTTP drives the specification's restart table through the real API
// and a crash: a ready message stays available, an in-flight one is redelivered, a delayed one
// stays delayed, and an acked one is gone.
func TestRestartBehaviourOverHTTP(t *testing.T) {
	s := newStack(t)
	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "orders", "ordering": "fifo"})

	for _, label := range []string{"A", "B", "D"} {
		s.mustStatus(http.StatusCreated, "POST", "/queues/orders/messages",
			map[string]any{"body": map[string]string{"label": label}})
	}
	s.mustStatus(http.StatusCreated, "POST", "/queues/orders/messages",
		map[string]any{"body": map[string]string{"label": "C"}, "delay_seconds": 3600})

	// Draw all three ready messages, put A back, ack D, leave B in flight.
	receipts := map[string]string{}
	for i := 0; i < 3; i++ {
		msg := s.mustStatus(http.StatusOK, "POST", "/queues/orders/receive",
			map[string]any{"visibility_timeout_seconds": 600})
		label := msg["body"].(map[string]any)["label"].(string)
		receipts[label] = msg["receipt"].(string)
	}
	s.mustStatus(http.StatusOK, "POST", "/queues/orders/nack", map[string]any{"receipt": receipts["A"]})
	s.mustStatus(http.StatusOK, "POST", "/queues/orders/ack", map[string]any{"receipt": receipts["D"]})

	s.assertStats("orders", map[string]float64{
		"ready": 1, "in_flight": 1, "delayed": 1, "total_enqueued": 4, "total_acked": 1,
	})

	s.crash()
	s.start()

	s.assertStats("orders", map[string]float64{
		"ready": 2, "in_flight": 0, "delayed": 1, "total_enqueued": 4, "total_acked": 1,
	})

	// A was never acked and B comes back for redelivery; D must not reappear.
	var seen []string
	for i := 0; i < 2; i++ {
		msg := s.mustStatus(http.StatusOK, "POST", "/queues/orders/receive", map[string]any{})
		seen = append(seen, msg["body"].(map[string]any)["label"].(string))
	}
	if len(seen) != 2 || (seen[0] != "A" && seen[1] != "A") || (seen[0] != "B" && seen[1] != "B") {
		t.Fatalf("received %v after restart, want A and B", seen)
	}
	s.mustStatus(http.StatusNoContent, "POST", "/queues/orders/receive", map[string]any{})

	// A receipt minted before the crash cannot settle a post-restart delivery.
	s.mustStatus(http.StatusNotFound, "POST", "/queues/orders/ack", map[string]any{"receipt": receipts["B"]})
}

func TestCleanShutdownAndReopen(t *testing.T) {
	s := newStack(t)
	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "events", "ordering": "lifo"})
	for i := 0; i < 5; i++ {
		s.mustStatus(http.StatusCreated, "POST", "/queues/events/messages",
			map[string]any{"body": map[string]int{"n": i}})
	}
	s.restart()

	s.assertStats("events", map[string]float64{"ready": 5, "total_enqueued": 5})
	view := s.mustStatus(http.StatusOK, "GET", "/queues/events", nil)
	if view["ordering"] != "lifo" {
		t.Fatalf("ordering = %v after restart, want lifo", view["ordering"])
	}
	// LIFO must still hand back the newest first, which needs the persisted sequence numbers.
	msg := s.mustStatus(http.StatusOK, "POST", "/queues/events/receive", map[string]any{})
	if n := msg["body"].(map[string]any)["n"].(float64); n != 4 {
		t.Fatalf("received n=%v, want 4", n)
	}
}

// TestCrashAtEveryStageOfTheLifecycle reopens the data directory after a crash at each point
// where state changes, and asserts the exact state that survived.
func TestCrashAtEveryStageOfTheLifecycle(t *testing.T) {
	cases := []struct {
		name   string
		act    func(s *stack)
		expect map[string]float64
	}{
		{
			name: "after enqueue",
			act: func(s *stack) {
				s.mustStatus(http.StatusCreated, "POST", "/queues/q/messages", map[string]any{"body": map[string]int{"n": 1}})
			},
			expect: map[string]float64{"ready": 1, "in_flight": 0, "delayed": 0, "total_enqueued": 1, "total_acked": 0},
		},
		{
			name: "after delivery",
			act: func(s *stack) {
				s.mustStatus(http.StatusCreated, "POST", "/queues/q/messages", map[string]any{"body": map[string]int{"n": 1}})
				s.mustStatus(http.StatusOK, "POST", "/queues/q/receive", map[string]any{"visibility_timeout_seconds": 600})
			},
			expect: map[string]float64{"ready": 1, "in_flight": 0, "total_enqueued": 1, "total_acked": 0},
		},
		{
			name: "after ack",
			act: func(s *stack) {
				s.mustStatus(http.StatusCreated, "POST", "/queues/q/messages", map[string]any{"body": map[string]int{"n": 1}})
				msg := s.mustStatus(http.StatusOK, "POST", "/queues/q/receive", map[string]any{})
				s.mustStatus(http.StatusOK, "POST", "/queues/q/ack", map[string]any{"receipt": msg["receipt"]})
			},
			expect: map[string]float64{"ready": 0, "in_flight": 0, "delayed": 0, "total_enqueued": 1, "total_acked": 1},
		},
		{
			name: "after immediate nack",
			act: func(s *stack) {
				s.mustStatus(http.StatusCreated, "POST", "/queues/q/messages", map[string]any{"body": map[string]int{"n": 1}})
				msg := s.mustStatus(http.StatusOK, "POST", "/queues/q/receive", map[string]any{})
				s.mustStatus(http.StatusOK, "POST", "/queues/q/nack", map[string]any{"receipt": msg["receipt"]})
			},
			expect: map[string]float64{"ready": 1, "delayed": 0, "total_enqueued": 1, "total_acked": 0},
		},
		{
			name: "after nack with retry delay",
			act: func(s *stack) {
				s.mustStatus(http.StatusCreated, "POST", "/queues/q/messages", map[string]any{"body": map[string]int{"n": 1}})
				msg := s.mustStatus(http.StatusOK, "POST", "/queues/q/receive", map[string]any{})
				s.mustStatus(http.StatusOK, "POST", "/queues/q/nack", map[string]any{"receipt": msg["receipt"], "delay_seconds": 3600})
			},
			expect: map[string]float64{"ready": 0, "delayed": 1, "total_enqueued": 1, "total_acked": 0},
		},
		{
			name: "after a delayed message is promoted",
			act: func(s *stack) {
				s.mustStatus(http.StatusCreated, "POST", "/queues/q/messages",
					map[string]any{"body": map[string]int{"n": 1}, "delay_seconds": 1})
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					if s.stats("q")["ready"] == 1 {
						return
					}
					time.Sleep(20 * time.Millisecond)
				}
				s.t.Fatal("delayed message was never promoted")
			},
			expect: map[string]float64{"ready": 1, "delayed": 0, "total_enqueued": 1},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newStack(t)
			s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "q", "ordering": "fifo"})
			tc.act(s)
			s.crash()
			s.start()
			s.assertStats("q", tc.expect)
		})
	}
}

// TestConcurrentHTTPProducersAndConsumers runs the exactly-once claim through the real HTTP
// stack, with every append fsynced to a real file.
func TestConcurrentHTTPProducersAndConsumers(t *testing.T) {
	s := newStack(t)
	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "work", "ordering": "fifo"})

	const (
		producers   = 8
		perProducer = 25
		consumers   = 6
		total       = producers * perProducer
	)

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				status, _ := s.do("POST", "/queues/work/messages", map[string]any{
					"body":     map[string]int{"producer": p, "index": i},
					"priority": i % 3,
				})
				if status != http.StatusCreated {
					t.Errorf("send: status %d", status)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	var (
		mu       sync.Mutex
		ackedIDs = make(map[string]int)
	)
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				done := len(ackedIDs) == total
				mu.Unlock()
				if done {
					return
				}
				status, msg := s.do("POST", "/queues/work/receive",
					map[string]any{"visibility_timeout_seconds": 300, "wait_seconds": 1})
				if status == http.StatusNoContent {
					continue
				}
				if status != http.StatusOK {
					t.Errorf("receive: status %d", status)
					return
				}
				if status, _ := s.do("POST", "/queues/work/ack", map[string]any{"receipt": msg["receipt"]}); status != http.StatusOK {
					t.Errorf("ack: status %d", status)
					return
				}
				mu.Lock()
				ackedIDs[msg["message_id"].(string)]++
				mu.Unlock()
			}
		}()
	}

	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(90 * time.Second):
		t.Fatal("consumers did not drain the queue over HTTP")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ackedIDs) != total {
		t.Fatalf("acked %d distinct messages, want %d", len(ackedIDs), total)
	}
	for id, n := range ackedIDs {
		if n != 1 {
			t.Fatalf("message %s acked %d times, want 1", id, n)
		}
	}
	s.assertStats("work", map[string]float64{
		"ready": 0, "in_flight": 0, "delayed": 0, "total_enqueued": total, "total_acked": total,
	})

	// The whole run must survive a restart with its counters intact.
	s.restart()
	s.assertStats("work", map[string]float64{"total_enqueued": total, "total_acked": total, "ready": 0})
}

func TestMultipleQueuesAreIndependent(t *testing.T) {
	s := newStack(t)
	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "first", "ordering": "fifo"})
	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "second", "ordering": "lifo"})

	for i := 0; i < 3; i++ {
		s.mustStatus(http.StatusCreated, "POST", "/queues/first/messages", map[string]any{"body": map[string]int{"n": i}})
	}
	s.mustStatus(http.StatusCreated, "POST", "/queues/second/messages", map[string]any{"body": map[string]int{"n": 99}})

	s.assertStats("first", map[string]float64{"ready": 3})
	s.assertStats("second", map[string]float64{"ready": 1})

	// Each queue owns its own file, so deleting one must not touch the other.
	s.mustStatus(http.StatusNoContent, "DELETE", "/queues/first", nil)
	if _, err := os.Stat(filepath.Join(s.dir, "first.log")); !os.IsNotExist(err) {
		t.Fatal("deleting a queue left its log behind")
	}
	if _, err := os.Stat(filepath.Join(s.dir, "second.log")); err != nil {
		t.Fatalf("deleting one queue disturbed another: %v", err)
	}
	s.restart()
	s.assertStats("second", map[string]float64{"ready": 1})
	s.mustStatus(http.StatusNotFound, "GET", "/queues/first", nil)
}

// TestCompactionAcrossRestart pushes a queue past the compaction threshold through HTTP and
// checks the log shrank while the state and counters came through unchanged.
func TestCompactionAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	m, err := queue.NewManager(dir, 40)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.NewRouter(m, nil))
	s := &stack{t: t, dir: dir, manager: m, server: srv}

	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "churn", "ordering": "fifo"})
	const cycles = 120
	for i := 0; i < cycles; i++ {
		s.mustStatus(http.StatusCreated, "POST", "/queues/churn/messages", map[string]any{"body": map[string]int{"n": i}})
		msg := s.mustStatus(http.StatusOK, "POST", "/queues/churn/receive", map[string]any{})
		s.mustStatus(http.StatusOK, "POST", "/queues/churn/ack", map[string]any{"receipt": msg["receipt"]})
	}
	s.mustStatus(http.StatusCreated, "POST", "/queues/churn/messages", map[string]any{"body": map[string]int{"n": 999}})

	info, err := os.Stat(filepath.Join(dir, "churn.log"))
	if err != nil {
		t.Fatal(err)
	}
	// An uncompacted log would hold 242 records; the compacted one is a small fraction.
	if info.Size() > 20000 {
		t.Fatalf("log is %d bytes after %d cycles; compaction did not run", info.Size(), cycles)
	}

	srv.Close()
	m.Close()

	m2, err := queue.NewManager(dir, 40)
	if err != nil {
		t.Fatalf("reopen after compaction: %v", err)
	}
	s.manager = m2
	s.server = httptest.NewServer(api.NewRouter(m2, nil))
	defer func() { s.server.Close(); m2.Close() }()

	s.assertStats("churn", map[string]float64{
		"ready": 1, "in_flight": 0, "delayed": 0,
		"total_enqueued": cycles + 1, "total_acked": cycles,
	})
	msg := s.mustStatus(http.StatusOK, "POST", "/queues/churn/receive", map[string]any{})
	if n := msg["body"].(map[string]any)["n"].(float64); n != 999 {
		t.Fatalf("recovered message n=%v, want 999", n)
	}
	if seq := msg["sequence"].(float64); seq != cycles+1 {
		t.Fatalf("sequence = %v, want %d; the sequence floor was lost in compaction", seq, cycles+1)
	}
}

func TestTornTailAcrossFullStack(t *testing.T) {
	s := newStack(t)
	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "orders", "ordering": "fifo"})
	s.mustStatus(http.StatusCreated, "POST", "/queues/orders/messages", map[string]any{"body": map[string]string{"label": "durable"}})
	s.mustStatus(http.StatusCreated, "POST", "/queues/orders/messages", map[string]any{"body": map[string]string{"label": "interrupted"}})
	s.crash()

	// Cut the final frame in half, which is what an interrupted append leaves on disk.
	path := filepath.Join(s.dir, "orders.log")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-25); err != nil {
		t.Fatal(err)
	}

	s.start()
	s.assertStats("orders", map[string]float64{"ready": 1, "total_enqueued": 1})
	msg := s.mustStatus(http.StatusOK, "POST", "/queues/orders/receive", map[string]any{})
	if label := msg["body"].(map[string]any)["label"].(string); label != "durable" {
		t.Fatalf("recovered %q, want durable", label)
	}
	// The truncated log must accept new writes.
	s.mustStatus(http.StatusCreated, "POST", "/queues/orders/messages", map[string]any{"body": map[string]string{"label": "after"}})
}

func TestLongPollAcrossFullStack(t *testing.T) {
	s := newStack(t)
	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "orders", "ordering": "fifo"})

	type result struct {
		status  int
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		status, _ := s.do("POST", "/queues/orders/receive", map[string]any{"wait_seconds": 10})
		done <- result{status, time.Since(start)}
	}()

	time.Sleep(100 * time.Millisecond)
	s.mustStatus(http.StatusCreated, "POST", "/queues/orders/messages", map[string]any{"body": map[string]int{"n": 1}})

	select {
	case r := <-done:
		if r.status != http.StatusOK {
			t.Fatalf("long poll status %d, want 200", r.status)
		}
		if r.elapsed > 3*time.Second {
			t.Fatalf("long poll took %v; it did not wake on the enqueue", r.elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("long poll never returned")
	}
}

func TestDataDirectoryLayout(t *testing.T) {
	s := newStack(t)
	for _, name := range []string{"alpha", "beta"} {
		s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": name, "ordering": "fifo"})
		s.mustStatus(http.StatusCreated, "POST", "/queues/"+name+"/messages", map[string]any{"body": map[string]int{"n": 1}})
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, e := range entries {
		got[e.Name()] = true
	}
	for _, want := range []string{"alpha.log", "beta.log"} {
		if !got[want] {
			t.Fatalf("data directory holds %v, want %s", got, want)
		}
	}
	if len(entries) != 2 {
		t.Fatalf("data directory holds %d files, want exactly one log per queue: %v", len(entries), got)
	}
}

func TestHealthEndpoint(t *testing.T) {
	s := newStack(t)
	body := s.mustStatus(http.StatusOK, "GET", "/health", nil)
	if body["status"] != "ok" {
		t.Fatalf("health = %v", body)
	}
}

// TestManagerRefusesCorruptLogAtStartup checks the whole server declines to start rather than
// silently serving a queue that is missing acknowledged records.
func TestManagerRefusesCorruptLogAtStartup(t *testing.T) {
	s := newStack(t)
	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "orders", "ordering": "fifo"})
	for i := 0; i < 6; i++ {
		s.mustStatus(http.StatusCreated, "POST", "/queues/orders/messages", map[string]any{"body": map[string]int{"n": i}})
	}
	dir := s.dir
	s.stop()

	f, err := os.OpenFile(filepath.Join(dir, "orders.log"), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], 80); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	f.WriteAt(b[:], 80)
	f.Close()

	if _, err := queue.NewManager(dir, 10000); err == nil {
		t.Fatal("the server started despite corruption in the middle of a log")
	} else if !bytes.Contains([]byte(err.Error()), []byte("offset")) {
		t.Fatalf("error %q does not report the byte offset", err)
	}
}

func TestSendReceiveWithLargeBody(t *testing.T) {
	s := newStack(t)
	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "orders", "ordering": "fifo"})

	blob := make([]byte, 256*1024)
	for i := range blob {
		blob[i] = byte('a' + i%26)
	}
	s.mustStatus(http.StatusCreated, "POST", "/queues/orders/messages",
		map[string]any{"body": map[string]string{"blob": string(blob)}})
	s.restart()

	msg := s.mustStatus(http.StatusOK, "POST", "/queues/orders/receive", map[string]any{})
	got := msg["body"].(map[string]any)["blob"].(string)
	if got != string(blob) {
		t.Fatalf("large body did not survive the restart: %d bytes back, want %d", len(got), len(blob))
	}
}

func TestPriorityAndDelayCombined(t *testing.T) {
	s := newStack(t)
	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "orders", "ordering": "lifo"})

	// A high priority message that is not yet available must not jump the queue.
	s.mustStatus(http.StatusCreated, "POST", "/queues/orders/messages",
		map[string]any{"body": map[string]string{"label": "urgent-later"}, "priority": 100, "delay_seconds": 3600})
	s.mustStatus(http.StatusCreated, "POST", "/queues/orders/messages",
		map[string]any{"body": map[string]string{"label": "normal-now"}, "priority": 0})

	msg := s.mustStatus(http.StatusOK, "POST", "/queues/orders/receive", map[string]any{})
	if label := msg["body"].(map[string]any)["label"].(string); label != "normal-now" {
		t.Fatalf("received %q, want normal-now; a delayed message was delivered early", label)
	}
	s.assertStats("orders", map[string]float64{"delayed": 1, "ready": 0, "in_flight": 1})
}

func TestVisibilityTimeoutOverHTTP(t *testing.T) {
	s := newStack(t)
	s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": "orders", "ordering": "fifo"})
	s.mustStatus(http.StatusCreated, "POST", "/queues/orders/messages", map[string]any{"body": map[string]int{"n": 1}})

	first := s.mustStatus(http.StatusOK, "POST", "/queues/orders/receive",
		map[string]any{"visibility_timeout_seconds": 1})
	if first["receive_count"].(float64) != 1 {
		t.Fatalf("receive_count = %v on first delivery, want 1", first["receive_count"])
	}
	s.mustStatus(http.StatusNoContent, "POST", "/queues/orders/receive", map[string]any{})

	second := s.mustStatus(http.StatusOK, "POST", "/queues/orders/receive",
		map[string]any{"wait_seconds": 5})
	if second["receive_count"].(float64) != 2 {
		t.Fatalf("receive_count = %v on redelivery, want 2", second["receive_count"])
	}
	if second["receipt"] == first["receipt"] {
		t.Fatal("redelivery reused the original receipt")
	}
	s.mustStatus(http.StatusNotFound, "POST", "/queues/orders/ack", map[string]any{"receipt": first["receipt"]})
	s.mustStatus(http.StatusOK, "POST", "/queues/orders/ack", map[string]any{"receipt": second["receipt"]})
}

func TestQueueListReportsStats(t *testing.T) {
	s := newStack(t)
	for i, name := range []string{"aaa", "bbb"} {
		s.mustStatus(http.StatusCreated, "POST", "/queues", map[string]string{"name": name, "ordering": "fifo"})
		for j := 0; j <= i; j++ {
			s.mustStatus(http.StatusCreated, "POST", "/queues/"+name+"/messages", map[string]any{"body": map[string]int{"n": j}})
		}
	}
	res, err := http.Get(s.server.URL + "/queues")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var list []struct {
		Name  string `json:"name"`
		Stats struct {
			Ready int `json:"ready"`
		} `json:"stats"`
	}
	if err := json.NewDecoder(res.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("listed %d queues, want 2", len(list))
	}
	if list[0].Name != "aaa" || list[0].Stats.Ready != 1 || list[1].Stats.Ready != 2 {
		t.Fatalf("list = %+v", list)
	}
}
