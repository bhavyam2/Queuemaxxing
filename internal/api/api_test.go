package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"queuemaxxing/internal/queue"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	m, err := queue.NewManager(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	srv := httptest.NewServer(NewRouter(m, nil))
	t.Cleanup(srv.Close)
	return srv
}

// do issues a request and returns the status plus decoded body. A nil body sends no payload.
func do(t *testing.T, srv *httptest.Server, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, srv.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	raw, _ := io.ReadAll(res.Body)
	var decoded map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			// List endpoints return arrays; callers that need those decode separately.
			return res.StatusCode, nil
		}
	}
	return res.StatusCode, decoded
}

func createQueue(t *testing.T, srv *httptest.Server, name, ordering string) {
	t.Helper()
	if status, body := do(t, srv, "POST", "/queues", map[string]string{"name": name, "ordering": ordering}); status != http.StatusCreated {
		t.Fatalf("create %s: status %d body %v", name, status, body)
	}
}

func TestCreateQueue(t *testing.T) {
	srv := newTestServer(t)

	status, body := do(t, srv, "POST", "/queues", map[string]string{"name": "orders", "ordering": "fifo"})
	if status != http.StatusCreated {
		t.Fatalf("status %d, want 201", status)
	}
	if body["name"] != "orders" || body["ordering"] != "fifo" {
		t.Fatalf("body = %v", body)
	}

	if status, _ := do(t, srv, "POST", "/queues", map[string]string{"name": "orders", "ordering": "fifo"}); status != http.StatusConflict {
		t.Fatalf("duplicate create returned %d, want 409", status)
	}
}

func TestCreateQueueValidation(t *testing.T) {
	srv := newTestServer(t)
	cases := []struct {
		name string
		req  map[string]string
	}{
		{"missing name", map[string]string{"ordering": "fifo"}},
		{"bad ordering", map[string]string{"name": "q", "ordering": "sideways"}},
		{"missing ordering", map[string]string{"name": "q"}},
		{"path traversal", map[string]string{"name": "../escape", "ordering": "fifo"}},
		{"name with slash", map[string]string{"name": "a/b", "ordering": "fifo"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if status, _ := do(t, srv, "POST", "/queues", tc.req); status != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", status)
			}
		})
	}
}

func TestMalformedJSON(t *testing.T) {
	srv := newTestServer(t)
	res, err := http.Post(srv.URL+"/queues", "application/json", strings.NewReader("{not json"))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", res.StatusCode)
	}
}

func TestUnknownQueue(t *testing.T) {
	srv := newTestServer(t)
	for _, tc := range []struct {
		method, path string
		body         any
	}{
		{"GET", "/queues/missing", nil},
		{"GET", "/queues/missing/stats", nil},
		{"DELETE", "/queues/missing", nil},
		{"POST", "/queues/missing/messages", map[string]any{"body": map[string]int{"a": 1}}},
		{"POST", "/queues/missing/receive", map[string]any{}},
		{"POST", "/queues/missing/ack", map[string]string{"receipt": "x"}},
		{"POST", "/queues/missing/nack", map[string]string{"receipt": "x"}},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			if status, _ := do(t, srv, tc.method, tc.path, tc.body); status != http.StatusNotFound {
				t.Fatalf("status %d, want 404", status)
			}
		})
	}
}

func TestMethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")
	if status, _ := do(t, srv, "PUT", "/queues/orders", nil); status != http.StatusMethodNotAllowed {
		t.Fatalf("status %d, want 405", status)
	}
}

func TestSendReceiveAckCycle(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")

	status, body := do(t, srv, "POST", "/queues/orders/messages", map[string]any{
		"body":     map[string]string{"task": "resize"},
		"priority": 3,
	})
	if status != http.StatusCreated {
		t.Fatalf("send status %d, want 201", status)
	}
	id, _ := body["message_id"].(string)
	if id == "" {
		t.Fatalf("send returned no message_id: %v", body)
	}

	status, msg := do(t, srv, "POST", "/queues/orders/receive", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("receive status %d, want 200", status)
	}
	if msg["message_id"] != id {
		t.Fatalf("received %v, want %s", msg["message_id"], id)
	}
	if msg["priority"] != float64(3) {
		t.Fatalf("priority = %v, want 3", msg["priority"])
	}
	if msg["receive_count"] != float64(1) {
		t.Fatalf("receive_count = %v, want 1", msg["receive_count"])
	}
	inner, ok := msg["body"].(map[string]any)
	if !ok || inner["task"] != "resize" {
		t.Fatalf("body round trip failed: %v", msg["body"])
	}
	receipt, _ := msg["receipt"].(string)
	if receipt == "" {
		t.Fatal("no receipt in delivery")
	}

	if status, _ := do(t, srv, "POST", "/queues/orders/ack", map[string]string{"receipt": receipt}); status != http.StatusOK {
		t.Fatalf("ack status %d, want 200", status)
	}
	if status, _ := do(t, srv, "POST", "/queues/orders/ack", map[string]string{"receipt": receipt}); status != http.StatusNotFound {
		t.Fatalf("second ack status %d, want 404", status)
	}
}

func TestReceiveEmptyReturns204(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")
	if status, _ := do(t, srv, "POST", "/queues/orders/receive", map[string]any{}); status != http.StatusNoContent {
		t.Fatalf("status %d, want 204", status)
	}
}

// TestReceiveWithNoRequestBody covers a bare POST: every field is optional, so an absent body
// must be read as "use the defaults" rather than as malformed input.
func TestReceiveWithNoRequestBody(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")
	if status, _ := do(t, srv, "POST", "/queues/orders/receive", nil); status != http.StatusNoContent {
		t.Fatalf("status %d, want 204", status)
	}
}

func TestSendRequiresBody(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")
	if status, _ := do(t, srv, "POST", "/queues/orders/messages", map[string]any{"priority": 1}); status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", status)
	}
}

func TestAckRequiresReceipt(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")
	if status, _ := do(t, srv, "POST", "/queues/orders/ack", map[string]any{}); status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", status)
	}
	if status, _ := do(t, srv, "POST", "/queues/orders/ack", map[string]string{"receipt": "nonexistent"}); status != http.StatusNotFound {
		t.Fatalf("status %d, want 404", status)
	}
}

func TestParameterBounds(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")
	cases := []struct {
		name string
		path string
		req  map[string]any
	}{
		{"negative delay", "/queues/orders/messages", map[string]any{"body": map[string]int{"a": 1}, "delay_seconds": -1}},
		{"delay too large", "/queues/orders/messages", map[string]any{"body": map[string]int{"a": 1}, "delay_seconds": 999999999}},
		{"negative wait", "/queues/orders/receive", map[string]any{"wait_seconds": -1}},
		{"wait too large", "/queues/orders/receive", map[string]any{"wait_seconds": 61}},
		{"negative visibility", "/queues/orders/receive", map[string]any{"visibility_timeout_seconds": -1}},
		{"visibility too large", "/queues/orders/receive", map[string]any{"visibility_timeout_seconds": 99999999}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if status, _ := do(t, srv, "POST", tc.path, tc.req); status != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", status)
			}
		})
	}
}

func TestOversizedBodyRejected(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")

	huge := strings.Repeat("x", 9<<20)
	payload := `{"body":{"blob":"` + huge + `"}}`
	res, err := http.Post(srv.URL+"/queues/orders/messages", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", res.StatusCode)
	}
}

func TestNackReturnsMessage(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")
	do(t, srv, "POST", "/queues/orders/messages", map[string]any{"body": map[string]int{"n": 1}})

	_, msg := do(t, srv, "POST", "/queues/orders/receive", map[string]any{})
	receipt := msg["receipt"].(string)

	if status, _ := do(t, srv, "POST", "/queues/orders/nack", map[string]any{"receipt": receipt}); status != http.StatusOK {
		t.Fatalf("nack status %d, want 200", status)
	}
	status, again := do(t, srv, "POST", "/queues/orders/receive", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("receive after nack status %d, want 200", status)
	}
	if again["receive_count"] != float64(2) {
		t.Fatalf("receive_count = %v after nack, want 2", again["receive_count"])
	}
}

func TestNackWithDelayHidesMessage(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")
	do(t, srv, "POST", "/queues/orders/messages", map[string]any{"body": map[string]int{"n": 1}})
	_, msg := do(t, srv, "POST", "/queues/orders/receive", map[string]any{})

	if status, _ := do(t, srv, "POST", "/queues/orders/nack", map[string]any{"receipt": msg["receipt"], "delay_seconds": 30}); status != http.StatusOK {
		t.Fatalf("nack status %d, want 200", status)
	}
	if status, _ := do(t, srv, "POST", "/queues/orders/receive", map[string]any{}); status != http.StatusNoContent {
		t.Fatalf("receive status %d, want 204 while the retry delay is pending", status)
	}
	_, stats := do(t, srv, "GET", "/queues/orders/stats", nil)
	if stats["delayed"] != float64(1) {
		t.Fatalf("stats = %v, want delayed 1", stats)
	}
}

func TestStatsEndpoint(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")
	for i := 0; i < 3; i++ {
		do(t, srv, "POST", "/queues/orders/messages", map[string]any{"body": map[string]int{"n": i}})
	}
	_, msg := do(t, srv, "POST", "/queues/orders/receive", map[string]any{})
	do(t, srv, "POST", "/queues/orders/ack", map[string]string{"receipt": msg["receipt"].(string)})

	status, stats := do(t, srv, "GET", "/queues/orders/stats", nil)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200", status)
	}
	want := map[string]float64{"ready": 2, "delayed": 0, "in_flight": 0, "total_enqueued": 3, "total_acked": 1}
	for k, v := range want {
		if stats[k] != v {
			t.Fatalf("stats[%s] = %v, want %v (full: %v)", k, stats[k], v, stats)
		}
	}
}

func TestListQueues(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "alpha", "fifo")
	createQueue(t, srv, "beta", "lifo")

	res, err := http.Get(srv.URL + "/queues")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var list []map[string]any
	if err := json.NewDecoder(res.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0]["name"] != "alpha" || list[1]["name"] != "beta" {
		t.Fatalf("list = %v, want alpha then beta", list)
	}
	if list[1]["ordering"] != "lifo" {
		t.Fatalf("beta ordering = %v, want lifo", list[1]["ordering"])
	}
}

func TestDeleteQueue(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")
	if status, _ := do(t, srv, "DELETE", "/queues/orders", nil); status != http.StatusNoContent {
		t.Fatalf("status %d, want 204", status)
	}
	if status, _ := do(t, srv, "GET", "/queues/orders", nil); status != http.StatusNotFound {
		t.Fatalf("status %d after delete, want 404", status)
	}
}

func TestLongPollWakesOnSend(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")

	done := make(chan int, 1)
	go func() {
		status, _ := do(t, srv, "POST", "/queues/orders/receive", map[string]any{"wait_seconds": 5})
		done <- status
	}()

	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	do(t, srv, "POST", "/queues/orders/messages", map[string]any{"body": map[string]int{"n": 1}})

	select {
	case status := <-done:
		if status != http.StatusOK {
			t.Fatalf("long poll status %d, want 200", status)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("long poll took %v to notice the send", elapsed)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("long poll never returned")
	}
}

func TestPriorityOrderingOverHTTP(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "lifo")

	// The worked example from the specification, driven entirely through the API.
	for _, m := range []struct {
		label    string
		priority int
	}{{"A", 10}, {"B", 10}, {"C", 5}, {"D", 10}} {
		status, _ := do(t, srv, "POST", "/queues/orders/messages", map[string]any{
			"body":     map[string]string{"label": m.label},
			"priority": m.priority,
		})
		if status != http.StatusCreated {
			t.Fatalf("send %s: status %d", m.label, status)
		}
	}

	var got []string
	for i := 0; i < 4; i++ {
		status, msg := do(t, srv, "POST", "/queues/orders/receive", map[string]any{})
		if status != http.StatusOK {
			t.Fatalf("receive %d: status %d", i, status)
		}
		got = append(got, msg["body"].(map[string]any)["label"].(string))
	}
	if want := "D B A C"; strings.Join(got, " ") != want {
		t.Fatalf("received %v, want %s", got, want)
	}
}

func TestDelayOverHTTP(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")

	do(t, srv, "POST", "/queues/orders/messages", map[string]any{"body": map[string]int{"n": 1}, "delay_seconds": 60})
	_, stats := do(t, srv, "GET", "/queues/orders/stats", nil)
	if stats["delayed"] != float64(1) || stats["ready"] != float64(0) {
		t.Fatalf("stats = %v, want delayed 1 ready 0", stats)
	}
	if status, _ := do(t, srv, "POST", "/queues/orders/receive", map[string]any{}); status != http.StatusNoContent {
		t.Fatalf("receive status %d, want 204 while the message is delayed", status)
	}
}

func TestBodyIsReturnedByteExact(t *testing.T) {
	srv := newTestServer(t)
	createQueue(t, srv, "orders", "fifo")

	// Large integers and nested structures must survive the round trip unchanged.
	payload := json.RawMessage(`{"big":10000000000000001,"nested":{"list":[1,"two",null,true]}}`)
	res, err := http.Post(srv.URL+"/queues/orders/messages", "application/json",
		strings.NewReader(fmt.Sprintf(`{"body":%s}`, payload)))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	got, err := http.Post(srv.URL+"/queues/orders/receive", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	var delivery struct {
		Body json.RawMessage `json:"body"`
	}
	if err := json.NewDecoder(got.Body).Decode(&delivery); err != nil {
		t.Fatal(err)
	}
	if string(delivery.Body) != string(payload) {
		t.Fatalf("body came back as %s, want %s", delivery.Body, payload)
	}
}
