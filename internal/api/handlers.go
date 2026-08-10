package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"queuemaxxing/internal/queue"
	"queuemaxxing/internal/storage"
)

// The API speaks whole seconds; the engine speaks time.Duration. This is the only place the
// two are converted, which is also what lets the engine be tested on millisecond timers.
const (
	defaultVisibility = 30 * time.Second
	maxVisibility     = 12 * time.Hour
	maxWait           = 60 * time.Second
	maxDelay          = 7 * 24 * time.Hour
)

type handlers struct {
	queues *queue.Manager
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		json.NewEncoder(w).Encode(v)
	}
}

func writeError(w http.ResponseWriter, status int, format string, args ...any) {
	writeJSON(w, status, errorResponse{Error: fmt.Sprintf(format, args...)})
}

// writeEngineError maps engine errors onto status codes in one place, so handlers stay free
// of queue logic.
func writeEngineError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrQueueNotFound), errors.Is(err, queue.ErrQueueDeleted):
		writeError(w, http.StatusNotFound, "%v", err)
	case errors.Is(err, queue.ErrReceiptNotFound):
		writeError(w, http.StatusNotFound, "%v", err)
	case errors.Is(err, queue.ErrQueueExists):
		writeError(w, http.StatusConflict, "%v", err)
	case errors.Is(err, queue.ErrEmptyBody):
		writeError(w, http.StatusBadRequest, "%v", err)
	case errors.Is(err, storage.ErrLogBroken):
		// The log could not be rolled back after a failed write, so the queue refuses
		// further writes rather than extending a log it cannot vouch for.
		writeError(w, http.StatusServiceUnavailable, "queue is not accepting writes: %v", err)
	default:
		writeError(w, http.StatusInternalServerError, "%v", err)
	}
}

// decodeBody reads an optional JSON object. An absent body means "all defaults", which keeps
// receive and ack usable without sending anything.
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, storage.MaxRecordSize)
	err := json.NewDecoder(r.Body).Decode(dst)
	switch {
	case err == nil, errors.Is(err, io.EOF):
		return true
	case errors.As(err, new(*http.MaxBytesError)):
		writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds %d bytes", storage.MaxRecordSize)
	default:
		writeError(w, http.StatusBadRequest, "invalid JSON: %v", err)
	}
	return false
}

func seconds(v *int, def, max time.Duration, field string) (time.Duration, error) {
	if v == nil {
		return def, nil
	}
	if *v < 0 {
		return 0, fmt.Errorf("%s must not be negative", field)
	}
	d := time.Duration(*v) * time.Second
	if d > max {
		return 0, fmt.Errorf("%s must not exceed %d", field, int(max.Seconds()))
	}
	return d, nil
}

type queueView struct {
	Name      string      `json:"name"`
	Ordering  string      `json:"ordering"`
	CreatedAt time.Time   `json:"created_at"`
	Stats     queue.Stats `json:"stats"`
}

func viewOf(q *queue.Queue) queueView {
	cfg := q.Config()
	return queueView{
		Name:      cfg.Name,
		Ordering:  string(cfg.Ordering),
		CreatedAt: cfg.CreatedAt,
		Stats:     q.Stats(),
	}
}

func (h *handlers) createQueue(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string `json:"name"`
		Ordering string `json:"ordering"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if err := queue.ValidateName(req.Name); err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	ordering, err := queue.ParseOrdering(req.Ordering)
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	q, err := h.queues.Create(req.Name, ordering)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, viewOf(q))
}

func (h *handlers) listQueues(w http.ResponseWriter, r *http.Request) {
	all := h.queues.List()
	views := make([]queueView, 0, len(all))
	for _, q := range all {
		views = append(views, viewOf(q))
	}
	writeJSON(w, http.StatusOK, views)
}

func (h *handlers) getQueue(w http.ResponseWriter, r *http.Request) {
	q, err := h.queues.Get(r.PathValue("name"))
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewOf(q))
}

func (h *handlers) deleteQueue(w http.ResponseWriter, r *http.Request) {
	if err := h.queues.Delete(r.PathValue("name")); err != nil {
		writeEngineError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) stats(w http.ResponseWriter, r *http.Request) {
	q, err := h.queues.Get(r.PathValue("name"))
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, q.Stats())
}

func (h *handlers) sendMessage(w http.ResponseWriter, r *http.Request) {
	q, err := h.queues.Get(r.PathValue("name"))
	if err != nil {
		writeEngineError(w, err)
		return
	}
	var req struct {
		Body         json.RawMessage `json:"body"`
		Priority     *int            `json:"priority"`
		DelaySeconds *int            `json:"delay_seconds"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if len(req.Body) == 0 {
		writeError(w, http.StatusBadRequest, "body is required")
		return
	}
	delay, err := seconds(req.DelaySeconds, 0, maxDelay, "delay_seconds")
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	priority := 0
	if req.Priority != nil {
		priority = *req.Priority
	}

	id, err := q.Enqueue(req.Body, priority, delay)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"message_id": id})
}

type deliveryView struct {
	MessageID          string          `json:"message_id"`
	Body               json.RawMessage `json:"body"`
	Priority           int             `json:"priority"`
	Sequence           uint64          `json:"sequence"`
	ReceiveCount       int             `json:"receive_count"`
	CreatedAt          time.Time       `json:"created_at"`
	AvailableAt        time.Time       `json:"available_at"`
	Receipt            string          `json:"receipt"`
	VisibilityDeadline time.Time       `json:"visibility_deadline"`
}

func (h *handlers) receive(w http.ResponseWriter, r *http.Request) {
	q, err := h.queues.Get(r.PathValue("name"))
	if err != nil {
		writeEngineError(w, err)
		return
	}
	var req struct {
		VisibilityTimeoutSeconds *int `json:"visibility_timeout_seconds"`
		WaitSeconds              *int `json:"wait_seconds"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	visibility, err := seconds(req.VisibilityTimeoutSeconds, defaultVisibility, maxVisibility, "visibility_timeout_seconds")
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	wait, err := seconds(req.WaitSeconds, 0, maxWait, "wait_seconds")
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}

	d, ok, err := q.Receive(r.Context(), visibility, wait)
	if err != nil {
		// A cancelled request means the client disconnected mid long poll; there is
		// nobody left to answer.
		if r.Context().Err() != nil {
			return
		}
		writeEngineError(w, err)
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, deliveryView{
		MessageID:          d.ID,
		Body:               d.Body,
		Priority:           d.Priority,
		Sequence:           d.Seq,
		ReceiveCount:       d.ReceiveCount,
		CreatedAt:          d.CreatedAt,
		AvailableAt:        d.AvailableAt,
		Receipt:            d.Receipt,
		VisibilityDeadline: d.Deadline,
	})
}

func (h *handlers) ack(w http.ResponseWriter, r *http.Request) {
	q, err := h.queues.Get(r.PathValue("name"))
	if err != nil {
		writeEngineError(w, err)
		return
	}
	var req struct {
		Receipt string `json:"receipt"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Receipt == "" {
		writeError(w, http.StatusBadRequest, "receipt is required")
		return
	}
	if err := q.Ack(req.Receipt); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "acked"})
}

func (h *handlers) nack(w http.ResponseWriter, r *http.Request) {
	q, err := h.queues.Get(r.PathValue("name"))
	if err != nil {
		writeEngineError(w, err)
		return
	}
	var req struct {
		Receipt      string `json:"receipt"`
		DelaySeconds *int   `json:"delay_seconds"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Receipt == "" {
		writeError(w, http.StatusBadRequest, "receipt is required")
		return
	}
	delay, err := seconds(req.DelaySeconds, 0, maxDelay, "delay_seconds")
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	if err := q.Nack(req.Receipt, delay); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "nacked"})
}
