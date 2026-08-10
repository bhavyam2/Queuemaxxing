package api

import (
	"io/fs"
	"net/http"

	"queuemaxxing/internal/queue"
)

// NewRouter wires the HTTP surface. Go 1.22 method-and-wildcard patterns cover the routing,
// so there is no third-party router and no hand-rolled path parsing.
func NewRouter(queues *queue.Manager, web fs.FS) http.Handler {
	h := &handlers{queues: queues}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /queues", h.createQueue)
	mux.HandleFunc("GET /queues", h.listQueues)
	mux.HandleFunc("GET /queues/{name}", h.getQueue)
	mux.HandleFunc("DELETE /queues/{name}", h.deleteQueue)
	mux.HandleFunc("POST /queues/{name}/messages", h.sendMessage)
	mux.HandleFunc("POST /queues/{name}/receive", h.receive)
	mux.HandleFunc("POST /queues/{name}/ack", h.ack)
	mux.HandleFunc("POST /queues/{name}/nack", h.nack)
	mux.HandleFunc("GET /queues/{name}/stats", h.stats)

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	if web != nil {
		mux.Handle("GET /", http.FileServer(http.FS(web)))
	}
	return mux
}
