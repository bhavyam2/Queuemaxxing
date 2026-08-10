package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	queuemaxxing "queuemaxxing"
	"queuemaxxing/internal/api"
	"queuemaxxing/internal/queue"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dataDir := flag.String("data-dir", "data", "directory holding one append-only log per queue")
	compactThreshold := flag.Int("compact-threshold", 10000, "log record count that triggers compaction")
	flag.Parse()

	if *compactThreshold < 1 {
		log.Fatalf("compact-threshold must be at least 1, got %d", *compactThreshold)
	}

	// Recovery happens before the listener opens, so the server never serves a queue whose
	// log it has not fully replayed.
	queues, err := queue.NewManager(*dataDir, *compactThreshold)
	if err != nil {
		log.Fatalf("recover data directory %s: %v", *dataDir, err)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.NewRouter(queues, queuemaxxing.WebFS()),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() {
		log.Printf("listening on %s, data dir %s, compact threshold %d", *addr, *dataDir, *compactThreshold)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			queues.Close()
			log.Fatalf("server: %v", err)
		}
	case <-ctx.Done():
		stop()
		log.Print("shutting down")
	}

	// Shutdown drains in-flight requests, including long polls, which observe their request
	// context and return promptly rather than holding the window open.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	if err := queues.Close(); err != nil {
		log.Printf("close queues: %v", err)
	}
}
