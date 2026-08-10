package queue

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"queuemaxxing/internal/storage"
)

var (
	ErrQueueExists   = errors.New("queue already exists")
	ErrQueueNotFound = errors.New("queue not found")
)

// Manager is the queue registry. Its lock guards only the name-to-queue map.
//
// Lock ordering: Manager.mu is always taken before a Queue.mu, never the reverse. No path
// holds a queue lock and then reaches back into the manager.
type Manager struct {
	mu               sync.RWMutex
	dir              string
	compactThreshold int
	queues           map[string]*Queue
}

// NewManager prepares the data directory and recovers every queue it finds. A log damaged
// anywhere but its tail fails startup: those records were acknowledged to clients, and
// starting up while quietly missing them would be worse than refusing to start.
func NewManager(dir string, compactThreshold int) (*Manager, error) {
	if err := storage.EnsureDir(dir); err != nil {
		return nil, fmt.Errorf("prepare data directory: %w", err)
	}
	if err := storage.CleanTemps(dir); err != nil {
		return nil, fmt.Errorf("clean compaction temporaries: %w", err)
	}

	names, err := storage.ListLogs(dir)
	if err != nil {
		return nil, err
	}

	m := &Manager{dir: dir, compactThreshold: compactThreshold, queues: make(map[string]*Queue)}
	for _, name := range names {
		if err := ValidateName(name); err != nil {
			log.Printf("queuemaxxing: ignoring %s: %v", name, err)
			continue
		}
		q, err := openQueue(dir, name, compactThreshold)
		if err != nil {
			// A log whose header never landed belongs to a queue whose creation was
			// interrupted before any client was told it existed.
			if errors.Is(err, storage.ErrIncompleteLog) {
				log.Printf("queuemaxxing: discarding incomplete log for %s: %v", name, err)
				if rerr := storage.Remove(dir, name); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
					return nil, rerr
				}
				continue
			}
			m.Close()
			return nil, err
		}
		m.queues[name] = q
	}
	return m, nil
}

func (m *Manager) Create(name string, ordering Ordering) (*Queue, error) {
	if err := ValidateName(name); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.queues[name]; exists {
		return nil, ErrQueueExists
	}
	q, err := createQueue(m.dir, Config{Name: name, Ordering: ordering, CreatedAt: time.Now()}, m.compactThreshold)
	if err != nil {
		if errors.Is(err, storage.ErrExists) {
			return nil, ErrQueueExists
		}
		return nil, err
	}
	m.queues[name] = q
	return q, nil
}

func (m *Manager) Get(name string) (*Queue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return q, nil
}

func (m *Manager) List() []*Queue {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// Delete closes the queue and removes its log. The registry lock is held throughout so the
// name cannot be reused while the old file is still being torn down; closing only waits for
// the scheduler goroutine to observe a channel, so the pause is short.
func (m *Manager) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.queues[name]
	if !ok {
		return ErrQueueNotFound
	}
	delete(m.queues, name)
	if err := q.Close(); err != nil {
		return err
	}
	return storage.Remove(m.dir, name)
}

// Close shuts every queue down cleanly, compacting each on the way out.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for name, q := range m.queues {
		if err := q.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close queue %s: %w", name, err)
		}
		delete(m.queues, name)
	}
	return firstErr
}
