// manager.go — queue-driven, per-endpoint distribution manager.
//
// Manager maintains one pool of worker goroutines per Stratum 1 endpoint.
// Each pool draws work from a bounded in-memory channel backed by a persistent
// spool directory so that pending transfers survive service restarts.
//
// Concurrency contract:
//   - Enqueue is safe to call from multiple goroutines concurrently.
//   - ResultFunc callbacks are called from an arbitrary goroutine; callers must
//     not assume the callback runs on the same goroutine as Enqueue.
//   - Drain blocks until all workers finish their current attempt (or ctx expires).
package distribute

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cvmfs.io/prepub/internal/cas"
)

// WorkItem is a distribution task: push the listed CAS objects to one or all
// configured Stratum 1 endpoints as part of a single publish job.
type WorkItem struct {
	// JobID is the unique publish-job identifier used as the idempotency key.
	JobID string `json:"job_id"`
	// Hashes are the CAS object hashes to push to the endpoint.
	Hashes []string `json:"hashes"`
	// TotalBytes is the total compressed size (disk-space pre-check hint for
	// the receiver; zero means "unknown" and disables the check).
	TotalBytes int64 `json:"total_bytes,omitempty"`
	// Repo is the CVMFS repository name (required by the MQTT announce path).
	Repo string `json:"repo"`
	// EnqueuedAt is when this item entered the distribution queue.
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// ResultFunc is called exactly once per WorkItem, after every endpoint worker
// has reported success or exhausted its retry budget.
// confirmed is the count of endpoints that received all objects; total is the
// total endpoint count.  The callback fires from a worker goroutine; callers
// must not block it.
type ResultFunc func(jobID string, confirmed, total int)

// Manager owns one worker pool per configured Stratum 1 endpoint.  Workers
// pull items from their shared queue and push objects with per-attempt timeouts
// and exponential-backoff retries.  Enqueue is non-blocking.
type Manager struct {
	cfg  Config
	cas  cas.Backend
	obs  interface {
		Info(msg string, args ...any)
		Warn(msg string, args ...any)
		Debug(msg string, args ...any)
		Error(msg string, args ...any)
	}

	workers []*endpointWorker
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	jobsMu sync.Mutex
	jobs   map[string]*jobEntry
}

// jobEntry tracks completion state across all endpoint workers for one job.
type jobEntry struct {
	total     int // equals number of endpoint workers
	done      int // workers that have reported
	confirmed int // workers that reported success
	fn        ResultFunc
}

// NewManager creates a Manager with one worker pool per configured endpoint.
// Call Start before calling Enqueue.
func NewManager(cfg Config, casBackend cas.Backend) *Manager {
	m := &Manager{
		cfg:  cfg,
		cas:  casBackend,
		obs:  cfg.Obs.Logger,
		jobs: make(map[string]*jobEntry),
	}
	for _, ep := range cfg.Endpoints {
		ep := ep
		var spoolDir string
		if cfg.QueueSpoolDir != "" {
			spoolDir = endpointSpoolDir(cfg.QueueSpoolDir, ep)
		}
		w := &endpointWorker{
			endpoint: ep,
			cfg:      cfg,
			cas:      casBackend,
			queue:    make(chan WorkItem, cfg.queueDepth()),
			spoolDir: spoolDir,
			manager:  m,
		}
		m.workers = append(m.workers, w)
	}
	return m
}

// Start launches all worker goroutines and re-queues any items persisted from
// a previous run.  Calling Start twice is a programming error.
func (m *Manager) Start(parentCtx context.Context) {
	ctx, cancel := context.WithCancel(parentCtx)
	m.cancel = cancel

	for _, w := range m.workers {
		w := w

		// Re-queue persisted items from the previous run.  Done before spawning
		// goroutines so the channel is pre-filled without concurrent writers.
		if w.spoolDir != "" {
			items, err := loadSpoolItems(w.spoolDir)
			if err != nil {
				m.obs.Warn("dist-manager: cannot load persisted queue items",
					"endpoint", w.endpoint, "error", err)
			} else if len(items) > 0 {
				m.obs.Info("dist-manager: re-queuing persisted items from previous run",
					"endpoint", w.endpoint, "count", len(items))
				for _, item := range items {
					// Register a placeholder job entry if none exists (the original
					// job goroutine is long gone; no ResultFunc will fire).
					m.jobsMu.Lock()
					if _, ok := m.jobs[item.JobID]; !ok {
						m.jobs[item.JobID] = &jobEntry{total: len(m.workers)}
					}
					m.jobsMu.Unlock()
					select {
					case w.queue <- item:
					default:
						m.obs.Warn("dist-manager: queue full re-queuing recovered item",
							"endpoint", w.endpoint, "job_id", item.JobID)
					}
				}
			}
		}

		// Start WorkerConcurrency goroutines that share this worker's queue.
		// Multiple goroutines drawing from the same channel give parallelism
		// without adding queue complexity — each item is still processed exactly once.
		conc := m.cfg.workerConcurrency()
		for i := 0; i < conc; i++ {
			m.wg.Add(1)
			go func(w *endpointWorker) {
				defer m.wg.Done()
				w.run(ctx)
			}(w)
		}
	}
	m.obs.Info("dist-manager: started",
		"endpoints", len(m.workers),
		"concurrency_per_endpoint", m.cfg.workerConcurrency(),
		"attempt_timeout", m.cfg.workerAttemptTimeout(),
		"initial_backoff", m.cfg.workerInitialBackoff(),
		"max_backoff", m.cfg.workerMaxBackoff(),
		"queue_depth", m.cfg.queueDepth(),
	)
}

// Enqueue adds item to every endpoint worker's queue and registers fn as the
// result callback.  fn is called once all endpoints have reported their
// outcome.  Enqueue is non-blocking: if an endpoint's in-memory queue is full
// the item is silently dropped for that endpoint (counted as an immediate
// failure) so fn can still fire.
func (m *Manager) Enqueue(item WorkItem, fn ResultFunc) {
	if len(m.workers) == 0 {
		if fn != nil {
			fn(item.JobID, 0, 0)
		}
		return
	}

	m.jobsMu.Lock()
	m.jobs[item.JobID] = &jobEntry{total: len(m.workers), fn: fn}
	m.jobsMu.Unlock()

	for _, w := range m.workers {
		w := w
		// Persist to spool before sending to channel: if the process crashes
		// between the write and the channel send, the item is still on disk.
		if w.spoolDir != "" {
			if err := writeSpoolItem(w.spoolDir, item); err != nil {
				m.obs.Warn("dist-manager: cannot write spool item (item not persisted)",
					"endpoint", w.endpoint, "job_id", item.JobID, "error", err)
			}
		}
		select {
		case w.queue <- item:
		default:
			m.obs.Warn("dist-manager: per-endpoint queue full — item dropped for this endpoint",
				"endpoint", w.endpoint, "job_id", item.JobID)
			m.reportEndpoint(item.JobID, w.endpoint, false)
		}
	}
}

// reportEndpoint is called by a worker when it finishes (success or failure).
// When all workers for a job have reported, the registered ResultFunc fires.
func (m *Manager) reportEndpoint(jobID, endpoint string, success bool) {
	m.jobsMu.Lock()
	st, ok := m.jobs[jobID]
	if !ok {
		m.jobsMu.Unlock()
		return
	}
	if success {
		st.confirmed++
	}
	st.done++
	allDone := st.done == st.total
	var fn ResultFunc
	var confirmed, total int
	if allDone {
		fn = st.fn
		confirmed = st.confirmed
		total = st.total
		delete(m.jobs, jobID)
	}
	m.jobsMu.Unlock()

	if allDone && fn != nil {
		fn(jobID, confirmed, total)
	}
}

// Drain signals all workers to stop retrying and waits for in-flight transfers
// to finish.  Items that were being retried (backoff wait) are abandoned; they
// remain in QueueSpoolDir and will be re-processed on the next Start call.
// After Drain returns no further ResultFunc callbacks will fire.
func (m *Manager) Drain(ctx context.Context) {
	if m.cancel != nil {
		m.cancel() // unblocks all backoff selects and stops run() loops
	}
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		m.obs.Info("dist-manager: all workers drained cleanly")
	case <-ctx.Done():
		m.obs.Warn("dist-manager: drain timed out — pending spool items will be retried on next start")
	}
}

// ── Persistent spool helpers ──────────────────────────────────────────────────

// endpointSpoolDir derives a stable, filesystem-safe directory name for the
// given endpoint URL by hashing it.  The first 16 hex chars of SHA-256 provide
// 64 bits of collision resistance — more than enough for a handful of endpoints.
func endpointSpoolDir(base, endpoint string) string {
	h := sha256.Sum256([]byte(endpoint))
	// Prefix with a sanitised short form of the endpoint for human readability.
	short := endpoint
	for _, pfx := range []string{"https://", "http://"} {
		short = strings.TrimPrefix(short, pfx)
	}
	short = strings.NewReplacer(":", "_", "/", "_", ".", "_").Replace(short)
	if len(short) > 32 {
		short = short[:32]
	}
	return filepath.Join(base, short+"-"+hex.EncodeToString(h[:4]))
}

// writeSpoolItem atomically writes item as JSON to {spoolDir}/{job-id}.json.
func writeSpoolItem(dir string, item WorkItem) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, item.JobID+".json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// removeSpoolItem deletes the persistent queue entry for jobID.  Errors are
// silently ignored (a stale file causes an idempotent re-push on next start).
func removeSpoolItem(dir, jobID string) {
	if dir == "" {
		return
	}
	_ = os.Remove(filepath.Join(dir, jobID+".json"))
}

// loadSpoolItems reads all pending work items from dir, silently skipping
// corrupt or unreadable files.  Returns nil, nil if dir does not exist.
func loadSpoolItems(dir string) ([]WorkItem, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var items []WorkItem
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") ||
			strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(dir, e.Name()))
		if readErr != nil {
			continue // best-effort
		}
		var item WorkItem
		if jsonErr := json.Unmarshal(data, &item); jsonErr != nil {
			continue // corrupt file — skip; will be cleaned up on next success
		}
		items = append(items, item)
	}
	return items, nil
}
