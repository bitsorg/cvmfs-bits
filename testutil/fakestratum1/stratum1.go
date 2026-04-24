package fakestratum1

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

type Stratum1 struct {
	Name        string
	PutLatency  time.Duration
	PutFailRate float64
	Partitioned bool

	received map[string][]byte
	mu       sync.RWMutex
	server   *httptest.Server
	obs      *observe.Provider
}

func New(name string, obs *observe.Provider) *Stratum1 {
	s := &Stratum1{
		Name:     name,
		received: make(map[string][]byte),
		obs:      obs,
	}

	mux := http.NewServeMux()
	// Batch endpoint must be registered before the single-object wildcard so
	// the more-specific path wins.
	mux.HandleFunc("/cvmfs-receiver/objects/batch", s.handleBatch)
	mux.HandleFunc("/cvmfs-receiver/objects/", s.handleObject)

	s.server = httptest.NewServer(mux)
	return s
}

func (s *Stratum1) URL() string {
	return s.server.URL
}

func (s *Stratum1) Close() {
	s.server.Close()
}

func (s *Stratum1) ReceivedCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.received)
}

func (s *Stratum1) Has(hash string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.received[hash]
	return ok
}

// handleBatch accepts POST /cvmfs-receiver/objects/batch (multipart/form-data).
// Each part has name "object" and filename set to the object hash.
// Returns {"received": ["hash1", "hash2", ...]} on success.
func (s *Stratum1) handleBatch(w http.ResponseWriter, r *http.Request) {
	_, span := s.obs.Tracer.Start(r.Context(), "fakestratum1.batch")
	defer span.End()

	if s.Partitioned {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "partitioned")
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if s.PutLatency > 0 {
		time.Sleep(s.PutLatency)
	}

	if err := r.ParseMultipartForm(512 << 20); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid multipart: %v", err)
		return
	}

	var received []string
	for _, fhs := range r.MultipartForm.File["object"] {
		f, err := fhs.Open()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		data, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		hash := fhs.Filename
		s.mu.Lock()
		s.received[hash] = data
		s.mu.Unlock()
		received = append(received, hash)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string][]string{"received": received})
}

func (s *Stratum1) handleObject(w http.ResponseWriter, r *http.Request) {
	_, span := s.obs.Tracer.Start(r.Context(), "fakestratum1.object")
	defer span.End()

	if s.Partitioned {
		span.RecordError(fmt.Errorf("partitioned"))
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "partitioned")
		return
	}

	if r.Method != "PUT" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if s.PutLatency > 0 {
		time.Sleep(s.PutLatency)
	}

	// Extract hash from URL
	parts := strings.Split(r.URL.Path, "/")
	hash := parts[len(parts)-1]

	// Read body
	data, err := io.ReadAll(r.Body)
	if err != nil {
		span.RecordError(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.received[hash] = data
	s.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
}
