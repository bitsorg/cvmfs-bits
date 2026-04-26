package fakegateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

type Payload struct {
	Token        string   `json:"token"`
	CatalogHash  string   `json:"catalog_hash"`
	ObjectHashes []string `json:"object_hashes"`
}

type fakeLease struct {
	token     string
	path      string
	repo      string
	expiresAt time.Time
}

type Gateway struct {
	AcquireLatency  time.Duration
	AcquireFailRate float64
	SubmitFailRate  float64

	leases   map[string]*fakeLease
	payloads []Payload
	mu       sync.Mutex

	server *httptest.Server
	obs    *observe.Provider
}

func New(obs *observe.Provider) *Gateway {
	g := &Gateway{
		leases: make(map[string]*fakeLease),
		obs:    obs,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/leases/", g.handleLease)
	mux.HandleFunc("/api/v1/payloads", g.handlePayload)

	g.server = httptest.NewServer(mux)
	return g
}

func (g *Gateway) URL() string {
	return g.server.URL
}

func (g *Gateway) Close() {
	g.server.Close()
}

func (g *Gateway) LeaseCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.leases)
}

func (g *Gateway) SubmittedPayloads() []Payload {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]Payload{}, g.payloads...)
}

func (g *Gateway) handleLease(w http.ResponseWriter, r *http.Request) {
	_, span := g.obs.Tracer.Start(r.Context(), "fakegateway.lease")
	defer span.End()

	path := r.URL.Path[len("/api/v1/leases/"):]

	if r.Method == "POST" {
		// Acquire lease
		if g.AcquireLatency > 0 {
			time.Sleep(g.AcquireLatency)
		}

		if g.randomFail(g.AcquireFailRate) {
			span.RecordError(fmt.Errorf("simulated failure"))
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"error":"simulated"}`)
			return
		}

		var req struct {
			Repo string `json:"repo"`
		}
		json.NewDecoder(r.Body).Decode(&req)

		token := fmt.Sprintf("lease-%d", time.Now().UnixNano())
		lease := &fakeLease{
			token:     token,
			path:      path,
			repo:      req.Repo,
			expiresAt: time.Now().Add(5 * time.Minute),
		}

		g.mu.Lock()
		g.leases[token] = lease
		g.mu.Unlock()

		// Use the same response format as the real cvmfs_gateway:
		//   {"status":"ok","session_token":"<tok>","max_lease_time":<seconds>}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":         "ok",
			"session_token":  token,
			"max_lease_time": int(5 * time.Minute / time.Second), // 300
		})

	} else if r.Method == "PUT" {
		// Renew lease
		g.mu.Lock()
		if lease, ok := g.leases[path]; ok {
			lease.expiresAt = time.Now().Add(5 * time.Minute)
		}
		g.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "renewed"})

	} else if r.Method == "DELETE" {
		// Release lease
		g.mu.Lock()
		delete(g.leases, path)
		g.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "released"})
	}
}

func (g *Gateway) handlePayload(w http.ResponseWriter, r *http.Request) {
	_, span := g.obs.Tracer.Start(r.Context(), "fakegateway.payload")
	defer span.End()

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if g.randomFail(g.SubmitFailRate) {
		span.RecordError(fmt.Errorf("simulated failure"))
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error":"simulated"}`)
		return
	}

	var payload Payload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	g.mu.Lock()
	g.payloads = append(g.payloads, payload)
	g.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

func (g *Gateway) randomFail(failRate float64) bool {
	// Simple deterministic fail for now
	return false
}
