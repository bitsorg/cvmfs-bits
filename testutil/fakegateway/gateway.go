package fakegateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// Payload records a single object received via the binary gateway payload protocol.
// Each call to POST /api/v1/payloads submits exactly one object.
type Payload struct {
	// SessionToken is the lease token that authorised this upload.
	SessionToken string `json:"session_token"`
	// PayloadDigest is the CAS hash of the uploaded object.
	PayloadDigest string `json:"payload_digest"`
	// ObjectSize is the byte length of the compressed object body.
	ObjectSize int `json:"-"`
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
	mux.HandleFunc("/api/v1/repos", g.handleRepos)
	// Register both the exact path (for POST /api/v1/leases acquire) and the
	// subtree pattern (for PUT/DELETE /api/v1/leases/<token>).  Without the
	// exact registration, Go's ServeMux would 301-redirect POST /api/v1/leases
	// to /api/v1/leases/ — a redirect the client won't follow for non-GET.
	mux.HandleFunc("/api/v1/leases", g.handleLease)
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

// handleRepos serves GET /api/v1/repos — used by Client.Probe for the startup
// health check.  It returns the list of configured repositories.
func (g *Gateway) handleRepos(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"repos":  []string{},
	})
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

	// The real gateway protocol: Message-Size header + binary body.
	// Body = <Message-Size bytes of JSON msg> + <compressed object bytes>.
	msgSizeStr := r.Header.Get("Message-Size")
	if msgSizeStr == "" {
		http.Error(w, "missing message-size header", http.StatusBadRequest)
		return
	}
	msgSize, err := strconv.Atoi(msgSizeStr)
	if err != nil || msgSize < 0 {
		http.Error(w, "invalid message-size header", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil || len(body) < msgSize {
		http.Error(w, "body shorter than message-size", http.StatusBadRequest)
		return
	}

	// Parse the JSON message header.
	var msg struct {
		SessionToken  string `json:"session_token"`
		PayloadDigest string `json:"payload_digest"`
	}
	if err := json.Unmarshal(body[:msgSize], &msg); err != nil {
		http.Error(w, "invalid JSON message header", http.StatusBadRequest)
		return
	}

	payload := Payload{
		SessionToken:  msg.SessionToken,
		PayloadDigest: msg.PayloadDigest,
		ObjectSize:    len(body) - msgSize,
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
