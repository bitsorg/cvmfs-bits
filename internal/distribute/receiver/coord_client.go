package receiver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

const (
	// coordHeartbeatInterval is how often the receiver sends a heartbeat to the
	// coordination service.
	coordHeartbeatInterval = 30 * time.Second

	// coordHTTPTimeout is the per-request deadline for all coordination service
	// calls.  Short enough to avoid blocking shutdown, long enough to survive a
	// momentary network hiccup.
	coordHTTPTimeout = 10 * time.Second

	coordRegisterPath   = "/api/v1/nodes"
	coordHeartbeatFmt   = "/api/v1/nodes/%s/heartbeat"
	coordDeregisterFmt  = "/api/v1/nodes/%s"
)

// coordRegisterRequest is the JSON body sent on initial registration.
type coordRegisterRequest struct {
	NodeID     string   `json:"node_id"`
	Repos      []string `json:"repos"`
	ControlURL string   `json:"control_url"` // HTTPS control channel of this receiver
	DataURL    string   `json:"data_url"`    // plain-HTTP data channel
}

// coordHeartbeatRequest is the JSON body sent every heartbeat interval.
// It carries lightweight health signals so the coordination service can
// route pre-warm traffic to nodes that are ready and not overloaded.
type coordHeartbeatRequest struct {
	BloomSize uint64 `json:"bloom_size"` // approximate objects in inventory filter
	Ready     bool   `json:"ready"`      // true once the initial CAS walk has completed
}

// CoordClient manages registration and periodic heartbeats with the HepCDN
// coordination service.
//
// When coordURL is empty, Start and Stop are no-ops and all methods return
// immediately, so callers do not need to nil-guard.
//
// The client performs a best-effort deregister on Stop; transient failures
// are logged but do not block shutdown.
//
// If the initial registration in Start fails, the heartbeat goroutine
// re-attempts registration on every tick until it succeeds, so a transient
// coordination-service outage at boot does not permanently disable the node.
type CoordClient struct {
	nodeID     string
	repos      []string
	coordURL   string
	token      string
	controlURL string // this receiver's HTTPS control endpoint
	dataURL    string // this receiver's plain-HTTP data endpoint
	inv        *inventory
	obs        *observe.Provider
	httpClient *http.Client
	stop       chan struct{}
	once       sync.Once
	registered atomic.Bool // true once registration has succeeded
	// newTicker creates the heartbeat ticker.  Overridable in tests to use a
	// short interval without modifying the production constant.
	newTicker  func() *time.Ticker
}

// newCoordClient returns a configured CoordClient, or nil if coordURL is empty.
// nodeID defaults to the OS hostname when empty.
// controlURL is the HTTPS control endpoint of this receiver (e.g. "https://host:9100").
// dataURL is the plain-HTTP data endpoint (e.g. "http://host:9101").
func newCoordClient(coordURL, token, nodeID string, repos []string, controlURL, dataURL string, inv *inventory, obs *observe.Provider) *CoordClient {
	if coordURL == "" {
		return nil
	}
	// Normalise: strip any trailing slash so path concatenations produce exactly
	// one slash (e.g. "https://coord.example.com/" → "https://coord.example.com").
	coordURL = strings.TrimRight(coordURL, "/")
	if len(repos) == 0 {
		// A node registered with no repos will never be routed any publish jobs;
		// this is almost always a configuration error rather than intentional.
		obs.Logger.Warn("coord: CoordURL is set but Repos is empty — node will not be routed to any repository")
	}
	if nodeID == "" {
		if h, err := os.Hostname(); err == nil {
			nodeID = h
		} else {
			nodeID = "unknown"
		}
	}
	return &CoordClient{
		nodeID:     nodeID,
		repos:      repos,
		coordURL:   coordURL,
		token:      token,
		controlURL: controlURL,
		dataURL:    dataURL,
		inv:        inv,
		obs:        obs,
		httpClient: &http.Client{
			Timeout: coordHTTPTimeout,
		},
		stop:      make(chan struct{}),
		newTicker: func() *time.Ticker { return time.NewTicker(coordHeartbeatInterval) },
	}
}

// Start registers the node and launches the background heartbeat loop.
// Safe to call on a nil CoordClient (disabled mode).
// If the initial registration fails (e.g. coordination service is temporarily
// unavailable at boot), the heartbeat goroutine retries on every tick until
// registration succeeds.
func (c *CoordClient) Start() {
	if c == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), coordHTTPTimeout)
	defer cancel()
	if err := c.register(ctx); err != nil {
		c.obs.Logger.Error("coord: initial registration failed — will retry in heartbeat loop",
			"error", err)
		c.obs.Metrics.ReceiverHeartbeatErrors.Inc()
		// registered remains false; run() will retry.
	}
	go c.run()
}

// Stop deregisters the node and halts the heartbeat goroutine.
// Blocks briefly (up to coordHTTPTimeout) on the deregister call.
// Safe to call multiple times and on a nil CoordClient.
func (c *CoordClient) Stop() {
	if c == nil {
		return
	}
	c.once.Do(func() {
		close(c.stop)
		ctx, cancel := context.WithTimeout(context.Background(), coordHTTPTimeout)
		defer cancel()
		if err := c.deregister(ctx); err != nil {
			c.obs.Logger.Warn("coord: deregister failed (best-effort)", "error", err)
		}
	})
}

// run is the background heartbeat goroutine.  It exits when stop is closed.
// On each tick it first ensures the node is registered (retrying if the
// initial registration failed), then sends a heartbeat.
func (c *CoordClient) run() {
	ticker := c.newTicker()
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			// Wrap the tick body in an IIFE so that defer cancel() is
			// guaranteed to fire on every code path — including panics —
			// rather than relying on two manual cancel() call-sites that
			// could be missed during future refactors.
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), coordHTTPTimeout)
				defer cancel()
				// If registration has not yet succeeded, retry it before sending a
				// heartbeat.  This handles transient coordination-service outages at
				// receiver boot without requiring a restart.
				if !c.registered.Load() {
					if err := c.register(ctx); err != nil {
						c.obs.Logger.Warn("coord: registration retry failed", "error", err)
						c.obs.Metrics.ReceiverHeartbeatErrors.Inc()
						return // skip heartbeat until registered
					}
				}
				if err := c.heartbeat(ctx); err != nil {
					c.obs.Logger.Warn("coord: heartbeat failed", "error", err)
					c.obs.Metrics.ReceiverHeartbeatErrors.Inc()
				}
			}()
		}
	}
}

// register sends POST /api/v1/nodes to the coordination service.
// On success it sets c.registered so the heartbeat loop knows registration
// has already been performed.
func (c *CoordClient) register(ctx context.Context) error {
	body, err := json.Marshal(coordRegisterRequest{
		NodeID:     c.nodeID,
		Repos:      c.repos,
		ControlURL: c.controlURL,
		DataURL:    c.dataURL,
	})
	if err != nil {
		return fmt.Errorf("coord: marshalling register request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.coordURL+coordRegisterPath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("coord: building register request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("coord: POST register: %w", err)
	}
	// Drain before Close so the underlying TCP connection can be returned to
	// the pool and reused on the next heartbeat, avoiding a new TLS handshake
	// per registration/heartbeat cycle across many receiver nodes.
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)) //nolint:errcheck
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("coord: register returned HTTP %d", resp.StatusCode)
	}

	c.registered.Store(true)
	c.obs.Logger.Info("coord: node registered",
		"node_id", c.nodeID, "repos", c.repos,
		"control_url", c.controlURL, "data_url", c.dataURL)
	return nil
}

// heartbeat sends PUT /api/v1/nodes/{node_id}/heartbeat to the coordination service.
func (c *CoordClient) heartbeat(ctx context.Context) error {
	body, err := json.Marshal(coordHeartbeatRequest{
		BloomSize: c.inv.approximateSize(),
		Ready:     c.inv.isReady(),
	})
	if err != nil {
		return fmt.Errorf("coord: marshalling heartbeat request: %w", err)
	}

	// url.PathEscape prevents a nodeID containing "/" or "?" from producing a
	// malformed URL or routing to an unintended endpoint on the coord service.
	url := c.coordURL + fmt.Sprintf(coordHeartbeatFmt, url.PathEscape(c.nodeID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("coord: building heartbeat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("coord: PUT heartbeat: %w", err)
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)) //nolint:errcheck
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("coord: heartbeat returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// deregister sends DELETE /api/v1/nodes/{node_id} to the coordination service.
func (c *CoordClient) deregister(ctx context.Context) error {
	url := c.coordURL + fmt.Sprintf(coordDeregisterFmt, url.PathEscape(c.nodeID))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("coord: building deregister request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("coord: DELETE deregister: %w", err)
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)) //nolint:errcheck
	resp.Body.Close()
	// 404 means the coord service already dropped us (e.g. it restarted); treat as success.
	if resp.StatusCode != http.StatusOK &&
		resp.StatusCode != http.StatusNoContent &&
		resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("coord: deregister returned HTTP %d", resp.StatusCode)
	}

	c.obs.Logger.Info("coord: node deregistered", "node_id", c.nodeID)
	return nil
}
