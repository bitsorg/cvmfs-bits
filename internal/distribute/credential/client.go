// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package credential

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Client is the Stratum-1 side of data-plane auth. It enrols with the publisher
// using the out-of-band node key, caches the returned short-lived token, and
// hands out a still-valid token on demand — transparently re-enrolling shortly
// before expiry. It is safe for concurrent use.
type Client struct {
	// Base is the publisher control base URL hosting the enroll endpoints.
	Base string
	// HTTP is the client used for enrollment (nil → a 10s-timeout client).
	HTTP *http.Client
	// Node is this receiver's id; Key is its out-of-band enrollment key.
	Node string
	Key  []byte
	// RefreshAhead re-enrols this long before expiry (default 60s).
	RefreshAhead time.Duration

	mu    sync.Mutex
	token string
	exp   time.Time
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func (c *Client) refreshAhead() time.Duration {
	if c.RefreshAhead > 0 {
		return c.RefreshAhead
	}
	return 60 * time.Second
}

// Token returns a currently-valid bearer token, enrolling (or re-enrolling) if
// the cached one is missing or within RefreshAhead of expiry.
func (c *Client) Token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Until(c.exp) > c.refreshAhead() {
		return c.token, nil
	}
	if err := c.enrollLocked(ctx); err != nil {
		return "", err
	}
	return c.token, nil
}

// enrollLocked performs the challenge–response and caches the token; caller holds mu.
func (c *Client) enrollLocked(ctx context.Context) error {
	if c.Node == "" || len(c.Key) == 0 || c.Base == "" {
		return fmt.Errorf("credential: Base, Node and Key are required")
	}
	base := strings.TrimRight(c.Base, "/")

	// Step 1: obtain a challenge nonce.
	cu := base + "/control/challenge?" + url.Values{"node": {c.Node}}.Encode()
	creq, err := http.NewRequestWithContext(ctx, http.MethodGet, cu, nil)
	if err != nil {
		return err
	}
	cresp, err := c.httpClient().Do(creq)
	if err != nil {
		return fmt.Errorf("credential: challenge: %w", err)
	}
	defer cresp.Body.Close()
	if cresp.StatusCode != http.StatusOK {
		return fmt.Errorf("credential: challenge status %d", cresp.StatusCode)
	}
	var ch challengeResp
	if err := json.NewDecoder(cresp.Body).Decode(&ch); err != nil || ch.Nonce == "" {
		return fmt.Errorf("credential: bad challenge response")
	}

	// Step 2: prove possession of the enrollment key over the nonce.
	body, _ := json.Marshal(enrollReq{
		Node:  c.Node,
		Nonce: ch.Nonce,
		MAC:   EnrollMACHex(c.Key, c.Node, ch.Nonce),
	})
	ereq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/control/enroll", bytes.NewReader(body))
	if err != nil {
		return err
	}
	ereq.Header.Set("Content-Type", "application/json")
	eresp, err := c.httpClient().Do(ereq)
	if err != nil {
		return fmt.Errorf("credential: enroll: %w", err)
	}
	defer eresp.Body.Close()
	if eresp.StatusCode != http.StatusOK {
		return fmt.Errorf("credential: enroll status %d", eresp.StatusCode)
	}
	var gr EnrollResp
	if err := json.NewDecoder(eresp.Body).Decode(&gr); err != nil || gr.Token == "" {
		return fmt.Errorf("credential: bad enroll response")
	}
	c.token = gr.Token
	c.exp = time.Unix(gr.ExpUnix, 0)
	return nil
}
