// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package credential

import (
	"container/list"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// IPRateLimiter is a dependency-free per-client-IP token-bucket rate limiter
// with a bounded set of tracked IPs (LRU eviction) plus a global ceiling.
// Requests over budget receive 429. It defends the (internet-exposed) control
// endpoints against request floods without relying on a firewall (R-DoS).
//
// Eviction is least-recently-USED, not FIFO: every request refreshes its IP to
// most-recently-used, so a flood of one-shot spoofed IPs evicts other one-shot
// entries rather than a legitimate client that keeps making requests.
type IPRateLimiter struct {
	mu      sync.Mutex
	ll      *list.List               // access order: most-recently-used at Front, LRU at Back
	buckets map[string]*list.Element // ip -> element whose Value is *ipBucket
	maxIPs  int
	rate    float64 // tokens/sec per IP
	burst   float64
	gTokens float64
	gRate   float64
	gBurst  float64
	gLast   time.Time

	trustedProxies []*net.IPNet // when the peer is one of these, honour X-Forwarded-For
}

type ipBucket struct {
	ip     string // kept so the map entry can be removed when this bucket is evicted
	tokens float64
	last   time.Time
}

// NewIPRateLimiter builds a limiter: perIPPerSec/perIPBurst bound a single IP,
// globalPerSec/globalBurst bound all traffic, maxIPs caps tracked IPs.
func NewIPRateLimiter(perIPPerSec, perIPBurst float64, maxIPs int, globalPerSec, globalBurst float64) *IPRateLimiter {
	if maxIPs < 1 {
		maxIPs = 1 // never silently disable per-IP tracking
	}
	return &IPRateLimiter{
		ll: list.New(), buckets: map[string]*list.Element{}, maxIPs: maxIPs,
		rate: perIPPerSec, burst: perIPBurst,
		gTokens: globalBurst, gRate: globalPerSec, gBurst: globalBurst, gLast: time.Now(),
	}
}

func (l *IPRateLimiter) allow(ip string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Global bucket.
	l.gTokens += l.gRate * now.Sub(l.gLast).Seconds()
	if l.gTokens > l.gBurst {
		l.gTokens = l.gBurst
	}
	l.gLast = now
	if l.gTokens < 1 {
		return false
	}
	// Per-IP bucket (LRU: refresh on hit, evict the least-recently-used).
	var b *ipBucket
	if el := l.buckets[ip]; el != nil {
		l.ll.MoveToFront(el) // this IP is now most-recently-used
		b = el.Value.(*ipBucket)
	} else {
		if len(l.buckets) >= l.maxIPs {
			if back := l.ll.Back(); back != nil { // evict the least-recently-used IP
				delete(l.buckets, l.ll.Remove(back).(*ipBucket).ip)
			}
		}
		b = &ipBucket{ip: ip, tokens: l.burst, last: now}
		l.buckets[ip] = l.ll.PushFront(b)
	}
	b.tokens += l.rate * now.Sub(b.last).Seconds()
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	l.gTokens--
	return true
}

// TrustProxies marks CIDRs whose requests may carry a trusted X-Forwarded-For
// header. Only when the immediate peer is in one of these ranges is the
// forwarded client IP used for rate limiting; otherwise the header is ignored
// (so a direct client cannot spoof its IP). Default: no trusted proxies.
func (l *IPRateLimiter) TrustProxies(cidrs []string) {
	for _, c := range cidrs {
		if _, n, err := net.ParseCIDR(c); err == nil {
			l.trustedProxies = append(l.trustedProxies, n)
		}
	}
}

// Middleware wraps next, rejecting requests that exceed the per-IP or global
// budget with 429.
func (l *IPRateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.allow(l.clientIP(r), time.Now()) {
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (l *IPRateLimiter) trusted(ip string) bool {
	p := net.ParseIP(ip)
	if p == nil {
		return false
	}
	for _, n := range l.trustedProxies {
		if n.Contains(p) {
			return true
		}
	}
	return false
}

// clientIP returns the IP the rate limiter keys on. When the direct peer is a
// trusted proxy and X-Forwarded-For is present, it returns the right-most
// forwarded hop that is not itself a trusted proxy (the real client as seen by
// the outermost trusted proxy). Otherwise it returns the direct peer.
func (l *IPRateLimiter) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if len(l.trustedProxies) == 0 || !l.trusted(host) {
		return host
	}
	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return host
	}
	parts := strings.Split(xff, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		ip := strings.TrimSpace(parts[i])
		if ip != "" && !l.trusted(ip) {
			return ip
		}
	}
	return strings.TrimSpace(parts[0])
}
