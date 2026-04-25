package receiver

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
)

// bloomHandler handles GET /api/v1/bloom on the control channel.
//
// It returns the current inventory Bloom filter serialised in the binary
// format produced by bloom.BloomFilter.WriteTo.  Callers (the distributor and
// the coordination service) deserialise the result with bloom.BloomFilter.ReadFrom
// and use it to compute the set of objects the receiver does not yet hold.
//
// Authentication: requires an Authorization: Bearer <token> header where the
// token is HMAC-SHA256(HMACSecret, "bloom-read") encoded as hex.  This reuses
// the same shared secret as the announce endpoint without requiring a per-request
// timestamp, since the bloom endpoint is read-only and has no replay surface.
// In DevMode the header is not checked.
//
// If the inventory is still being built (populateFromCAS has not finished),
// the handler returns 503 Service Unavailable with a Retry-After: 10 header.
// Callers should treat 503 as "retry in a few seconds" rather than a hard error.
//
// The response body is binary (Content-Type: application/octet-stream) and is
// not compressed because Bloom filter bit arrays are not compressible.
func (r *Receiver) bloomHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Authenticate: validate the static bloom-read bearer token derived from
	// the shared HMAC secret.  This prevents unauthenticated enumeration of
	// which CAS objects the node holds.  DevMode bypasses the check.
	if !r.cfg.DevMode {
		if err := r.verifyBloomToken(req); err != nil {
			r.cfg.Obs.Logger.Warn("bloom_handler: authentication failed",
				"remote", req.RemoteAddr, "error", err)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	if !r.inv.isReady() {
		// Serve a proper JSON body so clients can parse the error field.
		// http.Error would set Content-Type: text/plain, which is inconsistent
		// with the JSON-shaped body.
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "10")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"inventory building, retry shortly"}`)) //nolint:errcheck — headers sent
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	if _, err := r.inv.writeTo(w); err != nil {
		// Headers have already been sent (200 + Content-Type); we cannot change
		// the status code now.  Log the error so it surfaces in monitoring.
		r.cfg.Obs.Logger.Error("bloom_handler: serialising inventory filter", "error", err)
	}
}

// bloomReadToken computes the static bearer token for GET /api/v1/bloom.
// The token is HMAC-SHA256(secret, "bloom-read") encoded as hex.
// All callers (distributor, coordination service) derive the same token from
// the shared secret and present it in the Authorization: Bearer header.
func bloomReadToken(secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("bloom-read"))
	return hex.EncodeToString(mac.Sum(nil))
}

// verifyBloomToken checks that the request carries the expected bloom-read
// bearer token derived from the receiver's HMAC secret.
func (r *Receiver) verifyBloomToken(req *http.Request) error {
	auth := req.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return errorf("missing or malformed Authorization header")
	}
	got := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	want := bloomReadToken(r.cfg.HMACSecret)
	if !hmac.Equal([]byte(got), []byte(want)) {
		return errorf("invalid bloom-read token")
	}
	return nil
}

// errorf is a local helper so we don't import "errors" just for this.
func errorf(msg string) error {
	return bloomAuthError(msg)
}

type bloomAuthError string

func (e bloomAuthError) Error() string { return string(e) }
