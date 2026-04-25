package receiver

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// announceRequest is the JSON body of POST /api/v1/announce.
type announceRequest struct {
	// PayloadID is the sender's job UUID, used as an idempotency key so that a
	// restarted sender re-announcing the same payload receives its existing
	// session token.
	PayloadID string `json:"payload_id"`
	// ObjectCount is the number of CAS objects the sender intends to push.
	ObjectCount int `json:"object_count"`
	// TotalBytes is the total compressed size of all objects.  Zero means
	// unknown; the receiver then applies a fixed minimum free-space threshold.
	TotalBytes int64 `json:"total_bytes"`
}

// announceResponse is the JSON body returned on a successful announce.
type announceResponse struct {
	// SessionToken is the opaque bearer credential for subsequent PUTs on the
	// data channel.
	SessionToken string `json:"session_token"`
	// DataEndpoint is the base URL of the plain-HTTP data channel, e.g.
	// "http://stratum1.example.com:9101".  All object PUTs go to
	// DataEndpoint/api/v1/objects/{hash}.
	DataEndpoint string `json:"data_endpoint"`
}

// hashPattern matches a valid lowercase hex CAS hash (32–64 hex characters
// covering SHA-256 and shorter digests used in tests).  It is used to reject
// path traversal attempts before any filesystem operation.
var hashPattern = regexp.MustCompile(`^[0-9a-f]{32,64}$`)

// announceHandler handles POST /api/v1/announce on the control (HTTPS) channel.
//
// Flow:
//  1. Verify HMAC-SHA256 signature (unless DevMode).
//  2. Decode and validate the request body.
//  3. Return the existing session if the payload ID was already announced.
//  4. Check available disk space against total_bytes × disk_headroom.
//  5. Create a new session and respond with the session token and data endpoint.
func (r *Receiver) announceHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Bound the body size before reading so that an oversized request produces a
	// clear 413 rather than a confusing 401 (which would arise from computing an
	// HMAC over a silently truncated body that doesn't match the sender's HMAC).
	// 64 KiB is ample for any legitimate announce payload.
	req.Body = http.MaxBytesReader(w, req.Body, 64*1024)
	body, err := io.ReadAll(req.Body)
	if isErrMaxBytes(err) {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err != nil {
		http.Error(w, "reading body", http.StatusBadRequest)
		return
	}

	// HMAC verification on the control channel.  Skipped only in DevMode so
	// that integration tests do not require a shared secret.
	if !r.cfg.DevMode {
		if err := r.verifyHMAC(req, body); err != nil {
			r.cfg.Obs.Logger.Warn("announce HMAC verification failed",
				"remote", req.RemoteAddr, "error", err)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	var ar announceRequest
	if err := json.Unmarshal(body, &ar); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if ar.PayloadID == "" {
		http.Error(w, "payload_id is required", http.StatusBadRequest)
		return
	}

	// Idempotent re-announce: return the existing session if still live.
	if existing, ok := r.store.getByPayload(ar.PayloadID); ok {
		r.cfg.Obs.Logger.Info("re-announce: returning existing session",
			"payload_id", ar.PayloadID)
		writeJSON(w, announceResponse{
			SessionToken: existing.token,
			DataEndpoint: r.cfg.dataEndpoint(),
		})
		return
	}

	// Disk space pre-check.  When total_bytes is zero the sender does not know
	// the payload size, so we skip the headroom check.  The receiver will still
	// return 507 mid-transfer if it runs out of space during the actual PUTs.
	headroom := r.cfg.DiskHeadroom
	if headroom <= 0 {
		headroom = 1.2
	}
	if ar.TotalBytes > 0 {
		// Compute required space with headroom, guarding against overflow.
		// Use integer math where possible to avoid float64 precision loss.
		// If headroom > 1, we need TotalBytes + (TotalBytes * fractional_part).
		required := ar.TotalBytes
		if headroom > 1.0 {
			// Add the fractional headroom: fractional = headroom - 1
			// extra = int64(TotalBytes * fractional), capped at MaxInt64.
			fractional := headroom - 1.0
			// Avoid overflow: if TotalBytes * fractional would exceed MaxInt64,
			// cap it there (disk check will fail anyway).
			maxExtra := int64(float64(math.MaxInt64-ar.TotalBytes) / fractional)
			extra := int64(float64(ar.TotalBytes) * fractional)
			if extra > maxExtra {
				extra = maxExtra
			}
			if ar.TotalBytes > math.MaxInt64-extra {
				required = math.MaxInt64
			} else {
				required = ar.TotalBytes + extra
			}
		}
		if err := checkDiskSpace(r.cfg.CASRoot, required); err != nil {
			r.cfg.Obs.Logger.Warn("announce rejected: insufficient disk space",
				"payload_id", ar.PayloadID, "required_bytes", required, "error", err)
			http.Error(w, err.Error(), http.StatusInsufficientStorage)
			return
		}
	}

	ttl := r.cfg.SessionTTL
	if ttl <= 0 {
		ttl = time.Hour
	}
	s, ok := r.store.create(ar.PayloadID, ttl)
	if !ok {
		r.cfg.Obs.Logger.Warn("announce rejected: session store at capacity",
			"payload_id", ar.PayloadID)
		http.Error(w, "too many active sessions — try again later", http.StatusTooManyRequests)
		return
	}

	r.cfg.Obs.Logger.Info("announce accepted",
		"payload_id", ar.PayloadID,
		"object_count", ar.ObjectCount,
		"total_bytes", ar.TotalBytes,
		"session_expires", s.expiresAt.Format(time.RFC3339))

	writeJSON(w, announceResponse{
		SessionToken: s.token,
		DataEndpoint: r.cfg.dataEndpoint(),
	})
}

// putObjectHandler handles PUT /api/v1/objects/{hash} on the plain-HTTP data
// channel.
//
// Flow:
//  1. Validate the Authorization: Bearer header against the session store.
//  2. Validate {hash} is a well-formed hex string (path traversal guard).
//  3. If the object already exists in the CAS, return 200 immediately (idempotent).
//  4. Stream the body through a SHA-256 hasher into a sibling temp file.
//  5. Verify the computed digest matches the X-Content-SHA256 header.
//  6. Atomically rename the temp file to the final CAS path.
func (r *Receiver) putObjectHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Authorise via session token.
	token := bearerToken(req)
	if token == "" {
		http.Error(w, "missing Authorization header", http.StatusUnauthorized)
		return
	}
	if _, ok := r.store.get(token); !ok {
		http.Error(w, "unknown or expired session", http.StatusUnauthorized)
		return
	}

	// Extract and validate {hash} from the URL path.
	// Expected path: /api/v1/objects/{hash}
	hash := strings.TrimPrefix(req.URL.Path, "/api/v1/objects/")
	if !hashPattern.MatchString(hash) {
		http.Error(w, "invalid hash", http.StatusBadRequest)
		return
	}

	// Enforce maximum object size before any I/O — including before the
	// idempotent Stat check.  Without this, a caller with a valid session token
	// can send an unbounded body to any PUT whose target already exists: the
	// handler returns 200 immediately but the Go HTTP server then tries to drain
	// the unread body (up to a hardcoded limit) before reusing the connection,
	// tying up the goroutine for as long as the client sends data.
	maxSize := r.cfg.MaxObjectSize
	if maxSize <= 0 {
		maxSize = 1 << 30 // 1 GiB default
	}
	req.Body = http.MaxBytesReader(w, req.Body, maxSize)

	finalPath := casPath(r.cfg.CASRoot, hash)

	// Idempotent: if the object is already present, skip the write.
	if _, err := os.Stat(finalPath); err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Ensure the CAS sub-directory exists.
	if err := os.MkdirAll(filepath.Dir(finalPath), 0700); err != nil {
		r.cfg.Obs.Logger.Error("creating CAS subdirectory",
			"hash", hash, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Write to a unique temp file to avoid races when two concurrent PUTs
	// target the same hash. Use a random suffix to ensure uniqueness.
	tmpPath := finalPath + "." + randomToken()[:8] + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		r.cfg.Obs.Logger.Error("creating temp file", "hash", hash, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Stream body through SHA-256 hasher and temp file simultaneously.
	hasher := sha256.New()
	written, copyErr := io.Copy(f, io.TeeReader(req.Body, hasher))
	syncErr := f.Sync()
	f.Close()

	if copyErr != nil {
		os.Remove(tmpPath)
		if isErrMaxBytes(copyErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		} else if isErrNoDiskSpace(copyErr) {
			http.Error(w, "insufficient storage", http.StatusInsufficientStorage)
		} else {
			http.Error(w, "write error", http.StatusInternalServerError)
		}
		return
	}
	if syncErr != nil {
		os.Remove(tmpPath)
		r.cfg.Obs.Logger.Warn("fsync temp file failed", "hash", hash, "error", syncErr)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Verify transfer integrity: computed SHA-256 of the received bytes must
	// match the X-Content-SHA256 header supplied by the sender.
	//
	// This is the primary integrity mechanism on the plain-HTTP data channel.
	// The CAS hash in the URL path is the SHA-256 of the *uncompressed* content
	// (used as the CAS key and filesystem path); X-Content-SHA256 covers the
	// *compressed* bytes-in-flight, catching corruption or substitution during
	// transfer.
	expectedSHA := req.Header.Get("X-Content-SHA256")
	if expectedSHA == "" {
		os.Remove(tmpPath)
		r.cfg.Obs.Logger.Warn("PUT missing required X-Content-SHA256 header",
			"hash", hash)
		http.Error(w, "X-Content-SHA256 header is required", http.StatusBadRequest)
		return
	}
	computedSHA := hex.EncodeToString(hasher.Sum(nil))
	if computedSHA != expectedSHA {
		os.Remove(tmpPath)
		r.cfg.Obs.Logger.Warn("hash mismatch on PUT",
			"hash", hash,
			"expected_sha", expectedSHA,
			"computed_sha", computedSHA)
		http.Error(w, fmt.Sprintf("hash mismatch: expected %s got %s",
			expectedSHA, computedSHA), http.StatusBadRequest)
		return
	}

	// Atomic rename: the object is either the old version (or absent) or the
	// new one — never a partial write.
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		// If the file already exists (EEXIST/EACCES), treat as idempotent success.
		// This can occur due to a race with another concurrent PUT.
		if _, statErr := os.Stat(finalPath); statErr == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		r.cfg.Obs.Logger.Error("renaming temp to final", "hash", hash, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Update the inventory filter so that subsequent bloom queries reflect the
	// newly written object without waiting for the next CAS walk.
	r.inv.add(hash)
	r.cfg.Obs.Metrics.ReceiverObjectsReceived.Inc()
	r.cfg.Obs.Metrics.ReceiverBytesReceived.Add(float64(written))
	r.cfg.Obs.Metrics.ReceiverBloomSize.Set(float64(r.inv.approximateSize()))
	r.cfg.Obs.Metrics.SpoolTransitions.WithLabelValues("received", "ok").Inc()
	w.WriteHeader(http.StatusOK)
}

// verifyHMAC checks the X-Timestamp and X-Signature headers on an announce
// request using HMAC-SHA256.
//
// Canonical message:
//
//	METHOD + "\n" + request_path + "\n" + hex(SHA-256(body)) + "\n" + timestamp
//
// The timestamp window is ±60 s to tolerate clock skew between nodes while
// preventing replay attacks beyond that window.
func (r *Receiver) verifyHMAC(req *http.Request, body []byte) error {
	tsStr := req.Header.Get("X-Timestamp")
	if tsStr == "" {
		return fmt.Errorf("missing X-Timestamp header")
	}
	sig := req.Header.Get("X-Signature")
	if sig == "" {
		return fmt.Errorf("missing X-Signature header")
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid X-Timestamp: %w", err)
	}
	drift := time.Now().Unix() - ts
	if drift < -60 || drift > 60 {
		return fmt.Errorf("timestamp drift %d s exceeds ±60 s window", drift)
	}

	bodyHash := sha256.Sum256(body)
	msg := req.Method + "\n" +
		req.URL.RequestURI() + "\n" +
		hex.EncodeToString(bodyHash[:]) + "\n" +
		tsStr

	mac := hmac.New(sha256.New, []byte(r.cfg.HMACSecret))
	mac.Write([]byte(msg))
	expected := hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(sig), []byte(expected)) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

// checkDiskSpace returns an error if the filesystem containing root has fewer
// than minBytes of free space available to unprivileged processes.
func checkDiskSpace(root string, minBytes int64) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(root, &stat); err != nil {
		// If we can't stat the filesystem, fail open: space may be fine and
		// we do not want to block all publishes on a stat failure.
		return nil
	}
	// Safely compute available space, guarding against overflow.
	// stat.Bavail is the number of free blocks, stat.Bsize is bytes per block.
	// Both are uint64, but the result fits in int64 for any practical filesystem.
	const maxInt64 = 1<<63 - 1
	bsize := int64(stat.Bsize)
	if bsize > 0 && stat.Bavail > uint64(maxInt64)/uint64(bsize) {
		// Overflow prevented: available space is huge (> 9 exabytes).
		return nil
	}
	avail := int64(stat.Bavail) * bsize
	if avail < minBytes {
		return fmt.Errorf("insufficient disk space: %d bytes available, %d required",
			avail, minBytes)
	}
	return nil
}

// sweepTmpFiles walks the two-level CAS directory tree and removes any files
// whose name ends with ".tmp".  These are left behind when a PUT handler is
// interrupted (process crash, kill signal) after creating the temp file but
// before the atomic rename.  Their names are random (randomToken()[:8] suffix)
// so they will never be matched by the CAS walk and accumulate indefinitely
// without this cleanup.
//
// sweepTmpFiles is called once at receiver startup in a background goroutine.
// It is a best-effort operation: individual removal failures are logged and
// skipped rather than aborting the sweep.  The sweep respects ctx so that
// Shutdown() can interrupt it promptly on a stalling filesystem.
func sweepTmpFiles(ctx context.Context, casRoot string, logFn func(msg string, args ...any)) error {
	prefixEntries, err := os.ReadDir(casRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // CAS not initialised yet — nothing to sweep
		}
		return fmt.Errorf("sweepTmpFiles: reading CAS root %q: %w", casRoot, err)
	}
	var removed int
	for _, prefixEntry := range prefixEntries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if !prefixEntry.IsDir() || len(prefixEntry.Name()) != 2 {
			continue
		}
		subDir := filepath.Join(casRoot, prefixEntry.Name())
		entries, err := os.ReadDir(subDir)
		if err != nil {
			logFn("receiver: sweepTmpFiles skipping unreadable subdir",
				"dir", subDir, "error", err)
			continue
		}
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".tmp") {
				continue
			}
			tmpPath := filepath.Join(subDir, e.Name())
			if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
				logFn("receiver: sweepTmpFiles failed to remove", "path", tmpPath, "error", err)
			} else {
				removed++
			}
		}
	}
	if removed > 0 {
		logFn("receiver: removed orphaned .tmp files", "count", removed)
	}
	return nil
}

// casPath constructs the CVMFS CAS filesystem path for a given hash.
// Objects are stored at {root}/{hash[0:2]}/{hash}C where 'C' denotes a
// compressed (zlib) object — the standard CVMFS on-disk layout.
// hash must be at least 2 characters long; the caller must validate before calling.
func casPath(root, hash string) string {
	if len(hash) < 2 {
		// This should never happen if the caller validates the hash with
		// hashPattern.MatchString first; this is a defensive check.
		return filepath.Join(root, hash, hash+"C")
	}
	return filepath.Join(root, hash[:2], hash+"C")
}

// bearerToken extracts the token value from an "Authorization: Bearer <token>"
// header, or returns "" if the header is absent or malformed.
// Trims leading/trailing whitespace from the token.
func bearerToken(req *http.Request) string {
	auth := req.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return ""
	}
	token := strings.TrimPrefix(auth, "Bearer ")
	return strings.TrimSpace(token)
}

// isErrNoDiskSpace returns true if err (or any error in its chain) is ENOSPC,
// indicating the filesystem is full.  Using errors.As with syscall.Errno is
// more robust than string-matching err.Error(): it correctly unwraps *os.PathError
// and similar wrappers, and is not sensitive to locale-specific message text.
func isErrNoDiskSpace(err error) bool {
	if err == nil {
		return false
	}
	var errno syscall.Errno
	return errors.As(err, &errno) && errno == syscall.ENOSPC
}

// isErrMaxBytes returns true if err is the sentinel error produced by
// http.MaxBytesReader when the body exceeds the configured limit.
func isErrMaxBytes(err error) bool {
	if err == nil {
		return false
	}
	// http.MaxBytesReader returns *http.MaxBytesError (Go 1.19+).
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

// writeJSON serialises v as JSON and writes it to w with Content-Type
// application/json and HTTP 200 status.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Headers already sent; nothing useful we can do.
		return
	}
}
