package distribute

// push.go — per-object and batch push logic for Stratum 1 distribution.
//
// This file is split out of distributor.go to keep file sizes manageable.
// It contains:
//   • pushObject  — single-object PUT (legacy HTTPS or new two-channel path)
//   • pushBatch   — streaming multipart POST to batch endpoint
//   • isBatchUnsupported / errBatchUnsupported — fallback sentinel
//   • max — integer helper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"cvmfs.io/prepub/internal/cas"
)

// errBatchUnsupported is returned by pushBatch when the receiver signals
// it does not understand the batch endpoint (HTTP 404 or 415).
var errBatchUnsupported = errors.New("batch endpoint not supported")

// isBatchUnsupported reports whether err (possibly wrapped) signals that the
// remote Stratum 1 receiver does not support the batch endpoint.
// The Distributor uses this to fall back to per-object PUTs transparently.
func isBatchUnsupported(err error) bool {
	return errors.Is(err, errBatchUnsupported)
}

// pushBatch sends a batch of objects to the Stratum 1 batch endpoint via
// streaming multipart POST. It returns the list of hashes that were
// successfully received, or an error.
//
// The batch endpoint is:
//
//	POST <endpoint>/cvmfs-receiver/objects/batch
//	Content-Type: multipart/form-data; boundary=...
//
// Each part has name "object" and filename set to the object hash.
// A successful response is 200/201 JSON: {"received": ["hash1", "hash2", ...]}.
//
// The timeout applies to the entire batch, not per-object, to prevent multi-hour
// deadlines on large batches that could mask hung connections.
func pushBatch(ctx context.Context, endpoint string, hashes []string, casBackend cas.Backend, timeout time.Duration) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	// Stream objects from CAS into the multipart writer in a goroutine so the
	// HTTP request body can be consumed as it is produced.
	go func() {
		for _, hash := range hashes {
			select {
			case <-ctx.Done():
				pw.CloseWithError(ctx.Err())
				return
			default:
			}
			r, err := casBackend.Get(ctx, hash)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("cas get %s: %w", hash, err))
				return
			}
			part, err := mw.CreateFormFile("object", hash)
			if err != nil {
				r.Close()
				pw.CloseWithError(fmt.Errorf("creating form file for %s: %w", hash, err))
				return
			}
			if _, err := io.Copy(part, r); err != nil {
				r.Close()
				pw.CloseWithError(fmt.Errorf("copying %s to multipart: %w", hash, err))
				return
			}
			r.Close()
		}
		mw.Close()
		pw.Close()
	}()

	batchURL := fmt.Sprintf("%s/cvmfs-receiver/objects/batch", endpoint)
	req, err := http.NewRequestWithContext(ctx, "POST", batchURL, pr)
	if err != nil {
		pw.CloseWithError(err)
		return nil, fmt.Errorf("creating batch request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := sharedClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("batch push to %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusUnsupportedMediaType {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)) //nolint:errcheck
		return nil, fmt.Errorf("%w (status %d)", errBatchUnsupported, resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("batch push failed (status %d): %s", resp.StatusCode, body)
	}

	var result struct {
		Received []string `json:"received"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding batch response: %w", err)
	}
	return result.Received, nil
}

// max returns the larger of a and b.  Used to compute actual concurrency as
// min(requested, available) without underflowing to a negative value.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// pushObject sends a single object to a Stratum 1 endpoint via PUT.
//
// When sessionToken and dataEndpoint are non-empty the object is sent to the
// plain-HTTP data channel returned by a prior announce (§20 of REFERENCE.md).
// When either is empty (legacy receiver or announce not configured) the object
// is pushed directly to the HTTPS control endpoint.
//
// Both paths propagate OTel trace context for observability.
//
// devMode disables the SSRF check on the data endpoint so that private/Docker
// network addresses are accepted during local testing.
func pushObject(ctx context.Context, endpoint, hash string, backend cas.Backend, timeout time.Duration, sessionToken, dataEndpoint string, devMode bool) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	r, err := backend.Get(ctx, hash)
	if err != nil {
		return fmt.Errorf("getting from CAS: %w", err)
	}
	defer r.Close()

	var (
		targetURL string
		client    *http.Client
	)

	if sessionToken != "" && dataEndpoint != "" {
		// New two-channel path: plain HTTP data channel with session auth.
		u, err := url.Parse(dataEndpoint)
		if err != nil {
			return fmt.Errorf("invalid data endpoint URL %q: %w", dataEndpoint, err)
		}
		if u.Host == "" {
			return fmt.Errorf("data endpoint URL %q has no host", dataEndpoint)
		}
		// Reject private/loopback addresses (same check as endpoint validation).
		// In devMode this check is skipped so that Docker-internal hostnames
		// (e.g. stratum1-a resolving to 172.x.x.x) are accepted.
		if !devMode {
			if err := rejectPrivateHost(u.Hostname()); err != nil {
				return fmt.Errorf("data endpoint %q: %w", dataEndpoint, err)
			}
		}

		// Read the full object so we can compute X-Content-SHA256 before streaming.
		data, err := io.ReadAll(r)
		if err != nil {
			return fmt.Errorf("reading object %s from CAS: %w", hash, err)
		}
		sum := sha256.Sum256(data)
		contentSHA := hex.EncodeToString(sum[:])

		targetURL = fmt.Sprintf("%s/api/v1/objects/%s", dataEndpoint, hash)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("creating data-channel request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+sessionToken)
		req.Header.Set("X-Content-SHA256", contentSHA)
		req.Header.Set("Content-Type", "application/octet-stream")
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

		resp, err := dataClient.Do(req)
		if err != nil {
			return fmt.Errorf("pushing to data channel %s: %w", targetURL, err)
		}
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("data-channel push failed (status %d)", resp.StatusCode)
		}
		return nil
	}

	// Legacy path: direct HTTPS PUT to the endpoint.
	targetURL = fmt.Sprintf("%s/cvmfs-receiver/objects/%s", endpoint, hash)
	client = sharedClient

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, r)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("pushing to %s: %w", endpoint, err)
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("push failed: status %d", resp.StatusCode)
	}
	return nil
}
