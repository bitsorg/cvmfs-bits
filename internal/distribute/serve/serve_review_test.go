// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// existsButGetFails models an object that vanished between Exists and Get
// (e.g. a GC race): the handler must return a clean error, not a 500 carrying a
// stale Content-Length for a body it never sends.
type existsButGetFails struct{}

func (existsButGetFails) Exists(context.Context, string) (bool, error) { return true, nil }
func (existsButGetFails) Size(context.Context, string) (int64, error)  { return 7, nil }
func (existsButGetFails) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("gone")
}

func TestObjectHandlerNoStaleContentLengthOnGetError(t *testing.T) {
	h := &ObjectHandler{Store: existsButGetFails{}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cvmfs/r/data/00/aabb", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	if cl := rec.Header().Get("Content-Length"); cl != "" && cl != fmt.Sprint(rec.Body.Len()) {
		t.Fatalf("stale Content-Length %q on error body of %d bytes", cl, rec.Body.Len())
	}
}

func TestManifestIngestNDJSONOverCap(t *testing.T) {
	h := &ManifestIngestHandler{Store: NewMemManifestStore(), MaxBytes: 40} // tiny cap
	var body bytes.Buffer
	_ = sampleManifest().EncodeNDJSON(&body) // header + 2 objects, > 40 bytes
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/distribute/manifests", &body)
	req.Header.Set("Content-Type", "application/x-ndjson")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 over MaxBytes cap, got %d body=%q", rec.Code, rec.Body.String())
	}
}
