// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/lease"
)

// encodingBackend records what a commit was told of its payload.
type encodingBackend struct {
	noopBackend
	mu       sync.Mutex
	encoding string
	limit    int64
	called   bool
}

func (b *encodingBackend) Commit(_ context.Context, req lease.CommitRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.called, b.encoding, b.limit = true, req.TarEncoding, req.TarLimit
	return nil
}

func gzipBytes(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(raw)
	zw.Close()
	return buf.Bytes()
}

// A gzip payload on the ingest path is taken, and its commit is told so with
// the limit of its tar.
func TestSubmitJob_GzipPayloadReachesTheIngestCommit(t *testing.T) {
	srv, _, orch := newTestServer(t)
	be := &encodingBackend{}
	orch.PublishPaths = map[string]lease.Backend{"ingest": be}
	srv.SetUploadLimits(32<<30, 0)

	rec := httptest.NewRecorder()
	srv.submitJob(rec, newMultipartRequest(t, map[string]string{
		"repo": "software.cern.ch", "path": "x86_64-el9/pkg/1.0",
		"publish_path": "ingest", "tar_encoding": "gzip",
	}, gzipBytes(t, []byte("payload"))))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	be.mu.Lock()
	defer be.mu.Unlock()
	if !be.called || be.encoding != lease.EncodingGzip || be.limit != 32<<30 {
		t.Errorf("commit called=%v encoding=%q limit=%d", be.called, be.encoding, be.limit)
	}
}

// Refused before anything is kept: another encoding, a path that would read
// the payload as a plain tar, and a payload that is not gzip.
func TestSubmitJob_CompressedPayloadRefusals(t *testing.T) {
	gz := gzipBytes(t, []byte("payload"))
	for name, tc := range map[string]struct {
		fields  map[string]string
		payload []byte
		want    string
	}{
		"zstd":        {map[string]string{"publish_path": "ingest", "tar_encoding": "zstd"}, gz, "not supported"},
		"prepub path": {map[string]string{"tar_encoding": "gzip"}, gz, "paths only"},
		"not gzip":    {map[string]string{"publish_path": "ingest", "tar_encoding": "gzip"}, []byte("plain tar"), "is not"},
	} {
		srv, sp, orch := newTestServer(t)
		orch.Lease = &noopBackend{}
		orch.PublishPaths = map[string]lease.Backend{"ingest": &encodingBackend{}}
		tc.fields["repo"], tc.fields["path"] = "software.cern.ch", "x86_64-el9/pkg/1.0"
		rec := httptest.NewRecorder()
		srv.submitJob(rec, newMultipartRequest(t, tc.fields, tc.payload))
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), tc.want) {
			t.Errorf("%s: got %d %s, want 400 with %q", name, rec.Code, rec.Body.String(), tc.want)
		}
		if p := findSpooledTar(t, sp.Root); p != "" {
			t.Errorf("%s: refused submission left a payload behind: %s", name, p)
		}
	}
}

// A node with an ingest path says it takes gzip; one without says nothing.
func TestHealth_AdvertisesTarEncodings(t *testing.T) {
	for _, withIngest := range []bool{true, false} {
		srv, _, orch := newTestServer(t)
		if withIngest {
			orch.PublishPaths = map[string]lease.Backend{"ingest-batch": &encodingBackend{}}
		}
		rec := httptest.NewRecorder()
		srv.health(rec, httptest.NewRequest("GET", "/api/v1/health", nil))
		var body struct {
			TarEncodings []string `json:"tar_encodings"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(body.TarEncodings, ","); (got == "gzip") != withIngest {
			t.Errorf("ingest path %v: tar_encodings = %q", withIngest, got)
		}
	}
}

// gzipBatchFake commits batches, giving each compressed member a tar size as
// the ingest backend does once it has written it.
type gzipBatchFake struct {
	encodingBackend
	members []lease.BatchMember
}

func (b *gzipBatchFake) CommitBatch(_ context.Context, req lease.BatchCommitRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.members = append(b.members, req.Members...)
	for _, m := range req.Members {
		if m.TarBytes != nil {
			*m.TarBytes = 1000
		}
	}
	n := int64(2000)
	req.Stats.TarBytes = &n
	return nil
}

// Batched gzip payloads: each member is told its encoding and limit, and each
// record says what was sent and how large the tar was.
func TestIngestBatch_GzipMembersAndTheirRecords(t *testing.T) {
	srv, _, orch := newTestServer(t)
	w := withMeasurements(t, orch)
	be := &gzipBatchFake{}
	orch.PublishPaths = map[string]lease.Backend{IngestBatchPublishPath: be}
	orch.IngestBatch = IngestBatchConfig{Max: 2, Linger: 500 * time.Millisecond}
	srv.SetUploadLimits(32<<30, 0)
	gz := gzipBytes(t, []byte("payload"))
	for _, p := range []string{pkgs + "A/1-1", pkgs + "B/1-1"} {
		rec := httptest.NewRecorder()
		srv.submitJob(rec, newMultipartRequest(t, map[string]string{"repo": "software.cern.ch", "path": p,
			"publish_path": IngestBatchPublishPath, "build_id": "b1", "tar_encoding": "gzip"}, gz))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	be.mu.Lock()
	for _, m := range be.members {
		if m.Encoding != lease.EncodingGzip || m.Limit != 32<<30 || m.TarBytes == nil {
			t.Errorf("member %+v: want gzip, the limit and a size to fill", m)
		}
	}
	if len(be.members) != 2 {
		t.Errorf("%d members batched, want 2", len(be.members))
	}
	be.mu.Unlock()
	recs, err := w.Read("b1")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if r.TarEncoding != "gzip" || r.PayloadBytes == nil || *r.PayloadBytes != int64(len(gz)) ||
			r.TarBytes == nil || *r.TarBytes != 1000 {
			t.Errorf("record %s: encoding %q payload %v tar %v", r.Path, r.TarEncoding, r.PayloadBytes, r.TarBytes)
		}
	}
	if len(recs) != 2 {
		t.Errorf("%d records, want 2", len(recs))
	}
}
