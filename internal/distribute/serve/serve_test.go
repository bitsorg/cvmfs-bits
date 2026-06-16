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
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

type fakeObjStore struct{ obj map[string][]byte }

func (f fakeObjStore) Exists(_ context.Context, h string) (bool, error) {
	_, ok := f.obj[h]
	return ok, nil
}

func (f fakeObjStore) Size(_ context.Context, h string) (int64, error) {
	b, ok := f.obj[h]
	if !ok {
		return 0, fmt.Errorf("absent")
	}
	return int64(len(b)), nil
}

func (f fakeObjStore) Get(_ context.Context, h string) (io.ReadCloser, error) {
	b, ok := f.obj[h]
	if !ok {
		return nil, fmt.Errorf("absent")
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func TestObjectHandler(t *testing.T) {
	h := &ObjectHandler{Store: fakeObjStore{obj: map[string][]byte{"00aabb": []byte("payload")}}}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cvmfs/cms.cern.ch/data/00/aabb", nil))
	if rec.Code != 200 || rec.Body.String() != "payload" {
		t.Fatalf("GET object: code=%d body=%q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") == "" || rec.Header().Get("Content-Length") != "7" {
		t.Fatalf("headers: %v", rec.Header())
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/cvmfs/cms.cern.ch/data/00/aabb", nil))
	if rec.Code != 200 || rec.Body.Len() != 0 {
		t.Fatalf("HEAD: code=%d bodylen=%d", rec.Code, rec.Body.Len())
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cvmfs/cms.cern.ch/data/ff/ffff", nil))
	if rec.Code != 404 {
		t.Fatalf("missing object: code=%d", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cvmfs/cms.cern.ch/data/../etc", nil))
	if rec.Code != 400 {
		t.Fatalf("traversal path must be rejected: code=%d", rec.Code)
	}
}

func sampleManifest() *manifest.Manifest {
	return &manifest.Manifest{
		TransactionID:  "txn-1",
		Repo:           "cms.cern.ch",
		TargetRootHash: "bbbb",
		BaseURLs:       []string{"https://s0/cvmfs/cms.cern.ch/data"},
		Generator:      manifest.GeneratorPipeline,
		Auth:           manifest.AuthPublic,
		Objects:        []manifest.ObjRef{{Hash: "00aa", Size: 4}, {Hash: "11bb", Size: 8}},
	}
}

func TestManifestServeAndIngest(t *testing.T) {
	store := NewMemManifestStore()
	ingest := &ManifestIngestHandler{Store: store}
	serve := &ManifestHandler{Source: store}

	// Gateway POSTs a manifest (JSON).
	var body bytes.Buffer
	_ = sampleManifest().Encode(&body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/distribute/manifests", &body)
	req.Header.Set("Content-Type", "application/json")
	ingest.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("ingest JSON: code=%d body=%q", rec.Code, rec.Body.String())
	}

	// Receiver GETs it as JSON.
	rec = httptest.NewRecorder()
	serve.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/s1/txn-1/manifest", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"target_root_hash":"bbbb"`) {
		t.Fatalf("serve JSON: code=%d body=%q", rec.Code, rec.Body.String())
	}

	// Receiver GETs it as NDJSON (streamed).
	rec = httptest.NewRecorder()
	serve.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/s1/txn-1/manifest?stream=1", nil))
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("serve NDJSON: code=%d ct=%q", rec.Code, rec.Header().Get("Content-Type"))
	}
	if lines := strings.Count(strings.TrimRight(rec.Body.String(), "\n"), "\n"); lines != 2 {
		t.Fatalf("NDJSON should be header + 2 objects = 2 newlines between 3 lines, got %d", lines)
	}

	// Unknown txn → 404.
	rec = httptest.NewRecorder()
	serve.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/s1/nope/manifest", nil))
	if rec.Code != 404 {
		t.Fatalf("unknown txn: code=%d", rec.Code)
	}

	// Invalid manifest ingest → 400.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/distribute/manifests", strings.NewReader(`{"repo":"x"}`))
	req.Header.Set("Content-Type", "application/json")
	ingest.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Fatalf("invalid ingest: code=%d", rec.Code)
	}
}

func TestManifestIngestNDJSON(t *testing.T) {
	store := NewMemManifestStore()
	ingest := &ManifestIngestHandler{Store: store}
	var body bytes.Buffer
	_ = sampleManifest().EncodeNDJSON(&body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/distribute/manifests", &body)
	req.Header.Set("Content-Type", "application/x-ndjson")
	ingest.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("ndjson ingest: code=%d body=%q", rec.Code, rec.Body.String())
	}
	m, ok, _ := store.Manifest(context.Background(), "txn-1")
	if !ok || len(m.Objects) != 2 {
		t.Fatalf("stored manifest objects=%v ok=%v", m, ok)
	}
}

type discoFunc func(repo string) (Discovery, bool, error)

func (f discoFunc) Discovery(_ context.Context, repo string) (Discovery, bool, error) {
	return f(repo)
}

func TestDiscovery(t *testing.T) {
	base := Discovery{
		Repos:        []string{"cms.cern.ch"},
		ControlPlane: ControlPlaneRef{Type: "mqtt", URL: "mqtts://broker:8883"},
	}
	signed, err := base.Sign(func(payload []byte) (string, error) { return "sig-" + fmt.Sprint(len(payload)), nil })
	if err != nil || signed.Signature == "" {
		t.Fatalf("sign: sig=%q err=%v", signed.Signature, err)
	}

	h := &DiscoveryHandler{Source: discoFunc(func(repo string) (Discovery, bool, error) {
		if repo != "cms.cern.ch" {
			return Discovery{}, false, nil
		}
		return signed, true, nil
	})}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cvmfs/cms.cern.ch/.cvmfsbits", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"type":"mqtt"`) || !strings.Contains(rec.Body.String(), `"signature":"sig-`) {
		t.Fatalf("discovery serve: code=%d body=%q", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cvmfs/other.cern.ch/.cvmfsbits", nil))
	if rec.Code != 404 {
		t.Fatalf("unknown repo: code=%d", rec.Code)
	}
}

func TestPinner(t *testing.T) {
	p := NewMemPinner()
	p.Pin("txn-1", []string{"a", "b"}, time.Hour)
	p.Pin("txn-2", []string{"b", "c"}, time.Hour)
	for _, h := range []string{"a", "b", "c"} {
		if !p.IsPinned(h) {
			t.Fatalf("%s should be pinned", h)
		}
	}
	p.Release("txn-1")
	if p.IsPinned("a") {
		t.Fatalf("a should be unpinned after release")
	}
	if !p.IsPinned("b") {
		t.Fatalf("b still pinned by txn-2 (refcount)")
	}

	p.Pin("txn-3", []string{"z"}, -time.Second) // already expired
	released := p.Sweep(time.Now())
	if len(released) != 1 || released[0] != "txn-3" || p.IsPinned("z") {
		t.Fatalf("sweep should release txn-3: released=%v pinned=%v", released, p.IsPinned("z"))
	}
}

func TestBuildManifestAndObjectURL(t *testing.T) {
	store := fakeObjStore{obj: map[string][]byte{"00aa": []byte("xxxx"), "11bb": []byte("yyyyyyyy")}}
	m, err := BuildManifest(context.Background(), BuildParams{
		TxnID:          "t",
		Repo:           "cms.cern.ch",
		TargetRootHash: "r",
		BaseURLs:       []string{"https://s0/cvmfs/cms.cern.ch/data"},
		Generator:      manifest.GeneratorPipeline,
		Auth:           manifest.AuthPublic,
	}, []string{"00aa", "11bb"}, store)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if m.TotalSize != 12 || len(m.Objects) != 2 || m.Objects[0].Size != 4 {
		t.Fatalf("build sizes wrong: total=%d objs=%+v", m.TotalSize, m.Objects)
	}

	if got := ObjectURL("https://s0/cvmfs/cms.cern.ch/data", "00aabb"); got != "https://s0/cvmfs/cms.cern.ch/data/00/aabb" {
		t.Fatalf("ObjectURL = %q", got)
	}
}
