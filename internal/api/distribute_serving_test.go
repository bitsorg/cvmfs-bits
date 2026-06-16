// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute/manifest"
	"cvmfs.io/prepub/internal/distribute/serve"
)

func TestMountDistributeServing(t *testing.T) {
	ctx := context.Background()
	c, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	obj := []byte("served object bytes")
	hash := "0123456789abcdef0123456789abcdef01234567" // 40 hex
	if err := c.Put(ctx, hash, bytes.NewReader(obj), int64(len(obj))); err != nil {
		t.Fatal(err)
	}

	store := serve.NewMemManifestStore()
	router := mux.NewRouter()
	mountDistributeServing(router, nil /*no auth*/, nil, DistributeServing{CAS: c, Manifests: store})
	srv := httptest.NewServer(router)
	defer srv.Close()

	// Object GET.
	resp, err := http.Get(srv.URL + "/cvmfs/cms.cern.ch/data/" + hash[:2] + "/" + hash[2:])
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != string(obj) {
		t.Fatalf("object GET: code=%d body=%q", resp.StatusCode, body)
	}

	// Manifest ingest (POST) then receiver GET.
	var mb bytes.Buffer
	_ = (&manifest.Manifest{
		TransactionID: "t", Repo: "cms.cern.ch", TargetRootHash: "r",
		BaseURLs: []string{"u"}, Generator: manifest.GeneratorPipeline, Auth: manifest.AuthPublic,
	}).Encode(&mb)
	pr, err := http.Post(srv.URL+"/api/v1/distribute/manifests", "application/json", &mb)
	if err != nil {
		t.Fatal(err)
	}
	pr.Body.Close()
	if pr.StatusCode != 201 {
		t.Fatalf("ingest status = %d", pr.StatusCode)
	}

	gr, err := http.Get(srv.URL + "/s1/t/manifest")
	if err != nil {
		t.Fatal(err)
	}
	gb, _ := io.ReadAll(gr.Body)
	gr.Body.Close()
	if gr.StatusCode != 200 || !bytes.Contains(gb, []byte(`"transaction_id":"t"`)) {
		t.Fatalf("manifest GET: code=%d body=%q", gr.StatusCode, gb)
	}
}
