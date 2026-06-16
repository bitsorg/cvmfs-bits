// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package puller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute/manifest"
	"cvmfs.io/prepub/internal/distribute/serve"
)

// TestCoordinatorOnTransaction runs the full receiver flow against a single
// server that serves both the manifest (/s1/{txn}/manifest) and the objects
// (/cvmfs/{repo}/data/...), exactly as cvmfs-prepub does in pull mode.
func TestCoordinatorOnTransaction(t *testing.T) {
	ctx := context.Background()

	src, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a, b := []byte("coord-object-a"), []byte("coord-object-b-longer")
	hA, hB := sha1hex(a), sha1hex(b)
	mustPut(t, src, hA, a)
	mustPut(t, src, hB, b)

	store := serve.NewMemManifestStore()

	mux := http.NewServeMux()
	mux.Handle("/s1/", &serve.ManifestHandler{Source: store})
	mux.Handle("/cvmfs/", &serve.ObjectHandler{Store: src})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// The gateway/pipeline has posted the manifest; objects are served from the
	// same prepub endpoint during pre-warm.
	if err := store.Put(ctx, &manifest.Manifest{
		TransactionID: "txn-9", Repo: "cms.cern.ch", TargetRootHash: "ROOT9",
		BaseURLs:  []string{srv.URL + "/cvmfs/cms.cern.ch/data"},
		Generator: manifest.GeneratorPipeline, Auth: manifest.AuthPublic,
		Objects: []manifest.ObjRef{
			{Hash: hA, Size: int64(len(a))},
			{Hash: hB, Size: int64(len(b))},
		},
	}); err != nil {
		t.Fatal(err)
	}

	dst, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st := NewState(t.TempDir())
	coord := &Coordinator{
		ManifestBase: srv.URL,
		Puller:       &Puller{Store: dst, Fetcher: &HTTPFetcher{}, State: st},
	}

	res, err := coord.OnTransaction(ctx, "txn-9")
	if err != nil || res.Fetched != 2 {
		t.Fatalf("OnTransaction: err=%v res=%+v", err, res)
	}
	for _, h := range []string{hA, hB} {
		if ok, _ := dst.Exists(ctx, h); !ok {
			t.Fatalf("object %s not pulled", h)
		}
	}
	if root, _ := st.Get("cms.cern.ch"); root != "ROOT9" {
		t.Fatalf("state root = %q, want ROOT9", root)
	}

	// Unknown transaction → manifest 404 → error.
	if _, err := coord.OnTransaction(ctx, "missing"); err == nil {
		t.Fatalf("expected error for unknown transaction")
	}
}
