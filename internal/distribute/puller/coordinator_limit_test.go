// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package puller

import (
	"context"
	"net/http/httptest"
	"testing"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute/manifest"
	"cvmfs.io/prepub/internal/distribute/serve"
)

func TestCoordinatorManifestSizeCap(t *testing.T) {
	ctx := context.Background()
	store := serve.NewMemManifestStore()
	if err := store.Put(ctx, &manifest.Manifest{
		TransactionID: "big", Repo: "r", TargetRootHash: "t",
		BaseURLs: []string{"http://x/data"}, Generator: manifest.GeneratorPipeline, Auth: manifest.AuthPublic,
	}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(&serve.ManifestHandler{Source: store})
	defer srv.Close()

	dst, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	coord := &Coordinator{
		ManifestBase:     srv.URL,
		MaxManifestBytes: 10, // tiny: even a small manifest exceeds this → decode fails
		Puller:           &Puller{Store: dst, Fetcher: &HTTPFetcher{}},
	}
	if _, err := coord.OnTransaction(ctx, "big"); err == nil {
		t.Fatalf("expected a decode error under the tiny manifest-size cap")
	}
}
