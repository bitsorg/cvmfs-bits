// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package puller

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute/manifest"
	"cvmfs.io/prepub/internal/distribute/serve"
)

// TestPullChunked verifies the latency-tuned path: the missing set is split into
// chunks of FilesPerRequest and fetched with Slots requests in flight, and every
// object lands in the local CAS.
func TestPullChunked(t *testing.T) {
	ctx := context.Background()
	src, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var objs []manifest.ObjRef
	for i := 0; i < 25; i++ {
		b := []byte(fmt.Sprintf("chunked object %d — some payload bytes", i))
		h := sha1hex(b)
		mustPut(t, src, h, b)
		objs = append(objs, manifest.ObjRef{Hash: h, Size: int64(len(b))})
	}
	srv := httptest.NewServer(&serve.BundleHandler{Store: src})
	defer srv.Close()

	dst, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// 25 objects, 7 per request => 4 chunks (7,7,7,4); 3 requests in flight.
	p := &Puller{Store: dst, FilesPerRequest: 7, Slots: 3}
	m := &manifest.Manifest{
		TransactionID: "c1", Repo: "cms.cern.ch", TargetRootHash: "ROOT",
		Generator: manifest.GeneratorDiff, Auth: manifest.AuthPublic,
		Objects: objs,
	}
	res, err := p.PullChunked(ctx, srv.URL, m)
	if err != nil {
		t.Fatalf("PullChunked: %v (res=%+v)", err, res)
	}
	if res.Fetched != len(objs) || res.Failed != 0 {
		t.Fatalf("want fetched=%d failed=0, got %+v", len(objs), res)
	}
	for _, o := range objs {
		if ok, _ := dst.Exists(ctx, o.Hash); !ok {
			t.Errorf("object %s was not installed", o.Hash)
		}
	}
}
