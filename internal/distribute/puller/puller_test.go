// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package puller

import (
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec // matching CVMFS CAS key algorithm
	"encoding/hex"
	"net/http/httptest"
	"testing"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute/manifest"
	"cvmfs.io/prepub/internal/distribute/serve"
)

func sha1hex(b []byte) string {
	s := sha1.Sum(b) //nolint:gosec
	return hex.EncodeToString(s[:])
}

func mustPut(t *testing.T, c cas.Backend, hash string, b []byte) {
	t.Helper()
	if err := c.Put(context.Background(), hash, bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatalf("put %s: %v", hash, err)
	}
}

func TestPullVerifiesSkipsAndRejectsCorrupt(t *testing.T) {
	ctx := context.Background()

	// Source CAS on "S0", served via the P1 object handler.
	src, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	good := []byte("hello world object payload")
	hGood := sha1hex(good)
	mustPut(t, src, hGood, good)

	present := []byte("already present here")
	hPresent := sha1hex(present)
	mustPut(t, src, hPresent, present)

	// A corrupt object: stored under a hash that does NOT match its bytes, so the
	// receiver's verify must reject it.
	hBad := sha1hex([]byte("the original bytes"))
	mustPut(t, src, hBad, []byte("tampered bytes"))

	srv := httptest.NewServer(&serve.ObjectHandler{Store: src})
	defer srv.Close()
	base := srv.URL + "/cvmfs/cms.cern.ch/data"

	// Destination CAS on "S1"; pre-place hPresent to exercise Skipped.
	dst, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, dst, hPresent, present)

	st := NewState(t.TempDir())
	p := &Puller{Store: dst, Fetcher: &HTTPFetcher{}, Slots: 3, State: st}
	m := &manifest.Manifest{
		TransactionID: "t1", Repo: "cms.cern.ch", TargetRootHash: "ROOT",
		BaseURLs:  []string{base},
		Generator: manifest.GeneratorPipeline, Auth: manifest.AuthPublic,
		Objects: []manifest.ObjRef{
			{Hash: hGood, Size: int64(len(good))},
			{Hash: hPresent, Size: int64(len(present))},
			{Hash: hBad, Size: int64(len("tampered bytes"))},
		},
	}

	res, err := p.Pull(ctx, m)
	if err == nil {
		t.Fatalf("expected pull error due to corrupt object; res=%+v", res)
	}
	if res.Skipped != 1 || res.Fetched != 1 || res.Failed != 1 {
		t.Fatalf("counts wrong: %+v", res)
	}
	if ok, _ := dst.Exists(ctx, hGood); !ok {
		t.Fatalf("good object should be installed")
	}
	if ok, _ := dst.Exists(ctx, hBad); ok {
		t.Fatalf("corrupt object must NOT be installed")
	}
	// State must NOT advance on a failed pull.
	if root, _ := st.Get("cms.cern.ch"); root != "" {
		t.Fatalf("state advanced on failed pull: %q", root)
	}
}

func TestPullSuccessAdvancesState(t *testing.T) {
	ctx := context.Background()
	src, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a, b := []byte("object-a-bytes"), []byte("object-b-bytes-longer")
	hA, hB := sha1hex(a), sha1hex(b)
	mustPut(t, src, hA, a)
	mustPut(t, src, hB, b)

	srv := httptest.NewServer(&serve.ObjectHandler{Store: src})
	defer srv.Close()

	dst, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st := NewState(t.TempDir())
	p := &Puller{Store: dst, Fetcher: &HTTPFetcher{}, State: st}
	m := &manifest.Manifest{
		TransactionID: "t2", Repo: "lhcb.cern.ch", TargetRootHash: "ROOT2",
		BaseURLs:  []string{srv.URL + "/cvmfs/lhcb.cern.ch/data"},
		Generator: manifest.GeneratorPipeline, Auth: manifest.AuthPublic,
		Objects: []manifest.ObjRef{
			{Hash: hA, Size: int64(len(a))},
			{Hash: hB, Size: int64(len(b))},
		},
	}
	res, err := p.Pull(ctx, m)
	if err != nil || res.Fetched != 2 || res.Failed != 0 {
		t.Fatalf("pull: err=%v res=%+v", err, res)
	}
	if root, _ := st.Get("lhcb.cern.ch"); root != "ROOT2" {
		t.Fatalf("state not advanced: %q", root)
	}
	for _, h := range []string{hA, hB} {
		if ok, _ := dst.Exists(ctx, h); !ok {
			t.Fatalf("object %s not installed", h)
		}
	}
}
