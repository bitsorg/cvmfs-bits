// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package puller

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute/credential"
	"cvmfs.io/prepub/internal/distribute/manifest"
	"cvmfs.io/prepub/internal/distribute/serve"
)

// emitDiff is a serve.DiffSource backed by an explicit hash list (optionally
// failing mid-stream).
type emitDiff struct {
	objs      []manifest.ObjRef
	failAfter int
}

func (d emitDiff) Diff(_ context.Context, _, _, _ string, emit func(manifest.ObjRef) error) error {
	for i, o := range d.objs {
		if d.failAfter > 0 && i >= d.failAfter {
			return fmt.Errorf("diff: simulated failure")
		}
		if err := emit(o); err != nil {
			return err
		}
	}
	return nil
}

// catchupServer wires the S0 object endpoint and the catch-up endpoint behind one
// test server (the way cvmfs-prepub serves them in pull mode).
func catchupServer(t *testing.T, src cas.Backend, repo string, diff serve.DiffSource) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("/cvmfs/", &serve.ObjectHandler{Store: src})
	srv := httptest.NewServer(mux)
	// BaseURLs must be known to build the handler; create after we know srv.URL.
	mux.Handle("/s1/catchup", &serve.CatchupHandler{
		Source:   diff,
		BaseURLs: []string{srv.URL + "/cvmfs/" + repo + "/data"},
	})
	return srv
}

func TestCoordinatorCatchupEndToEnd(t *testing.T) {
	ctx := context.Background()
	repo := "atlas.cern.ch"

	src, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := []byte("alpha-object"), []byte("bravo-object-x"), []byte("charlie-object-yz")
	hA, hB, hC := sha1hex(a), sha1hex(b), sha1hex(c)
	mustPut(t, src, hA, a)
	mustPut(t, src, hB, b)
	mustPut(t, src, hC, c)

	diff := emitDiff{objs: []manifest.ObjRef{
		{Hash: hA, Size: int64(len(a))},
		{Hash: hB, Size: int64(len(b))},
		{Hash: hC, Size: int64(len(c))},
	}}
	srv := catchupServer(t, src, repo, diff)
	defer srv.Close()

	dst, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, dst, hB, b) // already present → should be Skipped
	st := NewState(t.TempDir())
	_ = st.Set(repo, "oldroot") // receiver starts behind

	co := &Coordinator{
		ManifestBase: srv.URL,
		Puller:       &Puller{Store: dst, Fetcher: &HTTPFetcher{}, Slots: 3, State: st},
	}
	res, err := co.Catchup(ctx, repo, "newroot")
	if err != nil {
		t.Fatalf("catchup: %v", err)
	}
	if res.Total != 3 || res.Fetched != 2 || res.Skipped != 1 || res.Failed != 0 {
		t.Fatalf("counts wrong: %+v", res)
	}
	for _, h := range []string{hA, hB, hC} {
		if ok, _ := dst.Exists(ctx, h); !ok {
			t.Fatalf("object %s not installed", h)
		}
	}
	// Synced root advanced only after a clean, complete stream.
	if root, _ := st.Get(repo); root != "newroot" {
		t.Fatalf("synced root = %q, want newroot", root)
	}
}

func TestCoordinatorCatchupIncompleteDoesNotAdvanceState(t *testing.T) {
	ctx := context.Background()
	repo := "cms.cern.ch"

	src, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a, b := []byte("one-object"), []byte("two-object")
	hA, hB := sha1hex(a), sha1hex(b)
	mustPut(t, src, hA, a)
	mustPut(t, src, hB, b)

	// Diff fails after emitting the first object → trailer reports incomplete.
	diff := emitDiff{objs: []manifest.ObjRef{
		{Hash: hA, Size: int64(len(a))},
		{Hash: hB, Size: int64(len(b))},
	}, failAfter: 1}
	srv := catchupServer(t, src, repo, diff)
	defer srv.Close()

	dst, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st := NewState(t.TempDir())
	_ = st.Set(repo, "baseline")

	co := &Coordinator{
		ManifestBase: srv.URL,
		Puller:       &Puller{Store: dst, Fetcher: &HTTPFetcher{}, State: st},
	}
	_, err = co.Catchup(ctx, repo, "newroot")
	if err == nil {
		t.Fatal("expected an error for an incomplete catch-up stream")
	}
	// State must remain at the baseline so the next attempt re-pulls the full diff.
	if root, _ := st.Get(repo); root != "baseline" {
		t.Fatalf("synced root advanced on incomplete catch-up: %q", root)
	}
}

// TestCoordinatorCatchupWithEnrollmentAuth exercises the full data-plane auth
// path: the catch-up endpoint is gated by a scoped token; the receiver enrols
// with its out-of-band node key to obtain one, and the gated pull succeeds. An
// unenrolled receiver (no token) is refused.
func TestCoordinatorCatchupWithEnrollmentAuth(t *testing.T) {
	ctx := context.Background()
	repo := "lhcb.cern.ch"

	src, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a := []byte("authed-object")
	hA := sha1hex(a)
	mustPut(t, src, hA, a)
	diff := emitDiff{objs: []manifest.ObjRef{{Hash: hA, Size: int64(len(a))}}}

	// S0: object endpoint + token-gated catch-up + enrollment endpoints.
	secret := []byte("s0-signing-secret-zzzzzzzzzzzzzzzz")
	store := credential.NewMapEnrollStore()
	nodeKey := []byte("oob-key-for-lhcb-s1")
	store.Add("s1-x", nodeKey)
	enroll := credential.NewEnrollServer(store, credential.NewMinter(secret), nil)
	verifier := credential.NewVerifier(secret)

	mux := http.NewServeMux()
	mux.Handle("/cvmfs/", &serve.ObjectHandler{Store: src})
	mux.Handle("/control/", enroll.Handler())
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.Handle("/s1/catchup", credential.RequireToken(verifier, "catchup")(&serve.CatchupHandler{
		Source:   diff,
		BaseURLs: []string{srv.URL + "/cvmfs/" + repo + "/data"},
	}))

	dst, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// Unenrolled receiver: no token → catch-up refused.
	noAuth := &Coordinator{ManifestBase: srv.URL, Puller: &Puller{Store: dst, Fetcher: &HTTPFetcher{}}}
	if _, err := noAuth.Catchup(ctx, repo, "newroot"); err == nil {
		t.Fatal("catch-up without a token must be refused")
	}

	// Enrolled receiver: obtains a token via challenge-response, pull succeeds.
	cred := &credential.Client{Base: srv.URL, HTTP: srv.Client(), Node: "s1-x", Key: nodeKey}
	st := NewState(t.TempDir())
	co := &Coordinator{
		ManifestBase: srv.URL,
		Puller:       &Puller{Store: dst, Fetcher: &HTTPFetcher{}, State: st},
		TokenSource:  cred.Token,
	}
	res, err := co.Catchup(ctx, repo, "newroot")
	if err != nil {
		t.Fatalf("authed catch-up: %v", err)
	}
	if res.Fetched != 1 {
		t.Fatalf("counts: %+v", res)
	}
	if ok, _ := dst.Exists(ctx, hA); !ok {
		t.Fatal("object not installed under authed catch-up")
	}
	if root, _ := st.Get(repo); root != "newroot" {
		t.Fatalf("synced root = %q", root)
	}
}

func TestPullStreamRejectsUnsafeHashWithoutFetching(t *testing.T) {
	ctx := context.Background()
	// Build an NDJSON stream by hand with a path-traversal hash.
	m := &manifest.Manifest{
		TransactionID: "t", Repo: "r", TargetRootHash: "root",
		BaseURLs: []string{"http://unused/cvmfs/r/data"}, Generator: manifest.GeneratorDiff,
		Auth: manifest.AuthPublic,
	}
	var buf bytes.Buffer
	_ = manifest.EncodeNDJSONHeader(&buf, m)
	// hand-written bad object line (bypasses manifest.Validate, which PullStream
	// must defend against per-object)
	buf.WriteString(`{"hash":"../../etc/passwd","size":10}` + "\n")

	dst, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := &Puller{Store: dst, Fetcher: &HTTPFetcher{}}
	res, _, err := p.PullStream(ctx, &buf)
	if err == nil {
		t.Fatal("expected failure on unsafe hash")
	}
	if res.Failed != 1 || res.Fetched != 0 {
		t.Fatalf("unsafe hash must be counted failed without fetch: %+v", res)
	}
}
