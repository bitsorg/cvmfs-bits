// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// fakeDiff emits the given object hashes, optionally failing after `failAfter`
// emits (to exercise the mid-stream error → trailer path).
type fakeDiff struct {
	hashes    []string
	failAfter int // 0 = never fail
	gotFrom   string
	gotTo     string
	gotRepo   string
}

func (d *fakeDiff) Diff(_ context.Context, repo, from, to string, emit func(manifest.ObjRef) error) error {
	d.gotRepo, d.gotFrom, d.gotTo = repo, from, to
	for i, h := range d.hashes {
		if d.failAfter > 0 && i >= d.failAfter {
			return fmt.Errorf("diff source: simulated failure")
		}
		if err := emit(manifest.ObjRef{Hash: h, Size: int64(10 + i)}); err != nil {
			return err
		}
	}
	return nil
}

func TestCatchupHandlerStreamsNDJSON(t *testing.T) {
	d := &fakeDiff{hashes: []string{"aa11", "bb22", "cc33"}}
	h := &CatchupHandler{Source: d, BaseURLs: []string{"http://s0/cvmfs/r/data"}}
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/s1/catchup?repo=cms.cern.ch&from=oldroot&to=newroot")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	var objs []manifest.ObjRef
	hdr, err := manifest.DecodeNDJSON(resp.Body, func(o manifest.ObjRef) error {
		objs = append(objs, o)
		return nil
	})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if hdr.Generator != manifest.GeneratorDiff || hdr.TargetRootHash != "newroot" || hdr.BaseRootHash != "oldroot" {
		t.Fatalf("header wrong: %+v", hdr)
	}
	if len(objs) != 3 || objs[0].Hash != "aa11" || objs[2].Hash != "cc33" {
		t.Fatalf("objects wrong: %+v", objs)
	}
	// Trailer is only readable after the body is fully consumed (done above).
	if got := resp.Trailer.Get("X-Catchup-Complete"); got != "1" {
		t.Fatalf("completion trailer = %q, want 1", got)
	}
	if d.gotRepo != "cms.cern.ch" || d.gotFrom != "oldroot" || d.gotTo != "newroot" {
		t.Fatalf("diff source got wrong args: %+v", d)
	}
}

func TestCatchupHandlerMidStreamErrorSignalsIncomplete(t *testing.T) {
	d := &fakeDiff{hashes: []string{"aa11", "bb22", "cc33"}, failAfter: 1}
	srv := httptest.NewServer(&CatchupHandler{Source: d, BaseURLs: []string{"http://s0/cvmfs/r/data"}})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/s1/catchup?repo=r&to=newroot")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// Header + 1 object were already sent with a 200, so the only failure signal
	// is the trailer.
	var objs []manifest.ObjRef
	if _, err := manifest.DecodeNDJSON(resp.Body, func(o manifest.ObjRef) error {
		objs = append(objs, o)
		return nil
	}); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Trailer.Get("X-Catchup-Complete") == "1" {
		t.Fatal("trailer must NOT report complete after a mid-stream diff error")
	}
}

func TestCatchupHandlerBadParams(t *testing.T) {
	srv := httptest.NewServer(&CatchupHandler{Source: &fakeDiff{}, BaseURLs: []string{"http://s0/x"}})
	defer srv.Close()

	cases := []struct {
		q    string
		code int
	}{
		{"repo=r&to=goodroot", http.StatusOK},
		{"to=goodroot", http.StatusBadRequest},                    // missing repo
		{"repo=r", http.StatusBadRequest},                         // missing to
		{"repo=r&to=../etc/passwd", http.StatusBadRequest},        // unsafe to
		{"repo=r&to=goodroot&from=../x", http.StatusBadRequest},   // unsafe from
		{"repo=%3Brm+-rf+%2F&to=goodroot", http.StatusBadRequest}, // shell-meta repo ";rm -rf /"
		{"repo=a%2Fb&to=goodroot", http.StatusBadRequest},         // repo with slash
	}
	for _, c := range cases {
		resp, err := http.Get(srv.URL + "/s1/catchup?" + c.q)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.code {
			t.Fatalf("q=%q: status %d, want %d", c.q, resp.StatusCode, c.code)
		}
	}
}

func TestCatchupHandlerRejectsNonGET(t *testing.T) {
	srv := httptest.NewServer(&CatchupHandler{Source: &fakeDiff{}, BaseURLs: []string{"http://s0/x"}})
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/s1/catchup?repo=r&to=root", "text/plain", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status %d, want 405", resp.StatusCode)
	}
}
