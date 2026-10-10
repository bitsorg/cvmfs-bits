// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cvmfs.io/prepub/pkg/cvmfscatalog"
)

// stubReadPublishedFiles swaps the seam (restoring the original, captured
// before the swap) and records the paths it was asked for.
func stubReadPublishedFiles(t *testing.T, files map[string][]byte, oversized []string, err error) *[]string {
	t.Helper()
	real := readPublishedFilesFn
	var asked []string
	readPublishedFilesFn = func(_ context.Context, _ *http.Client, _, _ string, paths []string,
		_ cvmfscatalog.ReadLimits) (map[string][]byte, []string, error) {
		asked = append(asked, paths...)
		return files, oversized, err
	}
	t.Cleanup(func() { readPublishedFilesFn = real })
	return &asked
}

func postPublishedFiles(srv *Server, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	srv.publishedFilesHandler(rec, httptest.NewRequest("POST", "/api/v1/published/files",
		strings.NewReader(body)))
	return rec
}

// Bad bodies are 400, a non-metadata file name is refused, too many paths are
// 413, and without a stratum0 the answer is 501.
func TestPublishedFiles_Validation(t *testing.T) {
	srv, _, _ := newTestServer(t)
	many := `"` + strings.Repeat(`a/.meta.json","`, maxPublishedFiles) + `a/.meta.json"`
	for body, want := range map[string]int{
		`not json`:                                           http.StatusBadRequest,
		`{"repo":"repo.cern.ch"}`:                            http.StatusBadRequest,
		`{"repo":"repo.cern.ch","paths":[]}`:                 http.StatusBadRequest,
		`{"repo":"bad repo","paths":["a/.meta.json"]}`:       http.StatusBadRequest,
		`{"repo":"repo.cern.ch","paths":["a/README"]}`:       http.StatusBadRequest,
		`{"repo":"repo.cern.ch","paths":["/a/.meta.json"]}`:  http.StatusBadRequest,
		`{"repo":"repo.cern.ch","paths":["../.meta.json"]}`:  http.StatusBadRequest,
		`{"repo":"repo.cern.ch","paths":["a//.meta.json"]}`:  http.StatusBadRequest,
		`{"repo":"repo.cern.ch","paths":["./a/.meta.json"]}`: http.StatusBadRequest,
		`{"repo":"repo.cern.ch","paths":["a/.meta.json/"]}`:  http.StatusBadRequest,
		`{"repo":"repo.cern.ch","paths":[` + many + `]}`:     http.StatusRequestEntityTooLarge,
		`{"repo":"repo.cern.ch","paths":["a/.meta.json"]}`:   http.StatusNotImplemented,
	} {
		if rec := postPublishedFiles(srv, body); rec.Code != want {
			t.Errorf("%.80s: got %d, want %d (%s)", body, rec.Code, want, rec.Body.String())
		}
	}
}

// A path outside the authorized namespace is 403, before anything is read.
func TestPublishedFiles_Namespace(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Stratum0URL = "http://stratum0.test"
	asked := stubReadPublishedFiles(t, nil, nil, nil)
	srv.SetAllowedPublishPrefixes([]string{"/cvmfs/repo.cern.ch/lcg"})
	rec := postPublishedFiles(srv,
		`{"repo":"repo.cern.ch","paths":["lcg/a/.meta.json","cms/b/.meta.json"]}`)
	if rec.Code != http.StatusForbidden || len(*asked) != 0 {
		t.Fatalf("got %d (read %v), want 403 and no read", rec.Code, *asked)
	}
}

// The answer holds every path asked for once: its JSON, null when it is not
// published, null and listed in "invalid" when it is not JSON.
func TestPublishedFiles_Answer(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Stratum0URL = "http://stratum0.test"
	asked := stubReadPublishedFiles(t, map[string][]byte{
		"v/.meta.json":           []byte(`{"package":{"hash":"h1"},"members":[{"package":"ROOT"}]}`),
		"p/ROOT/.bits-view.json": []byte(`{"entries":[["bin/root","file",""]]}`),
		"p/bad/.bits-view.json":  []byte(`{not json`),
	}, []string{"p/big/.bits-view.json"}, nil)
	rec := postPublishedFiles(srv, `{"repo":"repo.cern.ch","paths":["v/.meta.json",`+
		`"p/ROOT/.bits-view.json","p/gone/.bits-view.json","p/bad/.bits-view.json","p/big/.bits-view.json","v/.meta.json"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
	}
	if len(*asked) != 5 {
		t.Errorf("read %v, want each path once", *asked)
	}
	var got struct {
		Files   map[string]json.RawMessage `json:"files"`
		Invalid []string                   `json:"invalid"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if string(got.Files["p/gone/.bits-view.json"]) != "null" ||
		string(got.Files["p/bad/.bits-view.json"]) != "null" ||
		!strings.Contains(string(got.Files["v/.meta.json"]), `"members"`) ||
		!strings.Contains(string(got.Files["p/ROOT/.bits-view.json"]), `bin/root`) {
		t.Errorf("files: %s", rec.Body.String())
	}
	if len(got.Invalid) != 2 || got.Invalid[0] != "p/bad/.bits-view.json" ||
		got.Invalid[1] != "p/big/.bits-view.json" || string(got.Files["p/big/.bits-view.json"]) != "null" {
		t.Errorf("invalid: %v", got.Invalid)
	}
}

// A failed read is 502, never a partial answer.
func TestPublishedFiles_ReadError(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Stratum0URL = "http://stratum0.test"
	stubReadPublishedFiles(t, map[string][]byte{"a/.meta.json": []byte(`{}`)}, nil, errors.New("boom"))
	if rec := postPublishedFiles(srv, `{"repo":"repo.cern.ch","paths":["a/.meta.json"]}`); rec.Code != http.StatusBadGateway {
		t.Fatalf("got %d, want 502", rec.Code)
	}
}

// Files over the total limit are 413: the producer asks in smaller batches.
func TestPublishedFiles_TooLarge(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Stratum0URL = "http://stratum0.test"
	stubReadPublishedFiles(t, nil, nil, fmt.Errorf("a/.meta.json: %w", cvmfscatalog.ErrTooLarge))
	if rec := postPublishedFiles(srv, `{"repo":"repo.cern.ch","paths":["a/.meta.json"]}`); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("got %d, want 413", rec.Code)
	}
}
