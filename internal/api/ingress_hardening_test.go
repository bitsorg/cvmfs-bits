// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/httpsig"
	"cvmfs.io/prepub/internal/job"
)

var badRepoNames = []string{"..", "a..b", ".hidden", "repo.", "-repo", "re po", strings.Repeat("a", 61)}

// jsonSubmit builds a tar_path submission whose tar is valid, so a 400 can only
// come from the extra fields under test.
func jsonSubmit(t *testing.T, srv *Server, repo, extra string) *http.Request {
	t.Helper()
	tar := []byte("tar-content")
	f, err := os.CreateTemp(srv.stagingRoot, "t-*.tar")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	name := f.Name()
	if err := os.WriteFile(name, tar, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(tar)
	body := `{"repo":"` + repo + `","path":"p","tar_path":"` + name + `","tar_sha256":"` +
		hex.EncodeToString(sum[:]) + `"` + extra + `}`
	req := httptest.NewRequest("POST", "/api/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

// TestSubmitJob_RejectsInvalidRepoName: a name that is not a CVMFS repository
// name (notably one containing "..") is refused with 400 on every ingress.
func TestSubmitJob_RejectsInvalidRepoName(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	for _, repo := range badRepoNames {
		rec := httptest.NewRecorder()
		srv.submitJob(rec, newMultipartRequest(t, map[string]string{"repo": repo}, []byte("tar")))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("multipart repo %q: got %d, want 400", repo, rec.Code)
		}

		rec = httptest.NewRecorder()
		srv.submitJob(rec, jsonSubmit(t, srv, repo, ""))
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "repo") {
			t.Errorf("json repo %q: got %d %s, want 400", repo, rec.Code, rec.Body.String())
		}

		body := `{"repo":"` + repo + `","path":"a"}`
		rec = httptest.NewRecorder()
		srv.reserveHandler(rec, httptest.NewRequest("POST", "/api/v1/reserve", strings.NewReader(body)))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("reserve repo %q: got %d, want 400", repo, rec.Code)
		}
		rec = httptest.NewRecorder()
		srv.publishedHandler(rec, httptest.NewRequest("POST", "/api/v1/published", strings.NewReader(body)))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("published repo %q: got %d, want 400", repo, rec.Code)
		}
	}
}

// TestSubmitJob_WebhookURLMustBeHTTP: only absolute http(s) URLs are accepted.
func TestSubmitJob_WebhookURLMustBeHTTP(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	for _, u := range []string{"file:///etc/passwd", "ftp://host/x", "/relative", "http://", "gopher://h", "::bad"} {
		rec := httptest.NewRecorder()
		srv.submitJob(rec, newMultipartRequest(t, map[string]string{
			"repo": "software.cern.ch", "webhook_url": u,
		}, []byte("tar")))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("multipart webhook %q: got %d, want 400", u, rec.Code)
		}

		rec = httptest.NewRecorder()
		srv.submitJob(rec, jsonSubmit(t, srv, "software.cern.ch", `,"webhook_url":"`+u+`"`))
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "webhook_url") {
			t.Errorf("json webhook %q: got %d %s, want 400", u, rec.Code, rec.Body.String())
		}
	}
	rec := httptest.NewRecorder()
	srv.submitJob(rec, newMultipartRequest(t, map[string]string{
		"repo": "software.cern.ch", "webhook_url": "https://hooks.example/x",
	}, []byte("tar")))
	if rec.Code != http.StatusAccepted {
		t.Errorf("multipart https webhook: got %d, want 202 (%s)", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	srv.submitJob(rec, jsonSubmit(t, srv, "software.cern.ch", `,"webhook_url":"http://hooks.example/x"`))
	if rec.Code != http.StatusAccepted {
		t.Errorf("json http webhook: got %d, want 202 (%s)", rec.Code, rec.Body.String())
	}
}

// TestJobLog_RedactsSecrets: the log endpoint must not return the gateway lease
// token or the secret part of a webhook URL.
func TestJobLog_RedactsSecrets(t *testing.T) {
	srv, sp, _ := newTestServer(t)
	j := job.NewJob("job-log", "repo.cern.ch", "", "")
	j.State = job.StateLeased
	j.LeaseToken = "secret-lease-token"
	j.WebhookURL = "https://hooks.example/services/T0/B0/SECRETPART?token=qsecret"
	if err := sp.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	req := withMuxVars(httptest.NewRequest("GET", "/api/v1/jobs/job-log/log", nil), map[string]string{"id": "job-log"})
	rec := httptest.NewRecorder()
	srv.jobLogHandler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, secret := range []string{"secret-lease-token", "SECRETPART", "qsecret"} {
		if strings.Contains(body, secret) {
			t.Errorf("log response leaks %q: %s", secret, body)
		}
	}
	if !strings.Contains(body, "hooks.example") {
		t.Errorf("webhook host should stay visible: %s", body)
	}
}

// TestMountRevoke_BehindAPIAuth: the API revoke and unrevoke routes are
// reachable only with valid API credentials, each reaching its own handler.
func TestMountRevoke_BehindAPIAuth(t *testing.T) {
	srv, _ := authTestServer(t, AuthHMAC)
	reached := map[string]int{}
	handler := func(name string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reached[name]++
			w.WriteHeader(http.StatusOK)
		})
	}
	if !srv.MountRevoke(handler("revoke"), handler("unrevoke")) {
		t.Fatal("not mounted")
	}
	body := []byte(`{"node":"stratum1-a"}`)
	serve := func(r *http.Request) int {
		rec := httptest.NewRecorder()
		srv.router.ServeHTTP(rec, r)
		return rec.Code
	}

	for path, name := range map[string]string{RevokePath: "revoke", UnrevokePath: "unrevoke"} {
		if code := serve(httptest.NewRequest("POST", path, bytes.NewReader(body))); code != http.StatusUnauthorized || reached[name] != 0 {
			t.Fatalf("unauthenticated %s: code %d reached %d", name, code, reached[name])
		}
		req := httptest.NewRequest("POST", path, bytes.NewReader(body))
		req.Header.Set(httpsig.HeaderName, httpsig.Sign([]byte(testToken), SigningKeyID, "POST", path,
			httpsig.NoFields, httpsig.BodyDigest(body), time.Now(), randomNonce(t)))
		if code := serve(req); code != http.StatusOK || reached[name] != 1 {
			t.Fatalf("signed %s: code %d reached %v", name, code, reached)
		}
	}
}

// TestMountRevoke_NotInDevMode: with an empty API token (auth off) the revoke
// routes are not mounted at all.
func TestMountRevoke_NotInDevMode(t *testing.T) {
	srv, _ := authTestServer(t, AuthHMAC)
	srv.apiToken = ""
	if srv.MountRevoke(http.NotFoundHandler(), http.NotFoundHandler()) {
		t.Fatal("revoke mounted with auth off")
	}
	for _, path := range []string{RevokePath, UnrevokePath} {
		rec := httptest.NewRecorder()
		srv.router.ServeHTTP(rec, httptest.NewRequest("POST", path, strings.NewReader(`{"node":"x"}`)))
		if rec.Code == http.StatusOK {
			t.Errorf("%s reachable in dev mode: %d", path, rec.Code)
		}
	}
}
