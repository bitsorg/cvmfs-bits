// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

// Tests for request authentication. The properties worth protecting are the
// ones whose absence is invisible: a signature that verifies while the body was
// swapped, a captured request that can be replayed, or a "strict" mode that
// still accepts the credential it was meant to retire.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/httpsig"
)

const testToken = "test-shared-secret"

// signedRequest builds a multipart submission carrying a valid signature.
func signedRequest(t *testing.T, fields map[string]string, tarContent []byte, mutate func(*http.Request)) *http.Request {
	t.Helper()

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			t.Fatalf("WriteField: %v", err)
		}
	}
	bodyHash := httpsig.NoBody
	if tarContent != nil {
		sum := sha256.Sum256(tarContent)
		bodyHash = hex.EncodeToString(sum[:])
		fw, err := mw.CreateFormFile("tar", "payload.tar")
		if err != nil {
			t.Fatalf("CreateFormFile: %v", err)
		}
		if _, err := fw.Write(tarContent); err != nil {
			t.Fatalf("write tar: %v", err)
		}
	}
	mw.Close()

	req := httptest.NewRequest("POST", "/api/v1/jobs", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set(httpsig.HeaderName, httpsig.Sign(
		[]byte(testToken), "prepub", "POST", "/api/v1/jobs",
		httpsig.FieldsDigest(fields), bodyHash, time.Now(), randomNonce(t)))

	if mutate != nil {
		mutate(req)
	}
	return req
}

var nonceCounter int

func randomNonce(t *testing.T) string {
	t.Helper()
	nonceCounter++
	return hex.EncodeToString([]byte(t.Name())) + "-" + hex.EncodeToString([]byte{byte(nonceCounter)})
}

// authTestServer returns a server with auth enabled and a backend that accepts
// everything, so only the auth outcome is under test.
func authTestServer(t *testing.T, mode AuthMode) (*Server, *Orchestrator) {
	t.Helper()
	srv, _, orch := newTestServer(t)
	srv.apiToken = testToken
	srv.SetAuthMode(mode)
	orch.Lease = &noopBackend{}
	return srv, orch
}

// serveThroughAuth runs a request through the auth middleware, recording
// whether the wrapped handler was reached.
func serveThroughAuth(srv *Server, r *http.Request) (*httptest.ResponseRecorder, bool) {
	reached := false
	h := srv.requireAuth(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec, reached
}

func TestAuth_SignedRequestAccepted(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)
	fields := map[string]string{"repo": "software.cern.ch", "path": "p/1"}

	_, reached := serveThroughAuth(srv, signedRequest(t, fields, []byte("tar"), nil))
	if !reached {
		t.Error("a validly signed request was rejected")
	}
}

func TestAuth_BearerAcceptedOnlyWhenAllowed(t *testing.T) {
	for _, tc := range []struct {
		mode AuthMode
		want bool
	}{
		{AuthBearer, true},
		{AuthBoth, true},
		{AuthHMAC, false}, // the whole point: the token stops travelling
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			srv, _ := authTestServer(t, tc.mode)
			req := httptest.NewRequest("POST", "/api/v1/jobs", nil)
			req.Header.Set("Authorization", "Bearer "+testToken)

			rec, reached := serveThroughAuth(srv, req)
			if reached != tc.want {
				t.Errorf("mode %s: reached=%v want %v (%s)", tc.mode, reached, tc.want, rec.Body.String())
			}
		})
	}
}

func TestAuth_SignedRejectedInBearerOnlyMode(t *testing.T) {
	srv, _ := authTestServer(t, AuthBearer)
	_, reached := serveThroughAuth(srv, signedRequest(t, map[string]string{"repo": "r"}, nil, nil))
	if reached {
		t.Error("a signed request was accepted by a bearer-only deployment")
	}
}

func TestAuth_WrongSecretRejected(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)
	fields := map[string]string{"repo": "r"}
	req := signedRequest(t, fields, nil, func(r *http.Request) {
		r.Header.Set(httpsig.HeaderName, httpsig.Sign(
			[]byte("not-the-secret"), "prepub", "POST", "/api/v1/jobs",
			httpsig.FieldsDigest(fields), httpsig.NoBody, time.Now(), "deadbeef"))
	})
	if _, reached := serveThroughAuth(srv, req); reached {
		t.Error("a signature made with the wrong secret was accepted")
	}
}

// TestAuth_ReplayRejected is the property a bearer token cannot have: capturing
// a valid request does not let you repeat it.
func TestAuth_ReplayRejected(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)
	// A bodyless, non-multipart request binds its whole body, so it signs the
	// empty field set — see bindNonStreamingBody.
	header := httpsig.Sign([]byte(testToken), "prepub", "POST", "/api/v1/jobs",
		httpsig.NoFields, httpsig.NoBody, time.Now(), "fixed-nonce-1")

	build := func() *http.Request {
		r := httptest.NewRequest("POST", "/api/v1/jobs", nil)
		r.Header.Set(httpsig.HeaderName, header)
		return r
	}

	if _, reached := serveThroughAuth(srv, build()); !reached {
		t.Fatal("first use of a signature was rejected")
	}
	if _, reached := serveThroughAuth(srv, build()); reached {
		t.Error("the same signature was accepted twice — replay is possible")
	}
}

func TestAuth_ExpiredSignatureRejected(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)
	fields := map[string]string{"repo": "r"}

	for _, offset := range []time.Duration{-time.Hour, time.Hour} {
		req := httptest.NewRequest("POST", "/api/v1/jobs", nil)
		req.Header.Set(httpsig.HeaderName, httpsig.Sign(
			[]byte(testToken), "prepub", "POST", "/api/v1/jobs",
			httpsig.FieldsDigest(fields), httpsig.NoBody,
			time.Now().Add(offset), "nonce-"+offset.String()))

		if _, reached := serveThroughAuth(srv, req); reached {
			t.Errorf("a signature %s from now was accepted", offset)
		}
	}
}

// TestAuth_SignatureIsPathBound stops a signature for one endpoint being
// replayed against another — e.g. a job submission reused to seal a build.
func TestAuth_SignatureIsPathBound(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)
	fields := map[string]string{"repo": "r"}

	req := httptest.NewRequest("POST", "/api/v1/builds/x/seal", nil)
	req.Header.Set(httpsig.HeaderName, httpsig.Sign(
		[]byte(testToken), "prepub", "POST", "/api/v1/jobs",
		httpsig.FieldsDigest(fields), httpsig.NoBody, time.Now(), "path-bound"))

	if _, reached := serveThroughAuth(srv, req); reached {
		t.Error("a signature for /jobs was accepted on /builds/{id}/seal")
	}
}

func TestAuth_MethodBound(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)
	fields := map[string]string{"repo": "r"}

	req := httptest.NewRequest("DELETE", "/api/v1/jobs", nil)
	req.Header.Set(httpsig.HeaderName, httpsig.Sign(
		[]byte(testToken), "prepub", "POST", "/api/v1/jobs",
		httpsig.FieldsDigest(fields), httpsig.NoBody, time.Now(), "method-bound"))

	if _, reached := serveThroughAuth(srv, req); reached {
		t.Error("a POST signature was accepted for a DELETE")
	}
}

func TestAuth_UnknownKeyIDRejected(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)
	fields := map[string]string{"repo": "r"}

	req := httptest.NewRequest("POST", "/api/v1/jobs", nil)
	req.Header.Set(httpsig.HeaderName, httpsig.Sign(
		[]byte(testToken), "someone-else", "POST", "/api/v1/jobs",
		httpsig.FieldsDigest(fields), httpsig.NoBody, time.Now(), "unknown-key"))

	if _, reached := serveThroughAuth(srv, req); reached {
		t.Error("a signature with an unknown key_id was accepted")
	}
}

// TestSubmitJob_SignedTamperedFieldRejected is the test that gives the scheme
// its meaning: the MAC verified, but a form field was changed in flight, so the
// request is not the one that was signed.
func TestSubmitJob_SignedTamperedFieldRejected(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)

	// Sign for one path, then submit a different one.
	signedFields := map[string]string{"repo": "software.cern.ch", "path": "pkg/1.0"}
	content := []byte("payload")
	sum := sha256.Sum256(content)
	sigHeader := httpsig.Sign([]byte(testToken), "prepub", "POST", "/api/v1/jobs",
		httpsig.FieldsDigest(signedFields), hex.EncodeToString(sum[:]), time.Now(), "tamper-1")

	req := newMultipartRequest(t, map[string]string{
		"repo":       "software.cern.ch",
		"path":       "pkg/9.9", // changed after signing
		"tar_sha256": hex.EncodeToString(sum[:]),
	}, content)
	req.Header.Set(httpsig.HeaderName, sigHeader)

	// Through the ROUTER, not submitJob directly: the binding check depends on
	// the middleware having verified and stashed the signature, so a test that
	// called the handler on its own would pass while proving nothing.
	rec := httptest.NewRecorder()
	srv.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 for a tampered field, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestSubmitJob_SignedTamperedPayloadRejected covers the other half: the fields
// are untouched but the tar was swapped.
func TestSubmitJob_SignedTamperedPayloadRejected(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)

	original := []byte("the payload that was signed")
	sum := sha256.Sum256(original)
	fields := map[string]string{
		"repo":       "software.cern.ch",
		"path":       "pkg/1.0",
		"tar_sha256": hex.EncodeToString(sum[:]),
	}
	sigHeader := httpsig.Sign([]byte(testToken), "prepub", "POST", "/api/v1/jobs",
		httpsig.FieldsDigest(fields), hex.EncodeToString(sum[:]), time.Now(), "tamper-2")

	req := newMultipartRequest(t, fields, []byte("a completely different payload"))
	req.Header.Set(httpsig.HeaderName, sigHeader)

	rec := httptest.NewRecorder()
	srv.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized && rec.Code != http.StatusBadRequest {
		t.Fatalf("want the swapped payload rejected, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestSubmitJob_SignedWithoutDigestRejected: a signature that commits to no
// payload digest attests to nothing about the tar, so it must not be accepted
// as if it did.
func TestSubmitJob_SignedWithoutDigestRejected(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)

	content := []byte("payload")
	sum := sha256.Sum256(content)
	fields := map[string]string{"repo": "software.cern.ch", "path": "pkg/1.0"}
	// bh is the real payload hash, so Bound() passes, but tar_sha256 is absent.
	sigHeader := httpsig.Sign([]byte(testToken), "prepub", "POST", "/api/v1/jobs",
		httpsig.FieldsDigest(fields), hex.EncodeToString(sum[:]), time.Now(), "nodigest-1")

	req := newMultipartRequest(t, fields, content)
	req.Header.Set(httpsig.HeaderName, sigHeader)

	rec := httptest.NewRecorder()
	srv.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "tar_sha256") {
		t.Errorf("the error should name the missing field: %s", rec.Body.String())
	}
}

func TestParseAuthMode(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    AuthMode
		wantErr bool
	}{
		{"", AuthBoth, false}, // unset must not silently become the strict mode
		{"both", AuthBoth, false},
		{"BEARER", AuthBearer, false},
		{" hmac ", AuthHMAC, false},
		{"none", "", true},
		{"off", "", true},
	} {
		got, err := ParseAuthMode(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseAuthMode(%q) accepted an unknown mode", tc.in)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("ParseAuthMode(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}
}

// TestAuth_EmptyBodyCannotGainAPayload: a signature that says "no body" must
// not verify against a request that carries one. The convenience of accepting
// the marker for a GET is only safe because of this.
func TestAuth_EmptyBodyCannotGainAPayload(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)

	header := httpsig.Sign([]byte(testToken), "prepub", "POST", "/api/v1/reserve",
		httpsig.NoFields, httpsig.NoBody, time.Now(), "gain-payload-1")

	req := httptest.NewRequest("POST", "/api/v1/reserve",
		strings.NewReader(`{"repo":"attacker","path":"x"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(httpsig.HeaderName, header)

	if _, reached := serveThroughAuth(srv, req); reached {
		t.Error("a body was attached to a signature that committed to none")
	}
}

// TestAuth_JSONBodyIsBound covers the route that has no per-handler check: the
// middleware binds it, so /reserve and friends cannot be rewritten in flight.
func TestAuth_JSONBodyIsBound(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)

	signedBody := `{"repo":"software.cern.ch","path":"pkg/1.0"}`
	header := httpsig.Sign([]byte(testToken), "prepub", "POST", "/api/v1/reserve",
		httpsig.NoFields, httpsig.BodyDigest([]byte(signedBody)), time.Now(), "json-bound-1")

	// Same signature, different body.
	req := httptest.NewRequest("POST", "/api/v1/reserve",
		strings.NewReader(`{"repo":"other.cern.ch","path":"pkg/1.0"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(httpsig.HeaderName, header)

	if _, reached := serveThroughAuth(srv, req); reached {
		t.Error("a rewritten JSON body passed the binding check")
	}

	// The signed body is accepted, and the handler still sees it.
	req2 := httptest.NewRequest("POST", "/api/v1/reserve", strings.NewReader(signedBody))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set(httpsig.HeaderName, httpsig.Sign([]byte(testToken), "prepub",
		"POST", "/api/v1/reserve", httpsig.NoFields,
		httpsig.BodyDigest([]byte(signedBody)), time.Now(), "json-bound-2"))

	var seen string
	h := srv.requireAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seen = string(b)
	}))
	h.ServeHTTP(httptest.NewRecorder(), req2)
	if seen != signedBody {
		t.Errorf("the handler must still see the body; got %q", seen)
	}
}

// TestAuth_QueryStringIsSigned pins the fix for the injection where a captured
// request was replayed with query parameters appended.
func TestAuth_QueryStringIsSigned(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)
	fields := map[string]string{"repo": "software.cern.ch"}

	header := httpsig.Sign([]byte(testToken), "prepub", "POST", "/api/v1/jobs",
		httpsig.FieldsDigest(fields), httpsig.NoBody, time.Now(), "query-inject-1")

	req := httptest.NewRequest("POST", "/api/v1/jobs?finalize=true&webhook_url=http://attacker/x", nil)
	req.Header.Set(httpsig.HeaderName, header)

	if _, reached := serveThroughAuth(srv, req); reached {
		t.Error("query parameters were appended to a signed request and still verified")
	}
}

// TestAuth_ContentTypeCannotDisableBinding pins the fix for a real bypass.
//
// The middleware used to skip its body binding for any request whose
// Content-Type began with "multipart/", on the theory that such a request was
// the streamed upload and would bind itself in submitJob. Content-Type is
// client-supplied and is NOT part of the canonical string, so it verifies no
// matter what it says — and only submitJob ever binds. Rewriting the header on
// a captured request to any other route therefore left the body completely
// unauthenticated while the MAC still verified.
//
// The exemption now comes from the method and path, which the server decides.
func TestAuth_ContentTypeCannotDisableBinding(t *testing.T) {
	for _, route := range []string{
		"/api/v1/reserve",
		"/api/v1/builds/b-1/seal",
		"/api/v1/builds/b-1/finalize",
	} {
		t.Run(route, func(t *testing.T) {
			srv, _ := authTestServer(t, AuthBoth)

			signedBody := `{"repo":"software.cern.ch","path":"pkg/1.0","expect":1}`
			header := httpsig.Sign([]byte(testToken), "prepub", "POST", route,
				httpsig.NoFields, httpsig.BodyDigest([]byte(signedBody)),
				time.Now(), randomNonce(t))

			// The attacker keeps the MAC, swaps the body, and claims multipart.
			req := httptest.NewRequest("POST", route,
				strings.NewReader(`{"repo":"attacker.cern.ch","path":"pkg/1.0","expect":99}`))
			req.Header.Set("Content-Type", "multipart/form-data; boundary=x")
			req.Header.Set(httpsig.HeaderName, header)

			rec, reached := serveThroughAuth(srv, req)
			if reached {
				t.Fatalf("a rewritten body reached the handler by claiming to be multipart (status %d)", rec.Code)
			}
		})
	}
}

// TestAuth_MultipartOnlyExemptOnTheSubmitRoute is the other half: the exemption
// must still apply where it is needed, and must not apply anywhere else — not
// even for the same path under a different method.
func TestAuth_MultipartOnlyExemptOnTheSubmitRoute(t *testing.T) {
	for _, tc := range []struct {
		method, path string
		want         bool
	}{
		{"POST", "/api/v1/jobs", true},
		{"GET", "/api/v1/jobs", false},
		{"PUT", "/api/v1/jobs", false},
		{"POST", "/api/v1/jobs/", false},
		{"POST", "/api/v1/jobs/abc", false},
		{"POST", "/api/v1/reserve", false},
		{"POST", "/api/v1/builds/b/seal", false},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		if got := isStreamingRoute(r); got != tc.want {
			t.Errorf("isStreamingRoute(%s %s) = %v, want %v", tc.method, tc.path, got, tc.want)
		}
	}
}

// TestAuth_JSONSubmissionIsBound covers the branch that shares the streaming
// route: a tar_path submission is JSON, so the middleware defers to the
// handler — which must bind it, or the exemption becomes the bypass again.
func TestAuth_JSONSubmissionIsBound(t *testing.T) {
	srv, _ := authTestServer(t, AuthBoth)
	srv.stagingRoot = t.TempDir()

	signedBody := `{"repo":"software.cern.ch","path":"pkg/1.0","tar_path":"ok.tar"}`
	header := httpsig.Sign([]byte(testToken), "prepub", "POST", "/api/v1/jobs",
		httpsig.NoFields, httpsig.BodyDigest([]byte(signedBody)), time.Now(), randomNonce(t))

	req := httptest.NewRequest("POST", "/api/v1/jobs",
		strings.NewReader(`{"repo":"software.cern.ch","path":"pkg/1.0","tar_path":"../../etc/passwd"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(httpsig.HeaderName, header)

	rec := httptest.NewRecorder()
	srv.requireAuth(http.HandlerFunc(srv.submitJob)).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("a rewritten tar_path submission returned %d, want 401\n%s", rec.Code, rec.Body.String())
	}
}
