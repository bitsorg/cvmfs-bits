// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package httpsig

// The signer that runs in production is .gitlab/prepub-sign.py in the
// bits-console repository. Two implementations of one canonical form is exactly
// where a signing scheme silently stops interoperating — a different sort
// order, a different escaping rule, a stray newline — and the failure mode is
// "every publish returns 401" with neither side able to say why.
//
// These tests execute THAT FILE rather than a copy of it. A copy would drift,
// and a drifted copy still passes.
//
// The file lives in a sibling repository, so the tests skip when it is not
// checked out. Set PREPUB_SIGN_SCRIPT to point at it explicitly.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// signScript locates .gitlab/prepub-sign.py, or skips.
func signScript(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("PREPUB_SIGN_SCRIPT"); p != "" {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("PREPUB_SIGN_SCRIPT=%s: %v", p, err)
		}
		return p
	}
	// internal/httpsig -> repo root -> sibling checkout
	candidates := []string{
		filepath.Join("..", "..", "..", "bits-console", ".gitlab", "prepub-sign.py"),
		filepath.Join("..", "..", "..", "..", "bits-console", ".gitlab", "prepub-sign.py"),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	t.Skip("bits-console/.gitlab/prepub-sign.py not found; set PREPUB_SIGN_SCRIPT to run the interop tests")
	return ""
}

func requirePython(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
}

// runSigner invokes the production signer and returns the header value.
func runSigner(t *testing.T, secret, method, uri, bodyHash string, fields map[string]string) string {
	t.Helper()
	script := signScript(t)
	requirePython(t)

	args := []string{script, method, uri, bodyHash}
	for k, v := range fields {
		args = append(args, k+"="+v)
	}
	cmd := exec.Command("python3", args...)
	cmd.Env = append(os.Environ(), "PREPUB_API_TOKEN="+secret)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("signer failed: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

// TestSigner_VerifiesInGo is the end-to-end check: the production client signs,
// the server verifies, and the binding check passes.
func TestSigner_VerifiesInGo(t *testing.T) {
	const secret = "s3cr3t-token-value"
	fields := map[string]string{
		"repo":     "bits.cern.ch",
		"path":     "x86_64-el9/Packages/ROOT/6.32.02",
		"build_id": "987",
	}
	bodyHash := strings.Repeat("cd", 32)

	header := runSigner(t, secret, "POST", "/api/v1/jobs", bodyHash, fields)

	sig, err := Parse(header)
	if err != nil {
		t.Fatalf("Go cannot parse the signer's header %q: %v", header, err)
	}
	if err := sig.Verify([]byte(secret), "POST", "/api/v1/jobs", time.Now(), DefaultSkew); err != nil {
		t.Fatalf("Go rejects the signer's signature: %v", err)
	}
	if err := sig.Bound(fields, bodyHash); err != nil {
		t.Fatalf("binding check failed on a signed request: %v", err)
	}
}

// TestSigner_FieldsDigestMatches covers the encoding, which is where the two
// implementations are most likely to diverge. The awkward cases matter more
// than the typical one: they are the inputs a shell-based signer got wrong.
func TestSigner_FieldsDigestMatches(t *testing.T) {
	const secret = "k"

	cases := []struct {
		name   string
		fields map[string]string
	}{
		{"typical submission", map[string]string{
			"repo":       "bits.cern.ch",
			"path":       "x86_64-el9/Packages/ROOT/6.32.02",
			"build_id":   "123456",
			"tar_sha256": strings.Repeat("ab", 32),
		}},
		// build_id is sent unconditionally and is empty when coarse publish is off.
		{"empty value", map[string]string{"repo": "r", "build_id": ""}},
		{"no fields at all", map[string]string{}},
		// Keys whose order differs between "sort by key" and "sort by encoded
		// line" — the ambiguity the length-prefixed encoding exists to remove.
		{"key ordering edge", map[string]string{
			"a": "1", "a_b": "2", "a-b": "3", "a.b": "4", "a0": "5",
		}},
		// Separators inside values.
		{"separators in values", map[string]string{
			"path": "a=b:c", "repo": "x:1=2", "tag": "has spaces and = signs",
		}},
		// The cases a shell pipeline gets wrong: a value containing a newline
		// or a tab. tag_description can legitimately contain either.
		{"newline in value", map[string]string{
			"repo": "r", "tag_description": "line one\nline two",
		}},
		{"tab in value", map[string]string{
			"repo": "r", "tag_description": "col1\tcol2",
		}},
		{"unicode", map[string]string{"tag_description": "héllo — wörld"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			header := runSigner(t, secret, "POST", "/api/v1/jobs", NoBody, tc.fields)
			sig, err := Parse(header)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if want := FieldsDigest(tc.fields); !strings.EqualFold(sig.FieldsHash, want) {
				t.Errorf("digest mismatch\n signer: %s\n     go: %s", sig.FieldsHash, want)
			}
			if err := sig.Bound(tc.fields, NoBody); err != nil {
				t.Errorf("binding check failed: %v", err)
			}
		})
	}
}

// TestSigner_SignsTheQueryString pins the fix for the injection where an
// attacker appended query parameters to a captured request: the URI the client
// signs must include them.
func TestSigner_SignsTheQueryString(t *testing.T) {
	const secret = "k"
	fields := map[string]string{"repo": "r"}

	header := runSigner(t, secret, "POST", "/api/v1/jobs", NoBody, fields)
	sig, err := Parse(header)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// Same signature, request line rewritten with an appended query.
	if err := sig.Verify([]byte(secret), "POST", "/api/v1/jobs?finalize=true", time.Now(), DefaultSkew); err == nil {
		t.Error("a signature for /api/v1/jobs verified against /api/v1/jobs?finalize=true")
	}
}

// TestSigner_DetectsTamper confirms the interop path is not accidentally
// permissive.
func TestSigner_DetectsTamper(t *testing.T) {
	const secret = "s3cr3t-token-value"
	fields := map[string]string{"repo": "bits.cern.ch", "path": "pkg/1.0"}

	sig, err := Parse(runSigner(t, secret, "POST", "/api/v1/jobs", NoBody, fields))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := sig.Bound(map[string]string{"repo": "bits.cern.ch", "path": "pkg/9.9"}, NoBody); err == nil {
		t.Error("a changed path passed the binding check")
	}
}

// TestSigner_KeepsSecretOutOfArgv is the reason the signer is a script taking
// the secret from the environment rather than `openssl -hmac "$SECRET"`:
// argv is world-readable through /proc/<pid>/cmdline.
func TestSigner_KeepsSecretOutOfArgv(t *testing.T) {
	const secret = "s3cr3t-token-value"
	script := signScript(t)
	requirePython(t)

	cmd := exec.Command("python3", script, "POST", "/api/v1/jobs", NoBody, "repo=r")
	cmd.Env = append(os.Environ(), "PREPUB_API_TOKEN="+secret)
	for _, arg := range cmd.Args {
		if strings.Contains(arg, secret) {
			t.Fatalf("the secret appears in argv: %q", arg)
		}
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("signer failed: %v\n%s", err, out)
	}
}

// TestSigner_RefusesWithoutSecret: signing must fail loudly, never emit an
// unsigned-but-plausible header.
func TestSigner_RefusesWithoutSecret(t *testing.T) {
	script := signScript(t)
	requirePython(t)

	cmd := exec.Command("python3", script, "POST", "/api/v1/jobs", NoBody, "repo=r")
	cmd.Env = append(os.Environ(), "PREPUB_API_TOKEN=")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("signer succeeded with no secret, emitting %q", out)
	}
}

// TestNoFieldsMatchesDigest keeps the compile-time constant honest.
func TestNoFieldsMatchesDigest(t *testing.T) {
	if got := FieldsDigest(nil); got != NoFields {
		t.Errorf("NoFields = %s; FieldsDigest(nil) = %s", NoFields, got)
	}
	if got := FieldsDigest(map[string]string{}); got != NoFields {
		t.Errorf("an empty map must digest to NoFields, got %s", got)
	}
}

// TestSigner_TimestampIsCurrent guards against a signer that emits a stale or
// future timestamp, which would fail verification in a way that looks like a
// clock problem on the server.
func TestSigner_TimestampIsCurrent(t *testing.T) {
	const secret = "k"
	before := time.Now().Add(-2 * time.Second)
	sig, err := Parse(runSigner(t, secret, "GET", "/api/v1/jobs/x", NoBody, nil))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	after := time.Now().Add(2 * time.Second)
	if sig.Timestamp.Before(before) || sig.Timestamp.After(after) {
		t.Errorf("timestamp %s is not current (now %s)", sig.Timestamp, time.Now())
	}
	if _, err := strconv.ParseInt(strconv.FormatInt(sig.Timestamp.Unix(), 10), 10, 64); err != nil {
		t.Errorf("timestamp is not an integer unix time: %v", err)
	}
}
