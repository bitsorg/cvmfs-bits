// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package httpsig

// Interop with the OTHER Python signer: bits_helpers/httpsig.py in the bits
// repository, used by `bits publish`.
//
// There are now three implementations of one canonical form — this package,
// bits-console's .gitlab/prepub-sign.py, and bits_helpers/httpsig.py — and the
// failure mode when they diverge is a 401 that neither side can explain. The
// shell version this replaced got the field encoding wrong in three separate
// ways (locale collation, sort key, byte-vs-character lengths), and only a
// test that ran the real client caught it.
//
// So this executes bits_helpers/httpsig.py itself. It skips when the bits repo
// is not checked out alongside; set BITS_REPO to point at it.

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// bitsHelpersDir locates the bits repository's package directory, or skips.
func bitsHelpersDir(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("BITS_REPO"); p != "" {
		d := filepath.Join(p, "bits_helpers")
		if _, err := os.Stat(filepath.Join(d, "httpsig.py")); err != nil {
			t.Fatalf("BITS_REPO=%s: %v", p, err)
		}
		return p
	}
	for _, c := range []string{
		filepath.Join("..", "..", "..", "bits"),
		filepath.Join("..", "..", "..", "..", "bits"),
	} {
		if _, err := os.Stat(filepath.Join(c, "bits_helpers", "httpsig.py")); err == nil {
			return c
		}
	}
	t.Skip("bits/bits_helpers/httpsig.py not found; set BITS_REPO to run this interop test")
	return ""
}

// signWithBitsHelpers calls bits_helpers.httpsig.sign and returns the header.
func signWithBitsHelpers(t *testing.T, secret, method, uri string, fields map[string]string, bodyHash string) string {
	t.Helper()
	repo := bitsHelpersDir(t)
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}

	// Field pairs are passed as argv and reassembled, so a value containing a
	// newline survives intact — which is one of the cases under test.
	args := []string{"-c", `
import sys, json
sys.path.insert(0, sys.argv[1])
from bits_helpers import httpsig
fields = json.loads(sys.argv[5])
sys.stdout.write(httpsig.sign(sys.argv[2], sys.argv[3], sys.argv[4], fields, sys.argv[6]))
`, repo, secret, method, uri}

	blob, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal fields: %v", err)
	}
	args = append(args, string(blob), bodyHash)

	cmd := exec.Command("python3", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bits_helpers signer failed: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestBitsHelpers_SignatureVerifiesInGo(t *testing.T) {
	const secret = "shared-secret-value"
	fields := map[string]string{
		"repo":       "bits.cern.ch",
		"path":       "alice/x86_64-el9/O2/daily-20260730",
		"tar_sha256": strings.Repeat("9f", 32),
	}
	bodyHash := strings.Repeat("9f", 32)

	sig, err := Parse(signWithBitsHelpers(t, secret, "POST", "/api/v1/jobs", fields, bodyHash))
	if err != nil {
		t.Fatalf("Go cannot parse the client's header: %v", err)
	}
	if err := sig.Verify([]byte(secret), "POST", "/api/v1/jobs", time.Now(), DefaultSkew); err != nil {
		t.Fatalf("Go rejects the client's signature: %v", err)
	}
	if err := sig.Bound(fields, bodyHash); err != nil {
		t.Fatalf("binding check failed: %v", err)
	}
}

// TestBitsHelpers_FieldsDigestMatches covers the encoding. The unicode and
// newline cases are the ones that have actually broken a client before.
func TestBitsHelpers_FieldsDigestMatches(t *testing.T) {
	const secret = "k"

	for _, tc := range []struct {
		name   string
		fields map[string]string
	}{
		{"typical submission", map[string]string{
			"repo": "bits.cern.ch", "path": "alice/pkg/1.0",
			"tar_sha256": strings.Repeat("ab", 32),
		}},
		{"webhook included", map[string]string{
			"repo": "r", "path": "p", "tar_sha256": "x",
			"webhook_url": "https://example.org/hook?a=b&c=d",
		}},
		{"no fields", map[string]string{}},
		{"unicode value", map[string]string{"path": "héllo — wörld"}},
		{"newline in value", map[string]string{"path": "line one\nline two"}},
		{"tab in value", map[string]string{"path": "col1\tcol2"}},
		{"separators", map[string]string{"path": "a=b:c", "repo": "x:1=2"}},
		{"key ordering edge", map[string]string{
			"a": "1", "a_b": "2", "a-b": "3", "a.b": "4", "a0": "5",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sig, err := Parse(signWithBitsHelpers(t, secret, "POST", "/api/v1/jobs", tc.fields, NoBody))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if want := FieldsDigest(tc.fields); !strings.EqualFold(sig.FieldsHash, want) {
				t.Errorf("digest mismatch\n client: %s\n     go: %s", sig.FieldsHash, want)
			}
			if err := sig.Bound(tc.fields, NoBody); err != nil {
				t.Errorf("binding check failed: %v", err)
			}
		})
	}
}

// TestBitsHelpers_PollSignatureVerifies covers the GET path, whose URI carries
// the job id — so every poll needs its own signature.
func TestBitsHelpers_PollSignatureVerifies(t *testing.T) {
	const secret = "k"
	uri := "/api/v1/jobs/2f1c9a44-0000-4000-8000-000000000000"

	sig, err := Parse(signWithBitsHelpers(t, secret, "GET", uri, map[string]string{}, NoBody))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := sig.Verify([]byte(secret), "GET", uri, time.Now(), DefaultSkew); err != nil {
		t.Fatalf("Go rejects the poll signature: %v", err)
	}
	// A bodyless request signs the empty field set, which the server's
	// non-multipart binding requires.
	if !strings.EqualFold(sig.FieldsHash, NoFields) {
		t.Errorf("a bodyless GET must sign the empty field set, got fd=%s", sig.FieldsHash)
	}
	// The same signature must not verify for a different job.
	other := "/api/v1/jobs/00000000-0000-4000-8000-000000000000"
	if err := sig.Verify([]byte(secret), "GET", other, time.Now(), DefaultSkew); err == nil {
		t.Error("a poll signature verified against a different job id")
	}
}

// TestBitsHelpers_ConstantsMatch guards the shared constants from drifting.
func TestBitsHelpers_ConstantsMatch(t *testing.T) {
	repo := bitsHelpersDir(t)
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	cmd := exec.Command("python3", "-c", `
import sys
sys.path.insert(0, sys.argv[1])
from bits_helpers import httpsig
print(httpsig.HEADER_NAME)
print(httpsig.SCHEME)
print(httpsig.CANONICAL_PREFIX)
print(httpsig.NO_BODY)
print(httpsig.NO_FIELDS)
`, repo)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reading client constants: %v\n%s", err, out)
	}
	got := strings.Split(strings.TrimSpace(string(out)), "\n")
	want := []string{HeaderName, Scheme, canonicalPrefix, NoBody, NoFields}
	if len(got) != len(want) {
		t.Fatalf("expected %d constants, got %v", len(want), got)
	}
	names := []string{"HEADER_NAME", "SCHEME", "CANONICAL_PREFIX", "NO_BODY", "NO_FIELDS"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: client %q, server %q", names[i], got[i], want[i])
		}
	}
}
