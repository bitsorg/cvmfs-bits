// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

// Ingress for the staged-publish fields.
//
// staging_prefix names an S3 prefix a producer has already filled with prepared
// objects; catalog_hash names the subtree catalog to graft. They are one
// instruction in two fields, and a staged submission carries NO tar — its
// objects are already in the store, which is the point. Every combination the
// handler cannot act on has to be refused rather than answered with 202.

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
)

func catHash(fill string) string { return strings.Repeat(fill, 40) + "C" }

// newFieldsOnlyRequest builds a multipart submission with no tar part, which is
// the shape of a staged publish. newMultipartRequest always attaches a payload.
func newFieldsOnlyRequest(t *testing.T, fields map[string]string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			t.Fatalf("WriteField %q: %v", k, err)
		}
	}
	mw.Close()
	req := httptest.NewRequest("POST", "/api/v1/jobs", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

func TestSubmitJob_StagedFieldsIngress(t *testing.T) {
	good := catHash("a")
	const prefix = "staging/host7/job-1"

	for _, tc := range []struct {
		name       string
		fields     map[string]string
		withTar    bool
		wantCode   int
		wantMsg    string
		wantPrefix string
		wantHash   string
	}{
		{
			name: "accepted with no payload",
			fields: map[string]string{
				"publish_path": StagedPublishPath, "staging_prefix": prefix, "catalog_hash": good,
			},
			wantCode: http.StatusAccepted, wantPrefix: prefix, wantHash: good,
		},
		{
			// The objects are already in the store; a tar would publish the same
			// subtree a second way, by ingest, with no rule saying which wins.
			name: "refused when a tar is also sent",
			fields: map[string]string{
				"publish_path": StagedPublishPath, "staging_prefix": prefix, "catalog_hash": good,
			},
			withTar:  true,
			wantCode: http.StatusBadRequest, wantMsg: "must not carry a tar payload",
		},
		{
			name:     "prefix without catalog hash is refused",
			fields:   map[string]string{"publish_path": StagedPublishPath, "staging_prefix": prefix},
			wantCode: http.StatusBadRequest, wantMsg: "must be given together",
		},
		{
			name:     "catalog hash without prefix is refused",
			fields:   map[string]string{"publish_path": StagedPublishPath, "catalog_hash": good},
			wantCode: http.StatusBadRequest, wantMsg: "must be given together",
		},
		{
			name:     "refused on the default publish path",
			fields:   map[string]string{"staging_prefix": prefix, "catalog_hash": good},
			wantCode: http.StatusBadRequest, wantMsg: "staging_prefix is only supported",
		},
		{
			// A bare hash names a different CAS object than the catalog, and the
			// receiver refuses the graft outright.
			name: "unsuffixed catalog hash is refused",
			fields: map[string]string{
				"publish_path": StagedPublishPath, "staging_prefix": prefix,
				"catalog_hash": strings.Repeat("a", 40),
			},
			wantCode: http.StatusBadRequest, wantMsg: "catalog suffix",
		},
		{
			name: "non-hex catalog hash is refused",
			fields: map[string]string{
				"publish_path": StagedPublishPath, "staging_prefix": prefix,
				"catalog_hash": strings.Repeat("z", 40) + "C",
			},
			wantCode: http.StatusBadRequest, wantMsg: "catalog suffix",
		},
		{
			// A prefix that is merely wrong lists nothing and copies nothing
			// without erroring, so it has to be refused here.
			name: "traversal in the prefix is refused",
			fields: map[string]string{
				"publish_path": StagedPublishPath, "staging_prefix": "staging/../../etc",
				"catalog_hash": good,
			},
			wantCode: http.StatusBadRequest, wantMsg: "staging_prefix must be",
		},
		{
			name: "prefix ending in data is refused",
			fields: map[string]string{
				"publish_path": StagedPublishPath, "staging_prefix": "staging/host7/data",
				"catalog_hash": good,
			},
			wantCode: http.StatusBadRequest, wantMsg: "staging_prefix must be",
		},
		{
			name: "oversized prefix is refused",
			fields: map[string]string{
				"publish_path": StagedPublishPath, "staging_prefix": strings.Repeat("a", 129),
				"catalog_hash": good,
			},
			wantCode: http.StatusBadRequest, wantMsg: "staging_prefix must be",
		},
		{
			// direct_s3 wants publish_path "ingest"; a staged job wants "staged".
			// The refusal names the path, which is the real conflict.
			name: "direct_s3 with staging_prefix is refused",
			fields: map[string]string{
				"publish_path": StagedPublishPath, "staging_prefix": prefix,
				"catalog_hash": good, "direct_s3": "true",
			},
			wantCode: http.StatusBadRequest, wantMsg: "direct_s3 is only supported",
		},
		{
			name: "object_list with staging_prefix is refused",
			fields: map[string]string{
				"publish_path": StagedPublishPath, "staging_prefix": prefix,
				"catalog_hash": good, "object_list": "true",
			},
			wantCode: http.StatusBadRequest, wantMsg: "object_list is only supported",
		},
		{
			// The dangerous direction: the staged backend reads no tar, so this
			// was accepted, the payload discarded, an empty transaction committed,
			// and the job reported "published".
			name:     "the staged path with a tar and no prefix is refused",
			fields:   map[string]string{"publish_path": StagedPublishPath},
			withTar:  true,
			wantCode: http.StatusBadRequest, wantMsg: "must not carry a tar payload",
		},
		{
			name:     "the staged path without a prefix is refused",
			fields:   map[string]string{"publish_path": StagedPublishPath},
			wantCode: http.StatusBadRequest, wantMsg: "requires staging_prefix",
		},
		{
			name:     "absent leaves the ordinary ingest path alone",
			fields:   map[string]string{"publish_path": "ingest"},
			withTar:  true,
			wantCode: http.StatusAccepted,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, sp, orch := newTestServer(t)
			orch.Lease = &noopBackend{}
			orch.PublishPaths = map[string]lease.Backend{
				"ingest": &altBackend{}, StagedPublishPath: &altBackend{},
			}

			f := map[string]string{
				"repo": "software.cern.ch",
				"path": "x86_64-el9/pkg/1.0",
			}
			for k, v := range tc.fields {
				f[k] = v
			}
			rec := httptest.NewRecorder()
			if tc.withTar {
				srv.submitJob(rec, newMultipartRequest(t, f, []byte("dummy")))
			} else {
				srv.submitJob(rec, newFieldsOnlyRequest(t, f))
			}

			if rec.Code != tc.wantCode {
				t.Fatalf("want %d, got %d: %s", tc.wantCode, rec.Code, rec.Body.String())
			}
			if tc.wantMsg != "" && !strings.Contains(rec.Body.String(), tc.wantMsg) {
				t.Errorf("error should mention %q, got %s", tc.wantMsg, rec.Body.String())
			}

			if tc.wantCode == http.StatusAccepted {
				// Assert the fields actually REACHED the Job. Checking only the
				// status code leaves the assignment deletable — replacing
				// j.ObjectList = objectList with _ = objectList once kept the
				// whole repo's tests green.
				var body struct {
					JobID string `json:"job_id"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("cannot read job id from %s: %v", rec.Body.String(), err)
				}
				j, err := sp.FindJob(body.JobID)
				if err != nil {
					t.Fatalf("job %s not in the spool: %v", body.JobID, err)
				}
				if j.StagingPrefix != tc.wantPrefix {
					t.Errorf("j.StagingPrefix = %q, want %q", j.StagingPrefix, tc.wantPrefix)
				}
				if j.CatalogHash != tc.wantHash {
					t.Errorf("j.CatalogHash = %q, want %q", j.CatalogHash, tc.wantHash)
				}
			}
			if tc.wantCode == http.StatusBadRequest {
				if p := findSpooledTar(t, sp.Root); p != "" {
					t.Errorf("rejected submission left a payload behind: %s", p)
				}
			}
		})
	}
}

// The JSON submission mode is for a tar already on the server's filesystem and
// requires tar_path, so a staged publish cannot be expressed there. The fields
// must be refused by name rather than ignored — silently dropping them would
// answer 202 for an ordinary tar publish.
func TestSubmitJob_StagedFieldsRefusedInJSONMode(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{
		"ingest": &altBackend{}, StagedPublishPath: &altBackend{},
	}

	body := `{"repo":"software.cern.ch","path":"x/1.0","publish_path":"staged",` +
		`"staging_prefix":"staging/host7/job-1","catalog_hash":"` + catHash("a") + `"}`
	req := httptest.NewRequest("POST", "/api/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "multipart submissions") {
		t.Errorf("error should say the fields are multipart-only, got: %s", rec.Body.String())
	}
}

func TestValidCatalogHash(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{strings.Repeat("a", 40) + "C", true},  // SHA-1 plus the catalog suffix
		{strings.Repeat("a", 40), false},       // bare: names a different object
		{strings.Repeat("a", 40) + "P", false}, // partial-chunk suffix, not a catalog
		{strings.Repeat("a", 40) + "c", false}, // the suffix is case-sensitive
		{strings.Repeat("z", 40) + "C", false}, // not hex
		{strings.Repeat("A", 40) + "C", false}, // CVMFS hashes are lower-case
		{strings.Repeat("a", 39) + "C", false}, // one short
		{strings.Repeat("a", 41) + "C", false}, // one long
		// Wider algorithms render as "<hex>-rmd160" / "<hex>-shake128" and this
		// stack computes SHA-1 only, so neither a 47- nor a 49-hex string is a
		// hash it can produce or resolve.
		{strings.Repeat("0", 49) + "C", false},
		{"C", false},
		{"", false},
	} {
		if got := job.ValidCatalogHash(tc.in); got != tc.want {
			t.Errorf("ValidCatalogHash(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestValidStagingPrefix(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"staging/host7/job-1", true},
		{"staging", true},
		{"a.b_c-d/e", true},
		{"", false},
		{"/staging/host7", false},         // leading slash
		{"staging/host7/", false},         // trailing slash
		{"staging//host7", false},         // empty segment
		{"staging/../etc", false},         // traversal
		{"staging/./host7", false},        // dot segment
		{"staging/host7/data", false},     // promotion appends /data/ itself
		{"staging/host 7", false},         // space
		{"staging/host7?x=1", false},      // query-ish
		{"staging/hôte", false},           // non-ASCII
		{strings.Repeat("a", 128), true},  // at the limit
		{strings.Repeat("a", 129), false}, // over it
		{"data", false},                   // single segment named data
		{"staging/data/host7", true},      // only the LAST segment is special
	} {
		if got := job.ValidStagingPrefix(tc.in); got != tc.want {
			t.Errorf("ValidStagingPrefix(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// Blocking only the staged FIELDS in JSON mode leaves the staged PATH usable
// with a tar_path, which the staged backend cannot read.
func TestSubmitJob_StagedPathRefusedInJSONMode(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{StagedPublishPath: &altBackend{}}

	body := `{"repo":"software.cern.ch","path":"x/1.0","publish_path":"` +
		StagedPublishPath + `","tar_path":"/tmp/x.tar","tar_sha256":"` +
		strings.Repeat("a", 64) + `"}`
	req := httptest.NewRequest("POST", "/api/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "multipart submissions") {
		t.Errorf("error should say the path is multipart-only, got: %s", rec.Body.String())
	}
}
