// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cvmfs.io/prepub/internal/lease"
)

// object_list ingress. The list is produced only by the direct-S3 uploader,
// and cvmfs_server ABORTS the transaction when given --object-list without
// --direct-s3, so both mismatches must be refused. Accepting either would hand
// back a 202 for a request that cannot be honoured.
//
// Driven through submitJob rather than by re-deriving the rules in the test —
// a table that recomputes the conditions proves only that the test agrees with
// itself.
func TestSubmitJob_ObjectListIngress(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fields   map[string]string
		wantCode int
		wantMsg  string
		wantSet  bool // j.ObjectList after a successful submit
	}{
		{
			name: "accepted with ingest and direct_s3",
			fields: map[string]string{
				"publish_path": "ingest", "direct_s3": "true", "object_list": "true",
			},
			wantCode: http.StatusAccepted, wantSet: true,
		},
		{
			// No direct_s3 here: with it, the PRE-EXISTING direct_s3 refusal
			// fires first and this row would never reach the object_list
			// check it exists to test.
			name: "refused on the default publish path",
			fields: map[string]string{
				"object_list": "true",
			},
			// The message must name object_list specifically: the pre-existing
			// direct_s3 refusal also fires on this input and also says
			// "publish path", so a looser assertion passes even when the
			// object_list check is deleted.
			wantCode: http.StatusBadRequest, wantMsg: "object_list is only supported",
		},
		{
			name: "refused without direct_s3",
			fields: map[string]string{
				"publish_path": "ingest", "object_list": "true",
			},
			wantCode: http.StatusBadRequest, wantMsg: "requires direct_s3",
		},
		{
			name: "a typo fails loudly rather than meaning false",
			fields: map[string]string{
				"publish_path": "ingest", "direct_s3": "true", "object_list": "yes-please",
			},
			wantCode: http.StatusBadRequest, wantMsg: "must be a boolean",
		},
		{
			name: "absent leaves direct_s3 alone",
			fields: map[string]string{
				"publish_path": "ingest", "direct_s3": "true",
			},
			wantCode: http.StatusAccepted, wantSet: false,
		},
		{
			// Inertness, self-contained: the default publish path with no
			// object_list field at all must still be accepted and must leave
			// the flag false. Without this row, defaulting objectList to true
			// passes this whole file.
			name:     "absent on the default path",
			fields:   map[string]string{},
			wantCode: http.StatusAccepted, wantSet: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, sp, orch := newTestServer(t)
			orch.Lease = &noopBackend{}
			orch.PublishPaths = map[string]lease.Backend{"ingest": &altBackend{}}

			f := map[string]string{
				"repo": "software.cern.ch",
				"path": "x86_64-el9/pkg/1.0",
			}
			for k, v := range tc.fields {
				f[k] = v
			}
			rec := httptest.NewRecorder()
			srv.submitJob(rec, newMultipartRequest(t, f, []byte("dummy")))

			if rec.Code != tc.wantCode {
				t.Fatalf("want %d, got %d: %s", tc.wantCode, rec.Code, rec.Body.String())
			}
			if tc.wantMsg != "" && !strings.Contains(rec.Body.String(), tc.wantMsg) {
				t.Errorf("error should mention %q, got %s", tc.wantMsg, rec.Body.String())
			}
			if tc.wantCode == http.StatusAccepted {
				// Assert the flag actually REACHED the Job. Checking only the
				// status code leaves the assignment deletable: replacing
				// j.ObjectList = objectList with _ = objectList kept the whole
				// repo's tests green.
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
				if j.ObjectList != tc.wantSet {
					t.Errorf("j.ObjectList = %v, want %v", j.ObjectList, tc.wantSet)
				}
			}
			if tc.wantCode == http.StatusBadRequest {
				// A refused submission must not leave the payload in the spool.
				if p := findSpooledTar(t, sp.Root); p != "" {
					t.Errorf("rejected submission left a payload behind: %s", p)
				}
			}
		})
	}
}
