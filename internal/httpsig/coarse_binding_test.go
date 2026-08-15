// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package httpsig

import "testing"

// The producer signs a field LIST and sends a form; Bound compares a digest of
// the whole received set against the signed one. So adding a field to the POST
// without adding it to the signed list — or vice versa — 401s every publish.
//
// This pins that contract for `coarse`, which was added to both sides at once
// (bits-console .gitlab/cvmfs-prepub-publish.yml). The three curl variants
// there each expand ${_opt_fields[@]}; if one ever stops doing so, the shape
// below is what breaks.
func TestFieldsDigest_CoarseMustBeOnBothSides(t *testing.T) {
	base := map[string]string{
		"repo": "test.cvmfs.io", "path": "el9/Packages/x/1.0",
		"build_id": "15541355", "tar_sha256": "abc",
		"publish_path": "ingest", "direct_s3": "true",
	}
	with := func(extra map[string]string) map[string]string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}

	signed := with(map[string]string{"coarse": "false"})
	sent := with(map[string]string{"coarse": "false"})
	if FieldsDigest(signed) != FieldsDigest(sent) {
		t.Error("identical field sets must produce the same digest")
	}
	if FieldsDigest(signed) == FieldsDigest(base) {
		t.Error("a form that omits coarse must NOT match a signature that signed it")
	}
	if FieldsDigest(signed) == FieldsDigest(with(map[string]string{"coarse": "true"})) {
		t.Error("coarse=true must not satisfy a signature made for coarse=false")
	}
}
