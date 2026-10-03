// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"testing"

	"cvmfs.io/prepub/internal/cas"
)

// The staged path is offered only with a CAS that can promote a staging
// prefix (S3); a local-disk CAS cannot, so the path is not advertised and a
// staged submission gets the ordinary "publish path not configured" 400.
func TestCanPromote(t *testing.T) {
	lfs, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if CanPromote(lfs) {
		t.Error("a localfs CAS was reported able to serve the staged path")
	}
	if CanPromote(&plainCAS{}) {
		t.Error("a CAS without PromoteFrom was reported able to serve the staged path")
	}
	if !CanPromote(&fakeCAS{}) {
		t.Error("a promoting CAS was not recognised")
	}
	if CanPromote(nil) {
		t.Error("no CAS was reported able to serve the staged path")
	}
}
