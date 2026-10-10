// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/lease"
)

// tarPathBackend captures the TarPath the orchestrator hands to a backend and
// records whether that file actually existed at the moment Commit ran.
//
// NeedsPipeline() is false — the same shape as IngestBackend and LocalBackend,
// and the reason this bug existed: the orchestrator refreshed TarPath only
// inside the pipeline branch.
type tarPathBackend struct {
	noopBackend
	mu      sync.Mutex
	gotPath string
	existed bool
	called  bool
}

func (b *tarPathBackend) Commit(_ context.Context, req lease.CommitRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.called = true
	b.gotPath = req.TarPath
	if req.TarPath != "" {
		_, err := os.Stat(req.TarPath)
		b.existed = err == nil
	}
	return nil
}

func (b *tarPathBackend) snapshot() (string, bool, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gotPath, b.existed, b.called
}

// TestIngestPath_TarPathResolvesToAnExistingFile is the regression test for a
// publish that failed on the testbed with
//
//	cvmfs_server ingest -T /data/spool/leased/<job>/payload.tar
//	Impossible to open the archive: Failed to open '...'
//
// The spool renames a job's directory on every state transition, so an absolute
// path captured earlier goes stale as soon as the job advances. The
// orchestrator refreshed TarPath after the incoming->staging rename, but only
// inside `if o.leaseFor(j).NeedsPipeline()`. IngestBackend returns false there,
// so an ingest job never ran that code and carried a stale path all the way to
// cvmfs_server — which had already opened a gateway transaction by then, making
// it look like a corrupt payload rather than a wrong path.
//
// Asserting os.Stat rather than a string shape is deliberate: it is the
// property that actually matters, and it stays true if the spool layout
// changes.
func TestIngestPath_TarPathResolvesToAnExistingFile(t *testing.T) {
	srv, _, orch := newTestServer(t)
	be := &tarPathBackend{}
	orch.PublishPaths = map[string]lease.Backend{"ingest": be}

	submitOne(t, srv, map[string]string{
		"repo":         "software.cern.ch",
		"path":         "x86_64-el9/pkg/1.0",
		"publish_path": "ingest",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	got, existed, called := be.snapshot()
	if !called {
		t.Fatal("ingest backend Commit was never called — the job did not reach commit")
	}
	if got == "" {
		t.Fatal("ingest backend received an empty TarPath")
	}
	if !existed {
		t.Errorf("TarPath handed to the ingest backend does not exist: %q\n"+
			"the job directory is renamed on each state transition, so TarPath "+
			"must be re-derived from the job's CURRENT directory before commit, "+
			"not only inside the NeedsPipeline() branch", got)
	}
}
