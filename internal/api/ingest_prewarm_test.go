// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/distribute/manifest"
	"cvmfs.io/prepub/internal/distribute/serve"
	"cvmfs.io/prepub/internal/lease"
)

// confirmBackend is an ingest stand-in that reports two stored objects.
type confirmBackend struct{ altBackend }

func (c *confirmBackend) Commit(_ context.Context, req lease.CommitRequest) error {
	if req.ConfirmedObjects != nil {
		*req.ConfirmedObjects = []string{"abcdef", "123456P"}
	}
	return nil
}

// An ingest job with direct_s3 + object_list that asks for prewarm stores a pull
// manifest of the confirmed objects after its commit, but only on a node that
// makes pre-warming available.
func TestRun_IngestObjectListPreWarm(t *testing.T) {
	for _, prewarm := range []bool{true, false} {
		o, sp := minimalOrch(t, &noopBackend{})
		o.PublishPaths = map[string]lease.Backend{"ingest": &confirmBackend{}}
		store := serve.NewMemManifestStore()
		o.Manifests = store
		o.PullObjectBaseURL = "http://publisher:8080"
		o.PreWarm = prewarm

		j := newIncomingJob(t, sp)
		yes := true
		j.PublishPath, j.DirectS3, j.ObjectList, j.PreWarm = "ingest", true, true, &yes
		if err := sp.WriteManifest(j); err != nil {
			t.Fatal(err)
		}
		if err := o.Run(context.Background(), j, nil); err != nil {
			t.Fatalf("prewarm=%v: run: %v", prewarm, err)
		}
		// The manifest is stored off the job's path, so wait for it briefly.
		var (
			m   *manifest.Manifest
			ok  bool
			err error
		)
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if m, ok, err = store.Manifest(context.Background(), j.ID); err != nil || ok {
				break
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		if ok != prewarm {
			t.Fatalf("prewarm=%v: manifest stored = %v", prewarm, ok)
		}
		if !ok {
			continue
		}
		if len(m.Objects) != 2 || m.Objects[1].Hash != "123456P" {
			t.Errorf("objects = %+v", m.Objects)
		}
		if want := "http://publisher:8080/cvmfs/" + j.Repo + "/data"; m.BaseURLs[0] != want {
			t.Errorf("base url = %q, want %q", m.BaseURLs[0], want)
		}
	}
}
