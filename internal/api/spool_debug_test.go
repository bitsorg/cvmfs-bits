package api

import (
	"os"
	"path/filepath"
	"testing"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/spool"
	"cvmfs.io/prepub/pkg/observe"
)

func TestSpoolDebug(t *testing.T) {
	dir := t.TempDir()
	obs, shutdown, _ := observe.New("test")
	defer shutdown()
	sp, err := spool.New(dir, obs)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("spool root: %s", sp.Root)

	j := job.NewJob("job-test", "repo.cern.ch", "", "")
	j.State = job.StateStaging
	if err := sp.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	t.Logf("wrote manifest, j.State=%s", j.State)

	// Show what files exist
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil {
			t.Logf("  %s", path)
		}
		return nil
	})

	j2, err := sp.FindJob("job-test")
	t.Logf("FindJob: j2=%+v err=%v", j2, err)
}
