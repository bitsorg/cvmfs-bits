// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package lease

import (
	"strings"
	"testing"
)

func objectListArgv(t *testing.T, directS3, objectList bool) string {
	t.Helper()
	b := &IngestBackend{}
	return strings.Join(
		b.commitArgs("test.cvmfs.io", "base", "/tmp/p.tar", directS3, objectList), " ")
}

// The acceptance criterion for this feature: with the flag off, argv is what it
// was before the feature existed. TestIngestBackend_CommitArgsPutRepoLast
// covers the same ground with expectations written before this change and
// still passing unmodified, which is the stronger statement of the two.
func TestCommitArgs_InertWithoutObjectList(t *testing.T) {
	if got := objectListArgv(t, false, false); strings.Contains(got, "object-list") {
		t.Errorf("object-list leaked into the default argv: %s", got)
	}
	got := objectListArgv(t, true, false)
	if !strings.Contains(got, "--direct-s3") {
		t.Errorf("direct-s3 argv changed: %s", got)
	}
	if strings.Contains(got, "object-list") {
		t.Errorf("direct-s3 alone must not add object-list: %s", got)
	}
}

// cvmfs_server aborts the transaction when given --object-list without
// --direct-s3, so prepub must never emit that combination: the publish would
// fail on an argument prepub chose, not on anything the caller asked for.
func TestCommitArgs_ObjectListRequiresDirectS3(t *testing.T) {
	if got := objectListArgv(t, false, true); strings.Contains(got, "object-list") {
		t.Errorf("emitted --object-list without --direct-s3: %s", got)
	}
}

// cvmfs_server's option loop runs `while [ "$2" != "" ]` and then takes $1 as
// the repository, so anything appended after the repository is consumed AS the
// repository. Adding a flag at the wrong end of this slice is silent until the
// publish dies in load_repo_config.
func TestCommitArgs_RepoStaysLast(t *testing.T) {
	for _, tc := range []struct{ directS3, objectList bool }{
		{false, false}, {true, false}, {false, true}, {true, true},
	} {
		got := objectListArgv(t, tc.directS3, tc.objectList)
		fields := strings.Fields(got)
		if fields[len(fields)-1] != "test.cvmfs.io" {
			t.Errorf("direct_s3=%v object_list=%v: repo is not last: %s",
				tc.directS3, tc.objectList, got)
		}
	}
}

// The path must be the inherited pipe, not a file or a FIFO: anything else
// either blocks on open while the lease is held, or silently writes a list
// nobody reads.
func TestCommitArgs_ObjectListUsesTheInheritedPipe(t *testing.T) {
	// Literal, not objectListChildPath(): asserting with the same function
	// commitArgs uses makes the fd number unfalsifiable — changing it to 4
	// left this test passing. ExtraFiles[0] IS fd 3, so 3 is the contract.
	got := objectListArgv(t, true, true)
	if !strings.Contains(got, "--object-list /proc/self/fd/3") {
		t.Errorf("object-list path is not the inherited pipe: %s", got)
	}
}
