// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package measure

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAppendAndRead_RoundTrip(t *testing.T) {
	w, err := NewWriter(t.TempDir())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	objs := 11
	tar := int64(4096)
	in := Record{
		BuildID: "15540757", JobID: "j1", Repo: "test.cvmfs.io",
		Path:        "el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3",
		PublishPath: "ingest", Outcome: "published",
		TotalS: 1.25, BackendS: Secs(531 * time.Millisecond),
		Objects: &objs, ObjectsExact: true, TarBytes: &tar,
	}
	if err := w.Append(in); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, err := w.Read("15540757")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 record, got %d", len(got))
	}
	if got[0].JobID != "j1" || got[0].PublishPath != "ingest" {
		t.Errorf("identity lost: %+v", got[0])
	}
	if got[0].BackendS == nil || *got[0].BackendS != 0.531 {
		t.Errorf("backend_s = %v, want 0.531", got[0].BackendS)
	}
	if got[0].Objects == nil || *got[0].Objects != 11 {
		t.Errorf("objects = %v, want 11", got[0].Objects)
	}
}

// "Not measured" must not become 0. This is the whole reason the counts are
// pointers: the ingest path reported objects=0 for real publishes, and a
// record that repeats that lie is worse than no record.
func TestUnmeasuredCountsAreAbsentNotZero(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir)
	if err := w.Append(Record{BuildID: "b", JobID: "j", Outcome: "published"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	blob, _ := os.ReadFile(filepath.Join(dir, "b.ndjson"))
	var raw map[string]any
	if err := json.Unmarshal(blob, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{"objects", "tar_bytes", "backend_s", "bytes_raw"} {
		if _, present := raw[k]; present {
			t.Errorf("%q was written for an unmeasured value: %v", k, raw[k])
		}
	}
	// A measured zero, by contrast, must survive.
	zero := 0
	w2, _ := NewWriter(dir)
	if err := w2.Append(Record{BuildID: "b2", JobID: "j", Objects: &zero}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	blob2, _ := os.ReadFile(filepath.Join(dir, "b2.ndjson"))
	if !strings.Contains(string(blob2), `"objects":0`) {
		t.Errorf("a measured zero was dropped: %s", blob2)
	}
}

func TestFileFor_GroupsByBuildAndFallsBackToDate(t *testing.T) {
	w, _ := NewWriter(t.TempDir())
	ts := time.Date(2026, 8, 15, 17, 47, 0, 0, time.UTC)
	if got := filepath.Base(w.FileFor("15540757", ts)); got != "15540757.ndjson" {
		t.Errorf("build file = %q", got)
	}
	// No build id (the per-package paths do not need a coarse build): still
	// grouped, by day, rather than piled into one unbounded file.
	if got := filepath.Base(w.FileFor("", ts)); got != "nobuild-20260815.ndjson" {
		t.Errorf("fallback file = %q", got)
	}
}

// The build id arrives over the API, so it is untrusted input used to build a
// path. NEGATIVE CONTROL: drop the sanitiser and this escapes the directory.
func TestFileFor_RefusesToEscapeTheDirectory(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir)
	for _, evil := range []string{"../../etc/passwd", "a/b", "..", "x\x00y"} {
		got := w.FileFor(evil, time.Now())
		if filepath.Dir(got) != dir {
			t.Errorf("build id %q escaped to %q", evil, got)
		}
	}
}

func TestAppend_ConcurrentWritesDoNotInterleave(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_ = w.Append(Record{
				BuildID: "b", JobID: "job", Outcome: "failed",
				Error: strings.Repeat("x", 500), // > PIPE_BUF once encoded
			})
		}(i)
	}
	wg.Wait()
	recs, err := w.Read("b")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 50 {
		t.Fatalf("want 50 intact records, got %d", len(recs))
	}
}

// A service killed mid-write leaves a partial last line. It must cost that
// one record, not the whole run.
func TestRead_SkipsAMalformedTrailingLine(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir)
	_ = w.Append(Record{BuildID: "b", JobID: "good", Outcome: "published"})
	f, _ := os.OpenFile(filepath.Join(dir, "b.ndjson"), os.O_APPEND|os.O_WRONLY, 0o644)
	_, _ = f.WriteString(`{"job_id":"trunc","rep`)
	f.Close()

	recs, err := w.Read("b")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 1 || recs[0].JobID != "good" {
		t.Errorf("want the one intact record, got %+v", recs)
	}
}

func TestNilWriterIsUsable(t *testing.T) {
	var w *Writer
	if err := w.Append(Record{JobID: "j"}); err != nil {
		t.Errorf("nil writer Append: %v", err)
	}
	if _, err := w.Builds(); err != nil {
		t.Errorf("nil writer Builds: %v", err)
	}
}

func TestSummarise_MatchesTheNumbersAMeasurementSectionQuotes(t *testing.T) {
	base := time.Date(2026, 8, 15, 12, 15, 40, 0, time.UTC)
	var recs []Record
	// 169 fast publishes and one 65.9 s outlier -- the shape of the real
	// GEANT4 run in §24, where the tail is the story.
	for i := 0; i < 169; i++ {
		recs = append(recs, Record{
			BuildID: "b", Repo: "test.cvmfs.io", PublishPath: "ingest",
			Outcome: "published", Timestamp: base.Add(time.Duration(i) * time.Second),
			TotalS: 1.0, BackendS: Secs(500 * time.Millisecond),
		})
	}
	recs = append(recs, Record{
		BuildID: "b", Repo: "test.cvmfs.io", PublishPath: "ingest",
		Outcome: "published", Timestamp: base.Add(169 * time.Second),
		TotalS: 66.0, BackendS: Secs(65925 * time.Millisecond),
	})

	s := Summarise(recs)
	if s.Jobs != 170 || s.Published != 170 || s.Failed != 0 {
		t.Errorf("counts: jobs=%d published=%d failed=%d", s.Jobs, s.Published, s.Failed)
	}
	if s.Backend.Max != 65.925 {
		t.Errorf("max = %v, want 65.925 (the number a histogram cannot give)", s.Backend.Max)
	}
	if s.Backend.Median != 0.5 {
		t.Errorf("median = %v, want 0.5", s.Backend.Median)
	}
	if s.PublishPaths["ingest"] != 170 {
		t.Errorf("publish path breakdown = %v", s.PublishPaths)
	}
	// Serialised sum vs window is the ratio §24 reports.
	if s.Backend.Sum != round3(169*0.5+65.925) {
		t.Errorf("sum = %v", s.Backend.Sum)
	}
}

func TestSummarise_CountsConflictsAndReplacements(t *testing.T) {
	recs := []Record{
		{Outcome: "published", PublishPath: "ingest", Conflicted: true, Replaced: true},
		{Outcome: "published", PublishPath: "ingest"},
		{Outcome: "failed", PublishPath: "ingest", Conflicted: true},
	}
	s := Summarise(recs)
	if s.Conflicted != 2 || s.Replaced != 1 || s.Failed != 1 {
		t.Errorf("conflicted=%d replaced=%d failed=%d", s.Conflicted, s.Replaced, s.Failed)
	}
}

// A run where some records counted objects and others did not must not be
// reported as if the partial total were the run's total.
func TestSummarise_FlagsAPartialObjectCount(t *testing.T) {
	n := 5
	s := Summarise([]Record{{Objects: &n, Outcome: "published"}, {Outcome: "published"}})
	if s.Objects != 5 || !s.ObjectsPartial {
		t.Errorf("objects=%d partial=%v, want 5/true", s.Objects, s.ObjectsPartial)
	}
}

// The failure this whole feature exists to explain is a swissknife crash, and
// its discriminator sits AFTER the banner and the argv block. A head-only cut
// kept "PANIC" and dropped "UNIQUE constraint failed" — measured against the
// real 2026-08-15 error, where those sit at offsets 525 and 804 of 1586.
//
// NEGATIVE CONTROL: restore a head-only truncation at 600 bytes and this fails.
func TestAppend_TruncationKeepsTheCauseNotJustTheBanner(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir)
	realErr := "cvmfs_server ingest into \"el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3\": exit status 1 (output: " +
		strings.Repeat("Info: transaction on repository test.cvmfs.io\n", 12) +
		"terminate called after throwing an instance of 'ECvmfsException'\n" +
		"  what():  PANIC: cvmfs/catalog_rw.cc : 168\n" +
		strings.Repeat("cvmfs_swissknife ingest -u /cvmfs/test.cvmfs.io -c /var/spool/... ", 30) +
		"UNIQUE constraint failed: catalog.md5path_1, catalog.md5path_2\nAborted (core dumped)"
	if len(realErr) < 1500 {
		t.Fatalf("fixture too short (%d) to exercise truncation", len(realErr))
	}
	if err := w.Append(Record{BuildID: "b", JobID: "j", Outcome: "failed", Error: realErr}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	recs, _ := w.Read("b")
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	if !strings.Contains(recs[0].Error, "UNIQUE constraint failed") {
		t.Errorf("truncation dropped the cause:\n%s", recs[0].Error)
	}
	if !strings.Contains(recs[0].Error, "cvmfs_server ingest into") {
		t.Errorf("truncation dropped the head:\n%s", recs[0].Error)
	}
}

// A build id longer than the filename limit was written under a truncated
// name and read back under the full one, so its records 404'd while existing.
//
// NEGATIVE CONTROL: make Read use the raw id again and this fails.
func TestLongBuildID_IsWrittenAndReadUnderTheSameName(t *testing.T) {
	w, _ := NewWriter(t.TempDir())
	long := strings.Repeat("a", 70)
	if err := w.Append(Record{BuildID: long, JobID: "j", Outcome: "published"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	recs, err := w.Read(long)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("records written under a truncated name were unreadable: %d", len(recs))
	}
}

// The spool gets cleaned while the service runs; without a retry every later
// publish silently loses its record until restart.
func TestAppend_RecreatesADeletedDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "measurements")
	w, _ := NewWriter(dir)
	_ = w.Append(Record{BuildID: "b", JobID: "j1"})
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if err := w.Append(Record{BuildID: "b", JobID: "j2"}); err != nil {
		t.Fatalf("Append after the directory was removed: %v", err)
	}
	recs, err := w.Read("b")
	if err != nil || len(recs) != 1 || recs[0].JobID != "j2" {
		t.Errorf("want the post-delete record, got %+v (err %v)", recs, err)
	}
}

// WindowS must span the earliest SUBMISSION, not the earliest terminal
// record: a long job that starts first finishes last, and the old "lead"
// heuristic silently understated the run.
//
// NEGATIVE CONTROL: restore lead-from-the-first-record and this reports 90.
func TestSummarise_WindowSpansTheEarliestSubmission(t *testing.T) {
	base := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	recs := []Record{
		// submitted at t=10, terminal at t=11
		{Outcome: "published", Timestamp: base.Add(11 * time.Second), TotalS: 1},
		// submitted at t=0, terminal at t=100 — the real start of the run
		{Outcome: "published", Timestamp: base.Add(100 * time.Second), TotalS: 100},
	}
	if got := Summarise(recs).WindowS; got != 100 {
		t.Errorf("window = %v, want 100", got)
	}
}

// A count that came from a truncated object list is a lower bound and must be
// flagged as partial, even though every record carried a number.
func TestSummarise_InexactCountIsPartial(t *testing.T) {
	n := 7
	s := Summarise([]Record{{Outcome: "published", Objects: &n, ObjectsExact: false}})
	if !s.ObjectsPartial {
		t.Errorf("an inexact count was reported as the run total: %+v", s)
	}
}

// A healthy coarse build is 170 packages parked in StateAccumulated plus one
// finalize that published. Bucketing "not published" as failure reported that
// as 171 failures — a successful build looking like a total loss.
//
// NEGATIVE CONTROL: fold the incomplete bucket back into Failed and this
// fails with published=0 failed=171.
func TestSummarise_AccumulatedJobsAreNotFailures(t *testing.T) {
	var recs []Record
	for i := 0; i < 170; i++ {
		recs = append(recs, Record{Outcome: IncompletePrefix + "accumulated",
			PublishPath: "prepub", TotalS: 1})
	}
	recs = append(recs, Record{Outcome: "published", PublishPath: "prepub", TotalS: 5})

	s := Summarise(recs)
	if s.Published != 1 || s.Failed != 0 || s.Incomplete != 170 {
		t.Errorf("published=%d failed=%d incomplete=%d, want 1/0/170",
			s.Published, s.Failed, s.Incomplete)
	}
	if s.Jobs != 171 {
		t.Errorf("jobs = %d, want 171", s.Jobs)
	}
}

// An inexact count must be visible in the JSON, not implied by an absent key.
func TestObjectsExactIsEmittedEvenWhenFalse(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir)
	n := 7
	_ = w.Append(Record{BuildID: "b", JobID: "j", Objects: &n, ObjectsExact: false})
	blob, _ := os.ReadFile(filepath.Join(dir, "b.ndjson"))
	if !strings.Contains(string(blob), `"objects_exact":false`) {
		t.Errorf("an inexact count was not marked as such: %s", blob)
	}
}
