// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeClock advances only when told to.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }
func newFakeTimeline() (*timeline, *fakeClock) {
	c := &fakeClock{t: time.Unix(1000, 0)}
	return newTimelineAt(c.now), c
}

// A line is stamped with when its first byte and its newline arrived, however
// it was split into writes; blank lines and carriage returns are dropped, and
// the output is kept byte for byte.
func TestTimeline_StampsLinesWhenTheyComplete(t *testing.T) {
	tl, clock := newFakeTimeline()
	clock.add(1 * time.Second)
	fmt.Fprint(tl, "opening transac")
	clock.add(2 * time.Second)
	fmt.Fprint(tl, "tion\r\n\n  \n")
	clock.add(4 * time.Second)
	fmt.Fprint(tl, "ingesting\nno newline")

	got := tl.Lines()
	want := []timedLine{{1 * time.Second, 3 * time.Second, "opening transaction"},
		{7 * time.Second, 7 * time.Second, "ingesting"}, {7 * time.Second, 7 * time.Second, "no newline"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("lines = %v, want %v", got, want)
	}
	if out := tl.Output(); out != "opening transaction\r\n\n  \ningesting\nno newline" {
		t.Errorf("output not kept byte for byte: %q", out)
	}
	if s := tl.String(); s != "+1.0s..+3.0s opening transaction | +7.0s ingesting | +7.0s no newline" {
		t.Errorf("String() = %q", s)
	}
}

// A verbose run is cut in the middle, keeping where it started and ended.
func TestTimeline_RendersHeadAndTail(t *testing.T) {
	tl, _ := newFakeTimeline()
	n := timelineHead + timelineTail + 5
	for i := 0; i < n; i++ {
		fmt.Fprintf(tl, "line %d\n", i)
	}
	fmt.Fprintf(tl, "%s\n", strings.Repeat("x", timelineLineMax+50))
	s := tl.String()
	for _, want := range []string{"line 0 |", fmt.Sprintf("line %d |", timelineHead-1),
		"… 6 lines …", fmt.Sprintf("line %d |", n-1), strings.Repeat("x", timelineLineMax) + "…"} {
		if !strings.Contains(s, want) {
			t.Errorf("rendering lacks %q", want)
		}
	}
	if strings.Contains(s, fmt.Sprintf("line %d |", timelineHead)) {
		t.Error("a middle line was rendered")
	}
}

// Every publish logs its timeline -- the output lines with when they arrived
// -- at Info, and a failed one at Warn, with or without the object list; and
// the ancestors step is timed.
//
// NEGATIVE CONTROL: drop the logTimeline call in Commit and every case fails
// with no record; drop the Stats.Ancestors assignment and every case fails.
func TestCommit_LogsTheTimeline(t *testing.T) {
	for _, tc := range []struct {
		name       string
		exit       int
		objectList bool
		wantLevel  slog.Level
	}{
		{"published", 0, false, slog.LevelInfo},
		{"failed", 3, false, slog.LevelWarn},
		{"published with object list", 0, true, slog.LevelInfo},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubCvmfsServer(t, fmt.Sprintf("echo 'Info: opening'\nsleep 0.3\necho 'Swissknife Ingest: done' >&2\nexit %d", tc.exit))
			obs, logs := captureObs(t)
			repo := "test.cvmfs.io"
			b, mount := newAncestorBackend(t, repo)
			b.obs = obs
			base := filepath.Join(mount, repo, "pkg")
			if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
				t.Fatal(err)
			}
			var stats PublishStats
			err := b.Commit(context.Background(), CommitRequest{
				Token: repo, TarPath: oneEntryTar(t, t.TempDir()), CVMFSDir: base, Stats: &stats,
				DirectS3: tc.objectList, ObjectList: tc.objectList,
			})
			if (err != nil) != (tc.exit != 0) {
				t.Fatalf("Commit error = %v", err)
			}

			var rec *slog.Record
			logs.mu.Lock()
			for i := range logs.records {
				if logs.records[i].Message == "ingest backend: timeline" {
					rec = &logs.records[i]
				}
			}
			logs.mu.Unlock()
			if rec == nil {
				t.Fatal("no timeline record")
			}
			if rec.Level != tc.wantLevel {
				t.Errorf("level = %v, want %v", rec.Level, tc.wantLevel)
			}
			var line string
			rec.Attrs(func(a slog.Attr) bool {
				if a.Key == "timeline" {
					line = a.Value.String()
				}
				return true
			})
			parts := strings.Split(line, " | ")
			if len(parts) != 2 || !strings.HasSuffix(parts[0], "Info: opening") ||
				!strings.HasSuffix(parts[1], "Swissknife Ingest: done") {
				t.Fatalf("timeline = %q", line)
			}
			var at float64
			if _, err := fmt.Sscanf(parts[1], "+%fs", &at); err != nil || at < 0.25 {
				t.Errorf("second line stamped at %v (%v), want >= 0.25s", at, err)
			}
			if stats.Ancestors <= 0 {
				t.Error("ancestors step not timed")
			}
		})
	}
}
