// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package measure records one structured line per publish, so the numbers
// that go into a comparison table are read rather than reconstructed.
//
// Why not the Prometheus metrics prepub already exports: those are scraped on
// a 15 s interval into histogram buckets, which is right for dashboards and
// wrong for "median 0.536 s, max 65.925 s". A histogram cannot return a
// maximum at all, and the exponential buckets in use (0.1 s x 2^n) put a
// 0.54 s median in the 0.8 s bucket. Metrics stay for trends; these records
// are the measurement.
//
// Why not the service log: the numbers are there, but only as prose. Every
// figure in MEASUREMENTS.md so far was recovered by grepping multi-thousand
// line logs with ad-hoc regexes, which is slow, easy to get subtly wrong, and
// impossible once the containers are recreated. One JSON object per publish
// makes the same extraction a jq one-liner.
//
// A record is written for every terminal outcome, success and failure alike:
// a run where 170 publishes failed is exactly as interesting as one where
// they succeeded, and it was the failures that needed measuring most.
package measure

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Record is one publish, as measured. Optional numbers are pointers so that
// "not measured" survives the round trip: a 0 that means "nobody counted"
// is indistinguishable from a real zero once written, and that ambiguity has
// already cost a week of believing the ingest path published nothing.
type Record struct {
	// ── identity ──
	Timestamp   time.Time `json:"ts"`
	BuildID     string    `json:"build_id,omitempty"`
	JobID       string    `json:"job_id"`
	Repo        string    `json:"repo"`
	Path        string    `json:"path"`
	PublishPath string    `json:"publish_path"` // ingest | staged | prepub
	// Outcome is "published", "failed", or "incomplete:<state>" for a job
	// that reached neither -- a package accumulated against a coarse build
	// being the normal case.
	Outcome string `json:"outcome"`

	// ── timings, seconds ──
	// Total is submission to terminal state: the number a run's wall clock is
	// made of. The phases are the parts of it prepub can attribute; they do
	// not necessarily sum to Total (queueing and spool moves sit between).
	TotalS    float64  `json:"total_s"`
	QueuedS   *float64 `json:"queued_s,omitempty"`   // accepted -> work started
	CommitS   *float64 `json:"commit_s,omitempty"`   // orchestrator commit phase
	BackendS  *float64 `json:"backend_s,omitempty"`  // the publish tool itself
	PipelineS *float64 `json:"pipeline_s,omitempty"` // chunk/compress/upload

	// ── volume ──
	TarBytes *int64 `json:"tar_bytes,omitempty"`
	Objects  *int   `json:"objects,omitempty"`
	// ObjectsExact is emitted even when false: absence would be
	// indistinguishable from "exact", and an inexact count is precisely what a
	// reader must not quote as the total.
	ObjectsExact    bool   `json:"objects_exact"`
	BytesRaw        *int64 `json:"bytes_raw,omitempty"`
	BytesCompressed *int64 `json:"bytes_compressed,omitempty"`

	// ── conflict remediation (ADR-0011 D17 / replace_on_conflict) ──
	Conflicted bool `json:"conflicted,omitempty"`
	Replaced   bool `json:"replaced,omitempty"`

	// Error is the terminal error, truncated. Present only on failure.
	Error string `json:"error,omitempty"`
}

// IncompletePrefix marks an outcome that is neither success nor failure; the
// job's state follows it.
const IncompletePrefix = "incomplete:"

// Secs is a convenience for the optional second-valued fields.
func Secs(d time.Duration) *float64 { s := d.Seconds(); return &s }

// maxErrLen keeps one crashed swissknife (which prints a core-dump banner and
// the full argv) from dominating the file.
//
// 2 KiB, and the middle is what gets dropped: the head-only cut this started
// with was measured against the real 2026-08-15 failure (1586 bytes) and
// removed the discriminator. "PANIC" sits at offset 525 and "UNIQUE
// constraint failed" at 804, so a 600-byte head kept the banner and threw
// away the cause -- on exactly the failure the records exist to explain.
const maxErrLen = 2048

// truncateMiddle keeps the head and the tail, which is where a swissknife
// failure puts the reason (the argv block sits in between). Cuts land on rune
// boundaries: slicing a string mid-rune makes json.Marshal substitute U+FFFD.
func truncateMiddle(s string, max int) string {
	if len(s) <= max {
		return s
	}
	const marker = "\n…[middle truncated]…\n"
	head := max / 2
	tail := max - head - len(marker)
	if tail < 0 {
		tail = 0
	}
	for head > 0 && !utf8.RuneStart(s[head]) {
		head--
	}
	cut := len(s) - tail
	for cut < len(s) && !utf8.RuneStart(s[cut]) {
		cut++
	}
	return s[:head] + marker + s[cut:]
}

// Writer appends records as newline-delimited JSON, one file per build.
//
// Best-effort by construction: a measurement that cannot be written must
// never fail a publish, so every error is returned for logging and otherwise
// dropped by the caller.
type Writer struct {
	dir string
	mu  sync.Mutex
}

// NewWriter creates the directory and returns a Writer. A nil *Writer is
// usable and does nothing, so callers need no enabled/disabled branch.
func NewWriter(dir string) (*Writer, error) {
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("measurements dir %q: %w", dir, err)
	}
	return &Writer{dir: dir}, nil
}

// unsafeName matches everything not allowed in a build id used as a filename.
// Build ids come from CI (a GitLab pipeline id today) but are attacker-shaped
// input as far as this package is concerned: they arrive over the API.
var unsafeName = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// FileFor returns the file a record belongs in. Records with a build id group
// by it -- that is the unit an operator compares. Records without one (the
// per-package publish paths do not require a coarse build) fall back to a
// dated file, so they are still grouped and still findable, rather than being
// piled into one ever-growing "unknown".
func (w *Writer) FileFor(buildID string, ts time.Time) string {
	name := sanitiseBuildID(buildID)
	if name == "" {
		name = "nobuild-" + ts.UTC().Format("20060102")
	}
	return filepath.Join(w.dir, name+".ndjson")
}

// sanitiseBuildID maps a build id to a filename component. Used by BOTH the
// write and the read side: when only FileFor truncated, a build id over 64
// characters was written under one name and looked up under another, so its
// records existed and returned 404.
func sanitiseBuildID(buildID string) string {
	name := unsafeName.ReplaceAllString(strings.TrimSpace(buildID), "_")
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}

// Append writes one record. Safe for concurrent use and safe on a nil Writer.
func (w *Writer) Append(r Record) error {
	if w == nil {
		return nil
	}
	if r.Timestamp.IsZero() {
		r.Timestamp = time.Now().UTC()
	}
	r.Error = truncateMiddle(r.Error, maxErrLen)
	line, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshalling measurement: %w", err)
	}
	path := w.FileFor(r.BuildID, r.Timestamp)

	// One process writes these, but several job goroutines do: the mutex
	// keeps two records from interleaving inside one line. O_APPEND alone is
	// only atomic up to PIPE_BUF, and a record with a truncated swissknife
	// error can exceed it.
	w.mu.Lock()
	defer w.mu.Unlock()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if errors.Is(err, fs.ErrNotExist) {
		// The spool gets cleaned; without this every publish afterwards
		// silently drops its record until the service restarts.
		if mkErr := os.MkdirAll(w.dir, 0o700); mkErr == nil {
			f, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		}
	}
	if err != nil {
		return fmt.Errorf("opening %q: %w", path, err)
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("appending to %q: %w", path, err)
	}
	return nil
}

// Builds lists the recorded build ids, newest first by file modification.
func (w *Writer) Builds() ([]string, error) {
	if w == nil {
		return nil, nil
	}
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	type item struct {
		name string
		mod  time.Time
	}
	var items []item
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".ndjson") {
			continue
		}
		info, statErr := e.Info()
		if statErr != nil {
			continue
		}
		items = append(items, item{strings.TrimSuffix(e.Name(), ".ndjson"), info.ModTime()})
	}
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j].mod.After(items[j-1].mod); j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.name)
	}
	return out, nil
}

// Read returns the records of one build, in the order they were written.
// A malformed line is skipped rather than failing the read: a truncated last
// line (service killed mid-write) must not hide the 169 records before it.
func (w *Writer) Read(buildID string) ([]Record, error) {
	if w == nil {
		return nil, nil
	}
	name := sanitiseBuildID(buildID)
	if name == "" {
		return nil, fmt.Errorf("empty build id")
	}
	blob, err := os.ReadFile(filepath.Join(w.dir, name+".ndjson"))
	if err != nil {
		return nil, err
	}
	var out []Record
	for _, line := range strings.Split(string(blob), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var r Record
		if json.Unmarshal([]byte(line), &r) != nil {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}
