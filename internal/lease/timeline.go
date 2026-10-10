// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"
)

// timeline collects a subprocess's combined stdout and stderr, as
// CombinedOutput did, and notes when each line arrived.
//
// From prepub's side `cvmfs_server ingest` is one process, but inside it opens
// the transaction, runs swissknife, closes the transaction and remounts. The
// arrival times of its own messages are the only record of which of those
// steps a slow or stuck publish spent its time in.
type timeline struct {
	mu    sync.Mutex
	now   func() time.Time
	start time.Time
	out   bytes.Buffer
	next  int           // offset in out where the unfinished line starts
	from  time.Duration // when the unfinished line's first byte arrived
	last  time.Duration // when the most recent bytes arrived
	lines []timedLine
}

// timedLine is one output line with when its first byte and its newline
// arrived. They differ for a step that prints its name, works, and only then
// ends the line ("Note: Catalog ... gets defragmented... done").
type timedLine struct {
	from, to time.Duration
	text     string
}

// String renders at most timelineHead+timelineTail lines, each at most
// timelineLineMax bytes: a verbose run is cut in the middle, away from the
// first and last lines, which are where the steps begin and end.
const (
	timelineHead    = 40
	timelineTail    = 20
	timelineLineMax = 300
)

func newTimeline() *timeline {
	return newTimelineAt(time.Now)
}

func newTimelineAt(now func() time.Time) *timeline {
	return &timeline{now: now, start: now()}
}

// Write implements io.Writer. Only the new bytes are scanned, so output that
// arrives a byte at a time (progress dots) costs no more than any other.
func (t *timeline) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	at := t.now().Sub(t.start)
	scan := t.out.Len()
	if t.next == scan {
		t.from = at
	}
	t.out.Write(p)
	t.last = at
	for {
		i := bytes.IndexByte(t.out.Bytes()[scan:], '\n')
		if i < 0 {
			return len(p), nil
		}
		end := scan + i
		t.add(string(t.out.Bytes()[t.next:end]), t.from, at)
		t.next, scan, t.from = end+1, end+1, at
	}
}

func (t *timeline) add(s string, from, to time.Duration) {
	s = strings.TrimRight(s, "\r")
	if strings.TrimSpace(s) != "" {
		t.lines = append(t.lines, timedLine{from: from, to: to, text: s})
	}
}

// Output returns everything written.
func (t *timeline) Output() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.out.String()
}

// Lines returns the timed lines, including a last line without a newline.
func (t *timeline) Lines() []timedLine {
	t.mu.Lock()
	defer t.mu.Unlock()
	ls := append([]timedLine(nil), t.lines...)
	if rest := strings.TrimRight(string(t.out.Bytes()[t.next:]), "\r"); strings.TrimSpace(rest) != "" {
		ls = append(ls, timedLine{from: t.from, to: t.last, text: rest})
	}
	return ls
}

// String renders "+0.0s first | +4.1s second | +5.0s..+605.0s third | ...":
// one time when a line arrived at once, its first byte and its end when not.
func (t *timeline) String() string {
	ls := t.Lines()
	render := func(l timedLine) string {
		s := l.text
		if len(s) > timelineLineMax {
			s = strings.ToValidUTF8(s[:timelineLineMax], "") + "…"
		}
		if l.to-l.from >= 100*time.Millisecond {
			return fmt.Sprintf("+%.1fs..+%.1fs %s", l.from.Seconds(), l.to.Seconds(), s)
		}
		return fmt.Sprintf("+%.1fs %s", l.from.Seconds(), s)
	}
	var parts []string
	if len(ls) > timelineHead+timelineTail {
		for _, l := range ls[:timelineHead] {
			parts = append(parts, render(l))
		}
		parts = append(parts, fmt.Sprintf("… %d lines …", len(ls)-timelineHead-timelineTail))
		ls = ls[len(ls)-timelineTail:]
	}
	for _, l := range ls {
		parts = append(parts, render(l))
	}
	return strings.Join(parts, " | ")
}
