package spool

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"sync"
	"time"

	"cvmfs.io/prepub/internal/job"
)

// crc32Table uses the IEEE polynomial, matching most CRC32 implementations.
var crc32Table = crc32.MakeTable(crc32.IEEE)

type Entry struct {
	T     time.Time `json:"t"`
	JobID string    `json:"job_id"`
	From  job.State `json:"from"`
	To    job.State `json:"to"`
	RunID string    `json:"run_id"`
	Note  string    `json:"note,omitempty"`
}

type Journal struct {
	path string
	mu   sync.Mutex
}

// OpenJournal opens or creates a journal file.
func OpenJournal(dir string) *Journal {
	path := dir + "/journal.jsonl"
	return &Journal{path: path}
}

// Append writes an entry to the journal and fsyncs.
//
// Fix #10: Each line is prefixed with an 8-hex-character CRC32 checksum of
// the JSON payload separated by a space:
//
//	<crc32hex> <json>\n
//
// This lets Read() detect truncation, bit-flips, and partial writes.  A
// corrupted line causes Read() to return an error rather than silently skipping
// it — incomplete crash-recovery state is always safer than wrong state.
func (j *Journal) Append(e Entry) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	f, err := os.OpenFile(j.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("opening journal: %w", err)
	}
	defer f.Close()

	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshaling journal entry: %w", err)
	}

	checksum := crc32.Checksum(payload, crc32Table)
	line := fmt.Sprintf("%08x %s\n", checksum, payload)

	if _, err := f.Write([]byte(line)); err != nil {
		return fmt.Errorf("writing journal entry: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing journal: %w", err)
	}

	return nil
}

// Read reads and verifies all entries from the journal.
//
// Fix #10: Any line that fails CRC verification causes Read() to return an
// error immediately.  Partial or zero-byte lines at the very end of the file
// (from an interrupted write) are treated as an integrity failure so that
// crash recovery never proceeds with incomplete WAL state.
func (j *Journal) Read() ([]Entry, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	data, err := os.ReadFile(j.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading journal: %w", err)
	}

	var entries []Entry
	lineNum := 0

	for len(data) > 0 {
		// Find end of this line.
		eol := 0
		for eol < len(data) && data[eol] != '\n' {
			eol++
		}

		line := string(data[:eol])
		if eol < len(data) {
			data = data[eol+1:]
		} else {
			data = data[:0]
		}
		lineNum++

		if strings.TrimSpace(line) == "" {
			continue // skip blank lines (e.g. trailing newline)
		}

		// Expect "<8 hex digits> <json>".
		if len(line) < 10 || line[8] != ' ' {
			return nil, fmt.Errorf("journal line %d: missing checksum prefix", lineNum)
		}

		wantCRC, err := hex.DecodeString(line[:8])
		if err != nil {
			return nil, fmt.Errorf("journal line %d: invalid checksum %q: %w", lineNum, line[:8], err)
		}

		payload := []byte(line[9:])
		gotCRC := crc32.Checksum(payload, crc32Table)
		wantU32 := binary.BigEndian.Uint32(wantCRC)

		if gotCRC != wantU32 {
			return nil, fmt.Errorf("journal line %d: CRC mismatch (want %08x, got %08x) — possible corruption",
				lineNum, wantU32, gotCRC)
		}

		var e Entry
		if err := json.Unmarshal(payload, &e); err != nil {
			return nil, fmt.Errorf("journal line %d: JSON decode failed: %w", lineNum, err)
		}
		entries = append(entries, e)
	}

	return entries, nil
}
