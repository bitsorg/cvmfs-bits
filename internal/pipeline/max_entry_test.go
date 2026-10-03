// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
)

// hugeEntryTar is a tar whose single entry claims size bytes but carries only
// a few, so a size check and a streaming read can be told apart without
// allocating or writing anything large.
func hugeEntryTar(t *testing.T, size int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "big.bin", Mode: 0o644, Size: size, Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("short")); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes() // deliberately not closed: the body is truncated
}

// The per-file limit comes from Config.MaxEntrySize (the service sets it to
// the max tar size), not the 1 GiB unpack default, on both phase-0 paths --
// but only where files stream (spool dir + fixed grid).
func TestMaxEntrySize_ComesFromConfig(t *testing.T) {
	const twoGiB = 2 << 30
	tarData := hugeEntryTar(t, twoGiB)
	obs := newTestObs(t)
	ctx := context.Background()

	spool := t.TempDir()
	const grid = 6 << 20
	cfg := Config{Obs: obs, SpoolDir: spool, ChunkMin: grid, ChunkAvg: grid, ChunkMax: grid}
	_, err := RunFromReader(ctx, bytes.NewReader(tarData), cfg)
	if err == nil || !strings.Contains(err.Error(), "exceeds size limit") {
		t.Fatalf("default limit: err = %v, want the 1 GiB size-limit refusal", err)
	}

	// Allowed now: the read then fails on the truncated body, streamed into a
	// spill file under SpoolDir rather than a 2 GiB buffer.
	cfg.MaxEntrySize = 4 << 30
	_, err = RunFromReader(ctx, bytes.NewReader(tarData), cfg)
	if err == nil || strings.Contains(err.Error(), "exceeds size limit") {
		t.Fatalf("MaxEntrySize=4GiB: err = %v, want a truncated-body error, not the limit", err)
	}
	if left, _ := os.ReadDir(spool); len(left) != 0 {
		t.Errorf("spill directory left behind in SpoolDir: %v", left)
	}

	// The prefetch path takes the same limit.
	_, err = PrefetchFromReaderWithSpill(ctx, bytes.NewReader(tarData), t.TempDir(), 0, obs)
	if err == nil || !strings.Contains(err.Error(), "exceeds size limit") {
		t.Fatalf("prefetch default: err = %v, want the size-limit refusal", err)
	}
	_, err = PrefetchFromReaderWithSpill(ctx, bytes.NewReader(tarData), t.TempDir(), cfg.EntryLimit(), obs)
	if err == nil || strings.Contains(err.Error(), "exceeds size limit") {
		t.Fatalf("prefetch 4GiB: err = %v, want a truncated-body error, not the limit", err)
	}
}

// Paths that read a file whole into memory keep the 1 GiB cap whatever
// MaxEntrySize says: content-defined chunking, no spool dir, and a prefetch
// without a spill root.
func TestMaxEntrySize_InMemoryPathsKeepCap(t *testing.T) {
	tarData := hugeEntryTar(t, 2<<30)
	obs := newTestObs(t)
	ctx := context.Background()
	const big = 4 << 30
	for name, cfg := range map[string]Config{
		"cdc":      {Obs: obs, SpoolDir: t.TempDir(), MaxEntrySize: big, ChunkMin: 4 << 20, ChunkAvg: 8 << 20, ChunkMax: 16 << 20},
		"no-grid":  {Obs: obs, SpoolDir: t.TempDir(), MaxEntrySize: big},
		"no-spool": {Obs: obs, MaxEntrySize: big, ChunkMin: 6 << 20, ChunkAvg: 6 << 20, ChunkMax: 6 << 20},
	} {
		if got := cfg.EntryLimit(); got != 1<<30 {
			t.Errorf("%s: EntryLimit = %d, want 1 GiB", name, got)
		}
		if _, err := RunFromReader(ctx, bytes.NewReader(tarData), cfg); err == nil || !strings.Contains(err.Error(), "exceeds size limit") {
			t.Errorf("%s: err = %v, want the 1 GiB size-limit refusal", name, err)
		}
	}
	if _, err := PrefetchFromReaderWithSpill(ctx, bytes.NewReader(tarData), "", big, obs); err == nil || !strings.Contains(err.Error(), "exceeds size limit") {
		t.Errorf("prefetch without spill root: err = %v, want the size-limit refusal", err)
	}
}

// A small limit is enforced too, and entries within it still pass.
func TestMaxEntrySize_SmallLimit(t *testing.T) {
	tarData := buildTar([]struct{ name, content string }{{"a.txt", strings.Repeat("x", 100)}})
	_, err := PrefetchFromReaderWithSpill(context.Background(), bytes.NewReader(tarData), "", 10, nil)
	if err == nil || !strings.Contains(err.Error(), "exceeds size limit") {
		t.Fatalf("limit 10: err = %v, want a size-limit refusal", err)
	}
	if _, err := PrefetchFromReaderWithSpill(context.Background(), bytes.NewReader(tarData), "", 100, nil); err != nil {
		t.Fatalf("limit 100: %v", err)
	}
}
