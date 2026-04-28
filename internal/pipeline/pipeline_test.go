package pipeline

import (
	"archive/tar"
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/cvmfscatalog"
	"cvmfs.io/prepub/pkg/observe"
	"cvmfs.io/prepub/testutil/fakecas"
)

// newTestObs returns a minimal observe.Provider suitable for unit tests.
// It uses the default (no-op) options so no external services are needed.
func newTestObs(t *testing.T) *observe.Provider {
	t.Helper()
	obs, shutdown, err := observe.New("pipeline-test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	return obs
}

// buildTar builds an in-memory tar from a list of (name, content) pairs.
// Duplicate names are allowed — that is the point of L5 testing.
func buildTar(entries []struct{ name, content string }) []byte {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	now := time.Now()
	for _, e := range entries {
		data := []byte(e.content)
		tw.WriteHeader(&tar.Header{
			Name:     e.name,
			Mode:     0o644,
			Size:     int64(len(data)),
			ModTime:  now,
			Typeflag: tar.TypeReg,
		})
		tw.Write(data)
	}
	tw.Close()
	return buf.Bytes()
}

// TestPipelineDuplicatePathFails verifies that a tar containing two entries
// with the same path is rejected with an error (Fix L5 — previously the second
// entry silently overwrote the first in resultsByPath, producing a catalog with
// two rows for the same path but only one hash retained).
func TestPipelineDuplicatePathFails(t *testing.T) {
	obs := newTestObs(t)
	cas := fakecas.New(obs)
	spoolDir := t.TempDir()

	tarData := buildTar([]struct{ name, content string }{
		{"file.txt", "first content"},
		{"file.txt", "second content — same path!"},
	})

	cfg := Config{
		Workers:   1,
		ChunkSize: 0,
		CAS:       cas,
		SpoolDir:  spoolDir,
		Obs:       obs,
	}

	_, err := RunFromReader(context.Background(), bytes.NewReader(tarData), cfg)
	if err == nil {
		t.Fatal("expected error for duplicate path in tar, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("error %q should mention 'duplicate'", err.Error())
	}
}

// TestPipelineUniquePaths verifies that a well-formed tar with no duplicate
// paths succeeds (sanity check that the L5 guard doesn't break the happy path).
func TestPipelineUniquePaths(t *testing.T) {
	obs := newTestObs(t)
	cas := fakecas.New(obs)
	spoolDir := t.TempDir()

	tarData := buildTar([]struct{ name, content string }{
		{"a.txt", "content a"},
		{"b.txt", "content b"},
		{"c.txt", "content c"},
	})

	cfg := Config{
		Workers:   1,
		ChunkSize: 0,
		CAS:       cas,
		SpoolDir:  spoolDir,
		Obs:       obs,
	}

	result, err := RunFromReader(context.Background(), bytes.NewReader(tarData), cfg)
	if err != nil {
		t.Fatalf("unexpected error for unique-path tar: %v", err)
	}
	if result.NFiles != 3 {
		t.Errorf("expected NFiles=3, got %d", result.NFiles)
	}
}

// ── N7: chunk-meta allocation outside resultMu ────────────────────────────────

// TestPipelineChunkedFileMetaIsCorrect verifies that the chunk metadata stored
// in resultsByPath (now assembled outside the mutex, Fix N7) is faithfully
// propagated to the returned CatalogEntries.  We run a chunked pipeline and
// confirm that the catalog entry for the large file carries the right number of
// chunk records and non-empty hashes.
func TestPipelineChunkedFileMetaIsCorrect(t *testing.T) {
	obs := newTestObs(t)
	cas := fakecas.New(obs)
	spoolDir := t.TempDir()

	// Produce a 9-byte file and a chunk size of 3 → 3 chunks.
	const chunkSize = 3
	tarData := buildTar([]struct{ name, content string }{
		{"chunked.bin", "aabbccdd!"}, // 9 bytes
	})

	cfg := Config{
		Workers:   1,
		ChunkSize: chunkSize,
		CAS:       cas,
		SpoolDir:  spoolDir,
		Obs:       obs,
	}

	result, err := RunFromReader(context.Background(), bytes.NewReader(tarData), cfg)
	if err != nil {
		t.Fatalf("RunFromReader: %v", err)
	}

	if result.NFiles != 1 {
		t.Fatalf("expected NFiles=1, got %d", result.NFiles)
	}

	// Find the chunked entry.
	var chunkedEntry *cvmfscatalog.Entry
	for i := range result.CatalogEntries {
		if result.CatalogEntries[i].Name == "chunked.bin" {
			chunkedEntry = &result.CatalogEntries[i]
			break
		}
	}
	if chunkedEntry == nil {
		t.Fatal("chunked.bin not found in CatalogEntries")
	}

	// 9 bytes / 3 bytes per chunk = 3 chunks.
	if len(chunkedEntry.Chunks) != 3 {
		t.Errorf("expected 3 chunks, got %d", len(chunkedEntry.Chunks))
	}
	for i, ch := range chunkedEntry.Chunks {
		if len(ch.Hash) == 0 {
			t.Errorf("chunk %d: expected non-empty Hash", i)
		}
	}
}
