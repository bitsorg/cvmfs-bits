package cvmfscatalog

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestCreateAndUpsert(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.db.Close()

	// Upsert a file entry
	now := time.Now().Unix()
	fileEntry := Entry{
		FullPath: "/test.txt",
		Name:     "test.txt",
		Hash:     []byte("test_hash_value_1234567890123456"),
		HashAlgo: HashSha256,
		CompAlgo: CompZlib,
		Size:     1024,
		Mode:     0o100644,
		Mtime:    now,
		MtimeNs:  0,
		UID:      1000,
		GID:      1000,
		LinkCount: 1,
	}

	if err := cat.Upsert(fileEntry); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Verify file was inserted
	var count int
	err = cat.db.QueryRow("SELECT COUNT(*) FROM catalog").Scan(&count)
	if err != nil {
		t.Fatalf("Counting entries failed: %v", err)
	}
	// Should be 2: root + file
	if count != 2 {
		t.Errorf("Expected 2 entries, got %d", count)
	}
}

func TestDirectoryEntry(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.db.Close()

	// Upsert a directory entry
	now := time.Now().Unix()
	dirEntry := Entry{
		FullPath:  "/mydir",
		Name:      "mydir",
		Mode:      fs.ModeDir | 0o755,
		Size:      4096,
		Mtime:     now,
		LinkCount: 1,
	}

	if err := cat.Upsert(dirEntry); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Verify directory entry
	var flags int
	err = cat.db.QueryRow("SELECT flags FROM catalog WHERE name = 'mydir'").Scan(&flags)
	if err != nil {
		t.Fatalf("Querying directory flags failed: %v", err)
	}
	if (flags & FlagDir) == 0 {
		t.Errorf("FlagDir not set in flags: %d", flags)
	}
}

func TestSymlinkEntry(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.db.Close()

	// Upsert a symlink entry
	now := time.Now().Unix()
	linkEntry := Entry{
		FullPath:  "/link",
		Name:      "link",
		Mode:      fs.ModeSymlink | 0o777,
		Symlink:   "/target",
		Mtime:     now,
		LinkCount: 1,
	}

	if err := cat.Upsert(linkEntry); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Verify symlink entry
	var symlink string
	var flags int
	err = cat.db.QueryRow("SELECT symlink, flags FROM catalog WHERE name = 'link'").Scan(&symlink, &flags)
	if err != nil {
		t.Fatalf("Querying symlink failed: %v", err)
	}
	if symlink != "/target" {
		t.Errorf("Expected symlink '/target', got '%s'", symlink)
	}
	if (flags & FlagLink) == 0 {
		t.Errorf("FlagLink not set in flags: %d", flags)
	}
}

func TestRemove(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.db.Close()

	// Upsert a file
	now := time.Now().Unix()
	fileEntry := Entry{
		FullPath:  "/test.txt",
		Name:      "test.txt",
		Mode:      0o100644,
		Size:      100,
		Mtime:     now,
		LinkCount: 1,
	}

	if err := cat.Upsert(fileEntry); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Remove it
	if err := cat.Remove("/test.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify it's gone
	var count int
	err = cat.db.QueryRow("SELECT COUNT(*) FROM catalog WHERE name = 'test.txt'").Scan(&count)
	if err != nil {
		t.Fatalf("Counting entries failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 entries, got %d", count)
	}
}

func TestNestedCatalog(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.db.Close()

	// Upsert a directory that will be nested
	now := time.Now().Unix()
	dirEntry := Entry{
		FullPath:  "/nested",
		Name:      "nested",
		Mode:      fs.ModeDir | 0o755,
		Size:      4096,
		Mtime:     now,
		LinkCount: 1,
	}

	if err := cat.Upsert(dirEntry); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Add as nested mount
	hashHex := "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	if err := cat.AddNestedMount("/nested", hashHex, 5000); err != nil {
		t.Fatalf("AddNestedMount failed: %v", err)
	}

	// Verify nested_catalogs entry
	var count int
	err = cat.db.QueryRow("SELECT COUNT(*) FROM nested_catalogs").Scan(&count)
	if err != nil {
		t.Fatalf("Counting nested catalogs failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 nested catalog, got %d", count)
	}

	// Verify FlagDirNestedMount is set
	var flags int
	err = cat.db.QueryRow("SELECT flags FROM catalog WHERE name = 'nested'").Scan(&flags)
	if err != nil {
		t.Fatalf("Querying directory flags failed: %v", err)
	}
	if (flags & FlagDirNestedMount) == 0 {
		t.Errorf("FlagDirNestedMount not set in flags: %d", flags)
	}
}

func TestFinalize(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Upsert an entry
	now := time.Now().Unix()
	fileEntry := Entry{
		FullPath:  "/test.txt",
		Name:      "test.txt",
		Mode:      0o100644,
		Size:      100,
		Mtime:     now,
		LinkCount: 1,
	}

	if err := cat.Upsert(fileEntry); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Finalize
	destDir := filepath.Join(tmpdir, "cas")
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatalf("Creating dest dir failed: %v", err)
	}

	hash, _, err := cat.Finalize(destDir)
	if err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	if hash == "" {
		t.Errorf("Expected non-empty hash")
	}

	// Verify CAS file exists
	casPath := filepath.Join(destDir, "data", hash[:2], hash+"C")
	if _, err := os.Stat(casPath); err != nil {
		t.Errorf("CAS file not found at %s: %v", casPath, err)
	}
}

func TestNestedCatalogRoot(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "nested.db")

	// Create a nested catalog
	cat, err := Create(dbPath, "/atlas/24.0")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.db.Close()

	// Verify root entry has FlagDirNestedRoot
	var flags int
	err = cat.db.QueryRow("SELECT flags FROM catalog WHERE name = ''").Scan(&flags)
	if err != nil {
		t.Fatalf("Querying root entry failed: %v", err)
	}
	if (flags & FlagDirNestedRoot) == 0 {
		t.Errorf("FlagDirNestedRoot not set in root entry flags: %d", flags)
	}
}

func TestStatsDeltaTracking(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.db.Close()

	now := time.Now().Unix()

	// Upsert 2 regular files
	cat.Upsert(Entry{
		FullPath:  "/file1.txt",
		Name:      "file1.txt",
		Mode:      0o100644,
		Size:      100,
		Mtime:     now,
		LinkCount: 1,
	})
	cat.Upsert(Entry{
		FullPath:  "/file2.txt",
		Name:      "file2.txt",
		Mode:      0o100644,
		Size:      200,
		Mtime:     now,
		LinkCount: 1,
	})

	// Upsert 1 directory
	cat.Upsert(Entry{
		FullPath:  "/mydir",
		Name:      "mydir",
		Mode:      fs.ModeDir | 0o755,
		Size:      4096,
		Mtime:     now,
		LinkCount: 1,
	})

	// Upsert 1 symlink
	cat.Upsert(Entry{
		FullPath:  "/mylink",
		Name:      "mylink",
		Mode:      fs.ModeSymlink | 0o777,
		Symlink:   "/target",
		Mtime:     now,
		LinkCount: 1,
	})

	// Check delta: Create sets root dir (self_dir=1), then we add 2 files, 1 dir, 1 symlink
	// After Create: delta.SelfDir = 1
	// After Upsert file1: delta.SelfRegular = 1
	// After Upsert file2: delta.SelfRegular = 2
	// After Upsert mydir: delta.SelfDir = 2
	// After Upsert mylink: delta.SelfSymlink = 1

	if cat.delta.SelfRegular != 2 {
		t.Errorf("Expected SelfRegular=2, got %d", cat.delta.SelfRegular)
	}
	if cat.delta.SelfDir != 2 {
		t.Errorf("Expected SelfDir=2, got %d", cat.delta.SelfDir)
	}
	if cat.delta.SelfSymlink != 1 {
		t.Errorf("Expected SelfSymlink=1, got %d", cat.delta.SelfSymlink)
	}
}

func TestFinalizeFlushesStatistics(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	now := time.Now().Unix()

	// Upsert 3 regular files
	for i := 1; i <= 3; i++ {
		cat.Upsert(Entry{
			FullPath:  fmt.Sprintf("/file%d.txt", i),
			Name:      fmt.Sprintf("file%d.txt", i),
			Mode:      0o100644,
			Size:      int64(100 * i),
			Mtime:     now,
			LinkCount: 1,
		})
	}

	destDir := filepath.Join(tmpdir, "cas")
	os.MkdirAll(destDir, 0o755)

	hash, delta, err := cat.Finalize(destDir)
	if err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	if hash == "" {
		t.Errorf("Expected non-empty hash")
	}

	// Verify delta returned
	if delta.SelfRegular != 3 {
		t.Errorf("Expected delta.SelfRegular=3, got %d", delta.SelfRegular)
	}
	if delta.SelfDir != 1 {
		t.Errorf("Expected delta.SelfDir=1, got %d", delta.SelfDir)
	}

	// Reopen the SQLite database and verify statistics were flushed
	reopenedDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Opening database failed: %v", err)
	}
	defer reopenedDB.db.Close()

	stats, err := reopenedDB.GetStatistics()
	if err != nil {
		t.Fatalf("GetStatistics failed: %v", err)
	}

	if stats.SelfRegular != 3 {
		t.Errorf("Expected persisted SelfRegular=3, got %d", stats.SelfRegular)
	}
	if stats.SelfDir != 1 {
		t.Errorf("Expected persisted SelfDir=1, got %d", stats.SelfDir)
	}
}

func TestUpsertWithChunks(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.db.Close()

	now := time.Now().Unix()

	// Upsert an entry with chunks
	cat.Upsert(Entry{
		FullPath:  "/chunked.bin",
		Name:      "chunked.bin",
		Mode:      0o100644,
		Size:      2048,
		Mtime:     now,
		LinkCount: 1,
		Chunks: []ChunkRecord{
			{Offset: 0, Size: 1024, Hash: []byte("hash1")},
			{Offset: 1024, Size: 1024, Hash: []byte("hash2")},
		},
	})

	// Verify chunks table has 2 rows
	var count int
	err = cat.db.QueryRow("SELECT COUNT(*) FROM chunks WHERE name IS NULL OR name = ''").Scan(&count)
	if err == nil && count > 0 {
		t.Logf("Warning: unexpected named chunks found")
	}

	err = cat.db.QueryRow("SELECT COUNT(*) FROM chunks").Scan(&count)
	if err != nil {
		t.Fatalf("Counting chunks failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 chunks, got %d", count)
	}

	// Verify FlagFileChunk is set
	var flags int
	err = cat.db.QueryRow("SELECT flags FROM catalog WHERE name = 'chunked.bin'").Scan(&flags)
	if err != nil {
		t.Fatalf("Querying flags failed: %v", err)
	}
	if (flags & FlagFileChunk) == 0 {
		t.Errorf("FlagFileChunk not set in flags: %d", flags)
	}
}

func TestRemoveDeletesChunks(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.db.Close()

	now := time.Now().Unix()

	// Upsert a chunked entry
	cat.Upsert(Entry{
		FullPath:  "/chunked.bin",
		Name:      "chunked.bin",
		Mode:      0o100644,
		Size:      2048,
		Mtime:     now,
		LinkCount: 1,
		Chunks: []ChunkRecord{
			{Offset: 0, Size: 1024, Hash: []byte("hash1")},
			{Offset: 1024, Size: 1024, Hash: []byte("hash2")},
		},
	})

	// Verify chunks exist
	var count int
	err = cat.db.QueryRow("SELECT COUNT(*) FROM chunks").Scan(&count)
	if err != nil || count != 2 {
		t.Fatalf("Expected 2 chunks before remove, got %d", count)
	}

	// Remove the entry
	cat.Remove("/chunked.bin")

	// Verify chunks are deleted
	err = cat.db.QueryRow("SELECT COUNT(*) FROM chunks").Scan(&count)
	if err != nil {
		t.Fatalf("Counting chunks after remove failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 chunks after remove, got %d", count)
	}
}

func TestUpsertReplaceUpdatesDelta(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.db.Close()

	now := time.Now().Unix()

	// Upsert a file
	cat.Upsert(Entry{
		FullPath:  "/test.txt",
		Name:      "test.txt",
		Mode:      0o100644,
		Size:      100,
		Mtime:     now,
		LinkCount: 1,
	})

	// Delta should be 1 (after root dir which is 1)
	if cat.delta.SelfRegular != 1 {
		t.Errorf("Expected SelfRegular=1 after first upsert, got %d", cat.delta.SelfRegular)
	}

	// Upsert the same path again (replacement)
	cat.Upsert(Entry{
		FullPath:  "/test.txt",
		Name:      "test.txt",
		Mode:      0o100644,
		Size:      200,
		Mtime:     now,
		LinkCount: 1,
	})

	// Delta should still be 1 (trackRemove(-1) + trackAdd(+1) = net 0 from second upsert)
	if cat.delta.SelfRegular != 1 {
		t.Errorf("Expected SelfRegular=1 after second upsert (replacement), got %d", cat.delta.SelfRegular)
	}
}

// TestCatalogClose verifies that Close() is idempotent and that Finalize sets
// db to nil so subsequent Close calls are no-ops (Fix H1).
func TestCatalogClose(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// First Close should succeed.
	if err := cat.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	// db pointer should be nil after Close.
	if cat.db != nil {
		t.Error("db should be nil after Close()")
	}
	// Second Close on an already-closed catalog should be a no-op.
	if err := cat.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

// TestFinalizeNilsDB verifies that Finalize sets c.db = nil so a subsequent
// Close() is safe (Fix H1).
func TestFinalizeNilsDB(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")
	destDir := t.TempDir()

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if _, _, err := cat.Finalize(destDir); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	// db should be nil after Finalize.
	if cat.db != nil {
		t.Error("cat.db should be nil after Finalize")
	}

	// Close should be a safe no-op.
	if err := cat.Close(); err != nil {
		t.Fatalf("Close after Finalize failed: %v", err)
	}
}

// TestRemoveDeltaNetZero verifies that adding then removing an entry leaves the
// in-memory delta at zero for all counters (Fix C1 — delta only updated post-commit).
func TestRemoveDeltaNetZero(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.Close()

	now := time.Now().Unix()

	// Add a regular file.
	if err := cat.Upsert(Entry{
		FullPath:  "/file.txt",
		Name:      "file.txt",
		Mode:      0o100644,
		Mtime:     now,
		LinkCount: 1,
	}); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Add a symlink.
	if err := cat.Upsert(Entry{
		FullPath: "/link",
		Name:     "link",
		Mode:     fs.ModeSymlink | 0o777,
		Symlink:  "/target",
		Mtime:    now,
	}); err != nil {
		t.Fatalf("Upsert symlink failed: %v", err)
	}

	// Remove both entries.
	if err := cat.Remove("/file.txt"); err != nil {
		t.Fatalf("Remove file failed: %v", err)
	}
	if err := cat.Remove("/link"); err != nil {
		t.Fatalf("Remove symlink failed: %v", err)
	}

	// After balanced add+remove the relevant self-counters should be zero.
	// (Root dir adds SelfDir=1 from Create, which we don't remove here.)
	if cat.delta.SelfRegular != 0 {
		t.Errorf("SelfRegular should be 0 after add+remove, got %d", cat.delta.SelfRegular)
	}
	if cat.delta.SelfSymlink != 0 {
		t.Errorf("SelfSymlink should be 0 after add+remove, got %d", cat.delta.SelfSymlink)
	}
}

// TestUpsertAtomicReplace verifies that replacing an entry with Upsert correctly
// removes the old row and inserts the new one, with exactly one catalog row
// and updated delta (Fix C3 — single transaction for replace).
func TestUpsertAtomicReplace(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.Close()

	now := time.Now().Unix()

	// Insert a regular file.
	if err := cat.Upsert(Entry{
		FullPath:  "/file.txt",
		Name:      "file.txt",
		Mode:      0o100644,
		Size:      100,
		Mtime:     now,
		LinkCount: 1,
	}); err != nil {
		t.Fatalf("first Upsert failed: %v", err)
	}

	deltaAfterFirst := cat.delta.SelfRegular

	// Replace the same path with a different size.
	if err := cat.Upsert(Entry{
		FullPath:  "/file.txt",
		Name:      "file.txt",
		Mode:      0o100644,
		Size:      999,
		Mtime:     now + 1,
		LinkCount: 1,
	}); err != nil {
		t.Fatalf("second Upsert (replace) failed: %v", err)
	}

	// Delta should be unchanged: trackRemove(-1) + trackAdd(+1) = net 0.
	if cat.delta.SelfRegular != deltaAfterFirst {
		t.Errorf("SelfRegular: want %d after replace, got %d", deltaAfterFirst, cat.delta.SelfRegular)
	}

	// Exactly one catalog row for /file.txt.
	p1, p2 := MD5Path("/file.txt")
	var count int
	if err := cat.db.QueryRow(
		"SELECT COUNT(*) FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 catalog row for /file.txt after replace, got %d", count)
	}

	// The row should reflect the new size.
	var size int64
	if err := cat.db.QueryRow(
		"SELECT size FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	).Scan(&size); err != nil {
		t.Fatalf("size query failed: %v", err)
	}
	if size != 999 {
		t.Errorf("expected size=999 after replace, got %d", size)
	}
}

// TestCatalogUniqueConstraint verifies that the UNIQUE (md5path_1, md5path_2)
// constraint on the catalog table prevents a concurrent or buggy caller from
// inserting a second row for the same path (Fix L3).
//
// The constraint is enforced at the DB level, so even a raw INSERT (bypassing
// the transactional upsertEntry logic) must fail.
func TestCatalogUniqueConstraint(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.Close()

	now := time.Now().Unix()
	p1, p2 := MD5Path("/dup.txt")

	// First raw INSERT should succeed.
	_, err = cat.db.Exec(`
		INSERT INTO catalog (md5path_1, md5path_2, parent_1, parent_2,
			hardlinks, hash, size, mode, mtime, mtimens, flags, name, symlink, uid, gid, xattr)
		VALUES (?, ?, 0, 0, 1, NULL, 100, 33188, ?, 0, 0, 'dup.txt', '', 0, 0, NULL)`,
		p1, p2, now)
	if err != nil {
		t.Fatalf("first INSERT failed: %v", err)
	}

	// Second INSERT with the same (md5path_1, md5path_2) must be rejected.
	_, err = cat.db.Exec(`
		INSERT INTO catalog (md5path_1, md5path_2, parent_1, parent_2,
			hardlinks, hash, size, mode, mtime, mtimens, flags, name, symlink, uid, gid, xattr)
		VALUES (?, ?, 0, 0, 1, NULL, 200, 33188, ?, 0, 0, 'dup.txt', '', 0, 0, NULL)`,
		p1, p2, now)
	if err == nil {
		t.Error("expected UNIQUE constraint violation on second INSERT, got nil error")
	}
}

// TestChunksUniqueConstraint verifies that the UNIQUE (md5path_1, md5path_2, offset)
// constraint on the chunks table prevents duplicate chunk rows (Fix L3).
func TestChunksUniqueConstraint(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "test.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer cat.Close()

	p1, p2 := MD5Path("/chunked.bin")

	// First chunk INSERT should succeed.
	_, err = cat.db.Exec(
		"INSERT INTO chunks (md5path_1, md5path_2, offset, size, hash) VALUES (?, ?, 0, 4096, NULL)",
		p1, p2)
	if err != nil {
		t.Fatalf("first chunk INSERT failed: %v", err)
	}

	// Second INSERT with the same (path, offset=0) must be rejected.
	_, err = cat.db.Exec(
		"INSERT INTO chunks (md5path_1, md5path_2, offset, size, hash) VALUES (?, ?, 0, 4096, NULL)",
		p1, p2)
	if err == nil {
		t.Error("expected UNIQUE constraint violation on duplicate chunk INSERT, got nil error")
	}
}

// ── N3: UNIQUE indexes survive Open() ────────────────────────────────────────

// TestOpenAppliesUniqueIndexes verifies that Open() enforces UNIQUE constraints
// even on a catalog whose schema predates the explicit index creation (Fix N3).
// We simulate a "legacy" catalog by stripping the unique index from a freshly
// created one, re-opening it, and confirming the constraint is reinstated.
func TestOpenAppliesUniqueIndexes(t *testing.T) {
	tmpdir := t.TempDir()
	dbPath := filepath.Join(tmpdir, "legacy.db")

	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Drop the unique index to simulate a pre-constraint catalog.
	if _, err := cat.db.Exec("DROP INDEX IF EXISTS idx_catalog_path"); err != nil {
		t.Fatalf("DROP INDEX: %v", err)
	}
	cat.Close()

	// Re-open: Open() must recreate the index.
	cat2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cat2.Close()

	p1, p2 := MD5Path("/file.bin")
	insert := func() error {
		_, err := cat2.db.Exec(
			`INSERT INTO catalog (md5path_1, md5path_2, parent_1, parent_2,
			 hardlinks, hash, size, mode, mtime, mtimens, flags, name, symlink, uid, gid, xattr)
			 VALUES (?,?,0,0,1,NULL,0,0,0,0,0,'file.bin','',0,0,NULL)`, p1, p2)
		return err
	}

	if err := insert(); err != nil {
		t.Fatalf("first INSERT failed: %v", err)
	}
	if err := insert(); err == nil {
		t.Error("expected UNIQUE violation on second INSERT, got nil")
	}
}

// ── N4: Remove() returns ErrNotFound ─────────────────────────────────────────

// TestRemoveErrNotFound verifies that Remove returns ErrNotFound when the
// path does not exist, and that the sentinel is identifiable via errors.Is.
func TestRemoveErrNotFound(t *testing.T) {
	tmpdir := t.TempDir()
	cat, err := Create(filepath.Join(tmpdir, "cat.db"), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer cat.Close()

	err = cat.Remove("/does/not/exist")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// TestRemoveAfterUpsertNoError verifies that Remove does NOT return ErrNotFound
// when the entry was previously upserted — i.e. the happy path still works.
func TestRemoveAfterUpsertNoError(t *testing.T) {
	tmpdir := t.TempDir()
	cat, err := Create(filepath.Join(tmpdir, "cat.db"), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer cat.Close()

	now := time.Now().Unix()
	entry := Entry{
		FullPath: "/remove-me.txt",
		Name:     "remove-me.txt",
		Hash:     []byte("aabbccdd"),
		Size:     42,
		Mode:     0o100644,
		Mtime:    now,
	}
	if err := cat.Upsert(entry); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := cat.Remove("/remove-me.txt"); err != nil {
		t.Errorf("Remove of existing entry: %v", err)
	}
	// Second remove should return ErrNotFound.
	if err := cat.Remove("/remove-me.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("second Remove: expected ErrNotFound, got %v", err)
	}
}

// ── N5: Close() concurrent safety ────────────────────────────────────────────

// TestCloseConcurrentSafe verifies that calling Close() from many goroutines
// simultaneously does not panic or return a double-close error (Fix N5).
func TestCloseConcurrentSafe(t *testing.T) {
	tmpdir := t.TempDir()
	cat, err := Create(filepath.Join(tmpdir, "cat.db"), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	const goroutines = 32
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			errs[i] = cat.Close()
		}()
	}
	wg.Wait()

	// Exactly one Close() call must succeed (nil error); the rest must also
	// return nil because they hit the once gate on an already-closed catalog.
	// None must panic.
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: Close() returned unexpected error: %v", i, err)
		}
	}
}

// ── N6: nested_catalogs UNIQUE constraint ────────────────────────────────────

// TestNestedCatalogsUniqueConstraint verifies that inserting a second
// nested_catalogs row for the same path is rejected at the DB level (Fix N6).
func TestNestedCatalogsUniqueConstraint(t *testing.T) {
	tmpdir := t.TempDir()
	cat, err := Create(filepath.Join(tmpdir, "cat.db"), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer cat.Close()

	p1, p2 := MD5Path("/nested")

	insert := func() error {
		_, err := cat.db.Exec(
			"INSERT INTO nested_catalogs (md5path_1, md5path_2, sha1, size) VALUES (?, ?, NULL, 0)",
			p1, p2)
		return err
	}

	if err := insert(); err != nil {
		t.Fatalf("first INSERT into nested_catalogs failed: %v", err)
	}
	if err := insert(); err == nil {
		t.Error("expected UNIQUE constraint violation on duplicate nested_catalogs INSERT, got nil")
	}
}

// ── xattr feature ─────────────────────────────────────────────────────────────

// TestUpsertXattrStored verifies that xattrs written via Upsert are persisted
// in the xattr BLOB column. FlagXattr is an in-memory-only tracking constant
// and must NOT appear in the DB flags column (real CVMFS uses bit 14 for
// kFlagDirBindMountpoint; xattr presence is signalled by a non-NULL BLOB).
func TestUpsertXattrStored(t *testing.T) {
	tmpdir := t.TempDir()
	cat, err := Create(filepath.Join(tmpdir, "cat.db"), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer cat.Close()

	now := time.Now().Unix()
	entry := Entry{
		FullPath:  "/tagged.bin",
		Name:      "tagged.bin",
		Mode:      0o100644,
		Size:      100,
		Mtime:     now,
		LinkCount: 1,
		Xattr: map[string][]byte{
			"user.myapp.version": []byte("1.2.3"),
			"user.myapp.env":     []byte("prod"),
		},
	}
	if err := cat.Upsert(entry); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Read raw xattr BLOB and flags from the DB.
	var blob []byte
	var flags int
	row := cat.db.QueryRow(
		"SELECT xattr, flags FROM catalog WHERE name = 'tagged.bin'")
	if err := row.Scan(&blob, &flags); err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	// FlagXattr must NOT appear in the persisted flags: it is an internal
	// prepub constant that is masked out before the DB write.
	if flags&FlagXattr != 0 {
		t.Errorf("FlagXattr must not be stored in the DB flags column (got flags=%d)", flags)
	}

	// Xattr presence is determined solely by the BLOB being non-NULL.
	if len(blob) == 0 {
		t.Fatal("xattr BLOB is empty")
	}
}

// TestUpsertNoXattrNullBlob verifies that an entry with no xattrs stores a
// NULL BLOB and that FlagXattr is absent from the DB flags column.
func TestUpsertNoXattrNullBlob(t *testing.T) {
	tmpdir := t.TempDir()
	cat, err := Create(filepath.Join(tmpdir, "cat.db"), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer cat.Close()

	entry := Entry{
		FullPath:  "/plain.bin",
		Name:      "plain.bin",
		Mode:      0o100644,
		Size:      10,
		Mtime:     time.Now().Unix(),
		LinkCount: 1,
	}
	if err := cat.Upsert(entry); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	var blob []byte
	var flags int
	row := cat.db.QueryRow(
		"SELECT xattr, flags FROM catalog WHERE name = 'plain.bin'")
	if err := row.Scan(&blob, &flags); err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if flags&FlagXattr != 0 {
		t.Errorf("FlagXattr unexpectedly set for entry with no xattrs (flags=%d)", flags)
	}
	if blob != nil {
		t.Errorf("expected NULL xattr BLOB, got %d bytes", len(blob))
	}
}

// TestXattrDeltaTracking verifies that SelfXattr is incremented and decremented
// correctly as entries with and without xattrs are added and removed.
func TestXattrDeltaTracking(t *testing.T) {
	tmpdir := t.TempDir()
	cat, err := Create(filepath.Join(tmpdir, "cat.db"), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer cat.Close()

	now := time.Now().Unix()
	withXattr := Entry{
		FullPath:  "/x.bin",
		Name:      "x.bin",
		Mode:      0o100644,
		Size:      1,
		Mtime:     now,
		LinkCount: 1,
		Xattr:     map[string][]byte{"user.a": []byte("b")},
	}
	noXattr := Entry{
		FullPath:  "/y.bin",
		Name:      "y.bin",
		Mode:      0o100644,
		Size:      1,
		Mtime:     now,
		LinkCount: 1,
	}

	cat.Upsert(withXattr)
	cat.Upsert(noXattr)

	if cat.delta.SelfXattr != 1 {
		t.Errorf("after two upserts, SelfXattr=%d, want 1", cat.delta.SelfXattr)
	}

	// Remove the one with xattr; delta should go to 0.
	cat.Remove("/x.bin")
	if cat.delta.SelfXattr != 0 {
		t.Errorf("after removing xattr entry, SelfXattr=%d, want 0", cat.delta.SelfXattr)
	}
}
