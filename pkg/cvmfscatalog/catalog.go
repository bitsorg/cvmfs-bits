package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"cvmfs.io/prepub/pkg/cvmfshash"
	"cvmfs.io/prepub/pkg/cvmfsxattr"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned by Remove when the specified path does not exist
// in the catalog.  Callers that treat deletion as idempotent can check for
// this sentinel and continue rather than fail.
var ErrNotFound = errors.New("entry not found")

// cvmfsStatCounters is the canonical list of counter names that cvmfs_receiver's
// SqlGetCounter queries via: SELECT value FROM statistics WHERE counter = :counter
// The names match DeltaCounters::FillFieldsMap("self_", …) and FillFieldsMap("subtree_", …)
// in cvmfs/catalog_counters.h.  Every catalog MUST have a row for each of these
// or the receiver will crash with an assert failure inside Sql::LazyInit.
var cvmfsStatCounters = []string{
	"self_regular", "self_symlink", "self_special", "self_dir", "self_nested",
	"self_chunked", "self_chunks", "self_file_size", "self_chunked_size",
	"self_xattr", "self_external", "self_external_file_size",
	"subtree_regular", "subtree_symlink", "subtree_special", "subtree_dir", "subtree_nested",
	"subtree_chunked", "subtree_chunks", "subtree_file_size", "subtree_chunked_size",
	"subtree_xattr", "subtree_external", "subtree_external_file_size",
}

// Catalog represents a CVMFS catalog (SQLite database).
type Catalog struct {
	db         *sql.DB
	dbPath     string
	rootPrefix string
	delta      Statistics // accumulated changes to flush in Finalize
	// closeOnce ensures that Close() is safe to call from multiple goroutines
	// simultaneously — only the first call actually closes the underlying DB.
	closeOnce sync.Once
}

// Statistics holds all counter columns from the statistics table.
// Fields mirror the (counter TEXT PRIMARY KEY, value INTEGER) rows that
// cvmfs_receiver reads via SqlGetCounter.
type Statistics struct {
	// Type counts (matching cvmfs/catalog_counters.h self_* / subtree_*)
	SelfRegular     int64
	SelfSymlink     int64
	SelfDir         int64
	SelfNested      int64
	SelfSpecial     int64
	SelfExternal    int64
	SelfXattr       int64
	// Chunked-file counters (task #12)
	SelfChunked    int64 // files that use the chunked-upload path
	SelfChunks     int64 // total number of chunk records across all chunked files
	// Size counters (bytes, uncompressed)
	SelfFileSize         int64 // sum of non-chunked regular file sizes
	SelfChunkedSize      int64 // sum of chunked file sizes
	SelfExternalFileSize int64 // sum of external file sizes

	SubtreeRegular  int64
	SubtreeSymlink  int64
	SubtreeDir      int64
	SubtreeNested   int64
	SubtreeSpecial  int64
	SubtreeExternal int64
	SubtreeXattr    int64
	SubtreeChunked    int64
	SubtreeChunks     int64
	SubtreeFileSize         int64
	SubtreeChunkedSize      int64
	SubtreeExternalFileSize int64
}

// Create creates a new CVMFS catalog database at dbPath with the given rootPrefix.
func Create(dbPath, rootPrefix string) (*Catalog, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Use DELETE journal mode (the SQLite default) for new catalog databases.
	//
	// WAL mode is not used here because Finalize() reads the raw .db file via
	// os.ReadFile after closing the connection.  With WAL mode the passive
	// checkpoint that runs on close may leave frames in the WAL file that have
	// not yet been written back to the main .db — producing a truncated catalog
	// that crashes cvmfs_receiver when it tries to open it as SQLite.  DELETE
	// mode writes all changes into the main .db file on every COMMIT, so the
	// file is always complete and self-contained when Finalize() reads it.
	//
	// BatchUpsert already eliminates the per-transaction overhead that WAL was
	// added to address (it batches all inserts into one transaction), so there
	// is no performance loss from keeping DELETE mode here.

	// Create all required tables for CVMFS schema 2.5.
	//
	// UNIQUE constraints are expressed as explicit named indexes rather than
	// inline column constraints so that the same CREATE UNIQUE INDEX IF NOT
	// EXISTS statements can be re-issued by Open() to retrofit the constraint
	// onto catalogs created by older code.
	//
	// nested_catalogs uses a UNIQUE constraint so that calling AddNestedMount
	// twice for the same path is rejected at the DB level rather than silently
	// producing duplicate rows.
	schema := `
CREATE TABLE IF NOT EXISTS catalog (
	md5path_1 INTEGER,
	md5path_2 INTEGER,
	parent_1 INTEGER,
	parent_2 INTEGER,
	hardlinks INTEGER,
	hash BLOB,
	size INTEGER,
	mode INTEGER,
	mtime INTEGER,
	mtimens INTEGER,
	flags INTEGER,
	name TEXT,
	symlink TEXT,
	uid INTEGER,
	gid INTEGER,
	xattr BLOB
);

CREATE TABLE IF NOT EXISTS chunks (
	md5path_1 INTEGER,
	md5path_2 INTEGER,
	offset INTEGER,
	size INTEGER,
	hash BLOB
);

CREATE TABLE IF NOT EXISTS nested_catalogs (
	path TEXT,
	sha1 TEXT,
	size INTEGER,
	CONSTRAINT pk_nested_catalogs PRIMARY KEY (path)
);

-- Required for schema 2.5 revision >= 4.
-- SqlNestedCatalogListing (catalog_sql.cc) uses:
--   SELECT path, sha1, size FROM nested_catalogs
--   UNION ALL SELECT path, sha1, size FROM bind_mountpoints
-- for catalogs with schema_revision >= 4.  Without this table the
-- sqlite3_prepare_v2 call in Sql::LazyInit fails and the receiver
-- crashes with assert(success) (displayed as sql.h:496 in optimised builds).
CREATE TABLE IF NOT EXISTS bind_mountpoints (
	path TEXT,
	sha1 TEXT,
	size INTEGER,
	CONSTRAINT pk_bind_mountpoints PRIMARY KEY (path)
);

CREATE TABLE IF NOT EXISTS statistics (
	counter TEXT PRIMARY KEY,
	value   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS properties (
	key TEXT PRIMARY KEY,
	value TEXT
);
`

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating schema: %w", err)
	}

	// Enforce uniqueness with explicit named indexes (idempotent).
	// Using CREATE UNIQUE INDEX IF NOT EXISTS (rather than inline UNIQUE in the
	// CREATE TABLE) means Open() can issue the same statements to retrofit the
	// constraint onto catalogs created by older code — without risk of error.
	if err := applyUniqueIndexes(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating unique indexes: %w", err)
	}

	// Initialize properties table with required values
	now := time.Now().Unix()
	properties := map[string]string{
		"schema":            "2.5",
		"schema_revision":   "7",
		"root_prefix":       rootPrefix,
		"revision":          "0",
		"last_modified":     fmt.Sprintf("%d", now),
		"previous_revision": "",
	}

	for key, value := range properties {
		if _, err := db.Exec("INSERT INTO properties (key, value) VALUES (?, ?)", key, value); err != nil {
			db.Close()
			return nil, fmt.Errorf("inserting property %s: %w", key, err)
		}
	}

	// Initialize statistics with zeros — one row per CVMFS counter name.
	// The schema matches what cvmfs_receiver's SqlGetCounter expects:
	//   SELECT value FROM statistics WHERE counter = :counter
	for _, ctr := range cvmfsStatCounters {
		if _, err := db.Exec(
			`INSERT OR IGNORE INTO statistics (counter, value) VALUES (?, 0)`, ctr,
		); err != nil {
			db.Close()
			return nil, fmt.Errorf("initializing statistics counter %s: %w", ctr, err)
		}
	}

	c := &Catalog{
		db:         db,
		dbPath:     dbPath,
		rootPrefix: rootPrefix,
	}

	// Insert root directory entry.
	//
	// The CVMFS receiver (catalog_mgr_rw.cc) calls LookupPath(rootPrefix) on a
	// nested catalog when loading it via AttachFreely / LoadFreeCatalog.  Because
	// is_regular_mountpoint_ = (mountpoint == root_prefix), NormalizePath returns
	// MD5(path) unchanged, so LookupPath("/test/smoke") computes MD5("/test/smoke").
	//
	// For root catalogs (rootPrefix == ""), LookupPath("") → MD5("") matches the
	// entry at FullPath="" as before.
	// For nested catalogs (rootPrefix != ""), the entry MUST be stored at
	// MD5(rootPrefix) — achieved by setting FullPath = rootPrefix here.
	isNestedRoot := rootPrefix != ""
	rootEntryPath := "" // repo root catalog
	if isNestedRoot {
		rootEntryPath = rootPrefix // nested catalog: root entry at MD5(rootPrefix)
	}
	rootEntry := Entry{
		FullPath:     rootEntryPath,
		Name:         "",
		Mode:         fs.ModeDir | 0o755,
		Size:         4096,
		Mtime:        now,
		MtimeNs:      0,
		UID:          0,
		GID:          0,
		LinkCount:    1,
		HashAlgo:     HashSha256,
		CompAlgo:     CompZlib,
		IsNestedRoot: isNestedRoot,
	}

	if err := c.upsertEntry(rootEntry); err != nil {
		// Use c.Close() so the closeOnce gate is respected and the idempotent
		// Close path is consistent with the rest of the package.
		c.Close() //nolint:errcheck // best-effort cleanup on creation failure
		return nil, fmt.Errorf("inserting root entry: %w", err)
	}

	return c, nil
}

// Open opens an existing CVMFS catalog database.
func Open(dbPath string) (*Catalog, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Enable WAL + NORMAL synchronous for the same reasons as Create().
	// WAL mode dramatically reduces per-Upsert latency for large catalogs.
	// If the pragma fails (e.g. read-only filesystem or a locked WAL from a
	// previous crash) we fall back silently — WAL is a performance improvement,
	// not a correctness requirement.
	_, _ = db.Exec(`PRAGMA journal_mode=WAL`)   //nolint:errcheck // non-fatal
	_, _ = db.Exec(`PRAGMA synchronous=NORMAL`) //nolint:errcheck // non-fatal

	// Read root_prefix from properties.
	// CVMFS only writes root_prefix for non-root catalogs (catalog_sql.cc line 374:
	// "if (!root_path.empty()) SetProperty(root_prefix, ...)").  The root catalog
	// has no root_prefix row — default to "" in that case.
	var rootPrefix string
	err = db.QueryRow("SELECT value FROM properties WHERE key = 'root_prefix'").Scan(&rootPrefix)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		db.Close()
		return nil, fmt.Errorf("reading root_prefix: %w", err)
	}
	// err == sql.ErrNoRows → root catalog, rootPrefix stays ""

	// Re-apply unique indexes — a no-op for catalogs that already have them
	// (IF NOT EXISTS).  For native CVMFS catalogs that lack md5path_1/md5path_2
	// columns the call will fail — we silently ignore that error because those
	// catalogs are opened read-only and do not need write-path uniqueness guards.
	_ = applyUniqueIndexes(db)

	return &Catalog{
		db:         db,
		dbPath:     dbPath,
		rootPrefix: rootPrefix,
	}, nil
}

// SetRootPrefix overwrites the root_prefix value in the catalog's properties
// table and updates the in-memory field.
//
// This is called by BuildSubtree on the TOP-LEVEL lease catalog ONLY (not on
// split sub-catalogs).  The reason is subtle:
//
// cvmfs_receiver loads the submitted catalog via SimpleCatalogManager, which
// hard-codes mountpoint="" in GetNewRootCatalogContext().  Inside Catalog::Open,
// is_regular_mountpoint_ = (mountpoint == root_prefix).  When root_prefix is
// non-empty (e.g., "/atlas/24.0") and mountpoint is "", the flag is false and
// NormalizePath applies the broken transformation:
//
//	MD5(root_prefix + path[mountpoint.Len():]) = MD5("/atlas/24.0" + "/atlas/24.0")
//
// instead of the correct MD5(path).  Every Listing() and LookupPath() call
// during DiffRec then returns nothing, so no files are committed.
//
// Setting root_prefix="" makes is_regular_mountpoint_=(""=="")=true, restoring
// NormalizePath to the identity MD5(path).  Entries are already stored at
// correct absolute-path MD5 keys (e.g., MD5("/atlas/24.0/bin/sh") with parent
// MD5("/atlas/24.0/bin")), so only the properties row needs updating.
//
// Split sub-catalogs are loaded by GraftNestedCatalog via LoadFreeCatalog with
// their actual path as mountpoint, so they get is_regular_mountpoint_=true
// automatically — their root_prefix must stay correct for the
// new_catalog->root_prefix() != nested_root_ps PANIC check.
func (c *Catalog) SetRootPrefix(rp string) error {
	if _, err := c.db.Exec(`UPDATE properties SET value = ? WHERE key = 'root_prefix'`, rp); err != nil {
		return fmt.Errorf("updating root_prefix to %q: %w", rp, err)
	}
	c.rootPrefix = rp
	return nil
}

// applyUniqueIndexes creates the three named UNIQUE indexes on catalog, chunks,
// and nested_catalogs tables.  All three statements are idempotent (IF NOT
// EXISTS) so it is safe to call on a newly created database (which has no
// previous indexes) and on an existing database (which may already have them).
func applyUniqueIndexes(db *sql.DB) error {
	stmts := []struct {
		name string
		sql  string
	}{
		{
			"catalog path uniqueness",
			`CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_path
			 ON catalog (md5path_1, md5path_2)`,
		},
		{
			"chunk path+offset uniqueness",
			`CREATE UNIQUE INDEX IF NOT EXISTS idx_chunks_path_offset
			 ON chunks (md5path_1, md5path_2, offset)`,
		},
		// NOTE: nested_catalogs uses "path TEXT PRIMARY KEY" matching the CVMFS
		// native schema (catalog_sql.cc line 273).  The PRIMARY KEY constraint
		// already enforces uniqueness, so no additional UNIQUE INDEX is needed.
	}
	for _, s := range stmts {
		if _, err := db.Exec(s.sql); err != nil {
			return fmt.Errorf("creating %s index: %w", s.name, err)
		}
	}
	return nil
}

// entryTrackInfo holds the fields needed to update in-memory statistics counters
// when an entry is added or removed.  It is computed from the Entry struct (for
// new entries) or from the catalog DB (when replacing or removing an existing
// entry) and passed to trackAdd / trackRemove.
type entryTrackInfo struct {
	flags      int   // entry flags; FlagXattr is set when the xattr BLOB is non-NULL
	size       int64 // uncompressed entry size in bytes
	chunkCount int   // number of chunk records (0 for non-chunked files)
}

// trackAdd increments the appropriate self-counters for a newly inserted entry.
//
// Type dispatch order (checked before falling to the default):
//  1. FlagDir   → directory
//  2. FlagLink  → symlink
//  3. FlagFileSpecial → device / named pipe / socket
//  4. FlagFileExternal → external (catalogued without stored content)
//  5. FlagFileChunk → chunked regular file
//  6. default → plain regular file
//
// FlagXattr is orthogonal to the type bits and always updates SelfXattr.
func (c *Catalog) trackAdd(info entryTrackInfo) {
	switch {
	case info.flags&FlagDir != 0:
		c.delta.SelfDir++
	case info.flags&FlagLink != 0:
		c.delta.SelfSymlink++
	case info.flags&FlagFileSpecial != 0:
		c.delta.SelfSpecial++
	case info.flags&FlagFileExternal != 0:
		c.delta.SelfRegular++
		c.delta.SelfExternal++
		c.delta.SelfExternalFileSize += info.size
	case info.flags&FlagFileChunk != 0:
		c.delta.SelfRegular++
		c.delta.SelfChunked++
		c.delta.SelfChunks += int64(info.chunkCount)
		c.delta.SelfChunkedSize += info.size
	default: // plain regular file
		c.delta.SelfRegular++
		c.delta.SelfFileSize += info.size
	}
	if info.flags&FlagXattr != 0 {
		c.delta.SelfXattr++
	}
}

// trackRemove mirrors trackAdd but decrements all counters.
func (c *Catalog) trackRemove(info entryTrackInfo) {
	switch {
	case info.flags&FlagDir != 0:
		c.delta.SelfDir--
	case info.flags&FlagLink != 0:
		c.delta.SelfSymlink--
	case info.flags&FlagFileSpecial != 0:
		c.delta.SelfSpecial--
	case info.flags&FlagFileExternal != 0:
		c.delta.SelfRegular--
		c.delta.SelfExternal--
		c.delta.SelfExternalFileSize -= info.size
	case info.flags&FlagFileChunk != 0:
		c.delta.SelfRegular--
		c.delta.SelfChunked--
		c.delta.SelfChunks -= int64(info.chunkCount)
		c.delta.SelfChunkedSize -= info.size
	default:
		c.delta.SelfRegular--
		c.delta.SelfFileSize -= info.size
	}
	if info.flags&FlagXattr != 0 {
		c.delta.SelfXattr--
	}
}

// upsertOneInTx inserts or replaces a single entry within an existing
// transaction.  It is the shared inner implementation used by both upsertEntry
// and BatchUpsert (task #9 — eliminate duplication).
//
// Returns:
//   - hasExisting: whether a pre-existing row was found and deleted.
//   - oldInfo: entryTrackInfo for the replaced row (only valid when hasExisting).
//   - newInfo: entryTrackInfo for the newly inserted row.
//
// The caller must invoke trackRemove(oldInfo) and trackAdd(newInfo) AFTER the
// enclosing transaction commits — updating the delta post-commit means a
// rollback never leaves the in-memory statistics in a permanently wrong state.
func upsertOneInTx(tx *sql.Tx, e Entry) (hasExisting bool, oldInfo, newInfo entryTrackInfo, err error) {
	p1, p2 := MD5Path(e.FullPath)
	parentP1, parentP2 := int64(0), int64(0)
	if e.FullPath != "" {
		parentPath, ok := ParentAbsPath(e.FullPath)
		if ok {
			parentP1, parentP2 = MD5Path(parentPath)
		}
	}

	// internalFlags includes FlagXattr for in-memory statistics tracking.
	// dbFlags strips FlagXattr before writing to SQLite: the real CVMFS
	// catalog_sql.h uses bit 14 (0x4000) for kFlagDirBindMountpoint, bit 15
	// for kFlagHidden, and bit 16 for kFlagDirectIo; there is no separate
	// xattr bit — xattr presence is indicated by a non-NULL BLOB column.
	internalFlags := e.Flags()
	dbFlags := internalFlags &^ FlagXattr
	hardlinks := e.Hardlinks()
	xattrBlob := cvmfsxattr.Marshal(e.Xattr)

	// Check for an existing entry so we can replace it atomically.
	// Also read size and count chunks for entryTrackInfo (task #12).
	var existingFlags int
	var existingXattrPresent bool
	var existingSize int64
	if scanErr := tx.QueryRow(
		"SELECT flags, (xattr IS NOT NULL), size FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?",
		p1, p2,
	).Scan(&existingFlags, &existingXattrPresent, &existingSize); scanErr == nil {
		hasExisting = true
		if existingXattrPresent {
			existingFlags |= FlagXattr
		}
		var existingChunks int
		_ = tx.QueryRow(
			"SELECT COUNT(*) FROM chunks WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
		).Scan(&existingChunks)
		oldInfo = entryTrackInfo{flags: existingFlags, size: existingSize, chunkCount: existingChunks}

		if _, delErr := tx.Exec(
			"DELETE FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
		); delErr != nil {
			return false, entryTrackInfo{}, entryTrackInfo{}, fmt.Errorf("removing old entry %s: %w", e.FullPath, delErr)
		}
	}

	// Insert the new catalog row using dbFlags (FlagXattr masked out).
	if _, insErr := tx.Exec(`
		INSERT INTO catalog (
			md5path_1, md5path_2, parent_1, parent_2, hardlinks, hash, size, mode,
			mtime, mtimens, flags, name, symlink, uid, gid, xattr
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p1, p2, parentP1, parentP2, hardlinks, e.Hash, e.Size,
		UnixMode(e.Mode), e.Mtime, e.MtimeNs, dbFlags, e.Name,
		e.Symlink, e.UID, e.GID, xattrBlob,
	); insErr != nil {
		return false, entryTrackInfo{}, entryTrackInfo{}, fmt.Errorf("inserting entry %s: %w", e.FullPath, insErr)
	}

	// Replace chunk rows atomically within the same transaction.
	if _, delErr := tx.Exec(
		"DELETE FROM chunks WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	); delErr != nil {
		return false, entryTrackInfo{}, entryTrackInfo{}, fmt.Errorf("clearing old chunks for %s: %w", e.FullPath, delErr)
	}
	for _, ch := range e.Chunks {
		if _, insErr := tx.Exec(
			"INSERT INTO chunks (md5path_1, md5path_2, offset, size, hash) VALUES (?, ?, ?, ?, ?)",
			p1, p2, ch.Offset, ch.Size, ch.Hash,
		); insErr != nil {
			return false, entryTrackInfo{}, entryTrackInfo{}, fmt.Errorf("inserting chunk at offset %d for %s: %w", ch.Offset, e.FullPath, insErr)
		}
	}

	newInfo = entryTrackInfo{flags: internalFlags, size: e.Size, chunkCount: len(e.Chunks)}
	return hasExisting, oldInfo, newInfo, nil
}

// insertOneInTx inserts a single brand-new entry within an existing transaction.
// It is the fast path used by BatchInsert for fresh (empty) catalogs: there is
// no existence check (SELECT), no old-row cleanup (DELETE catalog / DELETE
// chunks), and no old-statistics tracking.  The caller guarantees that no row
// with the same md5path_1/md5path_2 already exists; if a duplicate is
// encountered the INSERT fails and the enclosing transaction is rolled back.
//
// Returns newInfo for post-commit statistics tracking via trackAdd.
func insertOneInTx(tx *sql.Tx, e Entry) (newInfo entryTrackInfo, err error) {
	p1, p2 := MD5Path(e.FullPath)
	parentP1, parentP2 := int64(0), int64(0)
	if e.FullPath != "" {
		parentPath, ok := ParentAbsPath(e.FullPath)
		if ok {
			parentP1, parentP2 = MD5Path(parentPath)
		}
	}

	internalFlags := e.Flags()
	dbFlags := internalFlags &^ FlagXattr
	hardlinks := e.Hardlinks()
	xattrBlob := cvmfsxattr.Marshal(e.Xattr)

	if _, insErr := tx.Exec(`
		INSERT INTO catalog (
			md5path_1, md5path_2, parent_1, parent_2, hardlinks, hash, size, mode,
			mtime, mtimens, flags, name, symlink, uid, gid, xattr
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p1, p2, parentP1, parentP2, hardlinks, e.Hash, e.Size,
		UnixMode(e.Mode), e.Mtime, e.MtimeNs, dbFlags, e.Name,
		e.Symlink, e.UID, e.GID, xattrBlob,
	); insErr != nil {
		return entryTrackInfo{}, fmt.Errorf("inserting entry %s: %w", e.FullPath, insErr)
	}

	// Insert chunk rows.  No prior DELETE is needed: the catalog is fresh so
	// there are no pre-existing chunks for this path.
	for _, ch := range e.Chunks {
		if _, insErr := tx.Exec(
			"INSERT INTO chunks (md5path_1, md5path_2, offset, size, hash) VALUES (?, ?, ?, ?, ?)",
			p1, p2, ch.Offset, ch.Size, ch.Hash,
		); insErr != nil {
			return entryTrackInfo{}, fmt.Errorf("inserting chunk at offset %d for %s: %w", ch.Offset, e.FullPath, insErr)
		}
	}

	return entryTrackInfo{flags: internalFlags, size: e.Size, chunkCount: len(e.Chunks)}, nil
}

// upsertEntry inserts or replaces a single entry in the catalog table.
//
// All catalog and chunk mutations are wrapped in a single transaction so that a
// crash between the DELETE and INSERT cannot leave the catalog in an inconsistent
// state.  The statistics delta is updated only after the transaction commits
// successfully, so a rollback never leaves the in-memory delta permanently wrong.
func (c *Catalog) upsertEntry(e Entry) error {
	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction for %s: %w", e.FullPath, err)
	}
	defer tx.Rollback() // no-op after Commit

	hasExisting, oldInfo, newInfo, err := upsertOneInTx(tx, e)
	if err != nil {
		return err
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("committing upsert for %s: %w", e.FullPath, commitErr)
	}

	// Update the in-memory statistics delta only after the transaction commits.
	if hasExisting {
		c.trackRemove(oldInfo)
	}
	c.trackAdd(newInfo)
	return nil
}

// Upsert inserts or replaces an entry in the catalog.
func (c *Catalog) Upsert(e Entry) error {
	return c.upsertEntry(e)
}

// BatchUpsert inserts or replaces multiple entries in a single SQLite
// transaction.  This is dramatically faster than calling Upsert for each entry
// individually because it eliminates the per-entry BEGIN/COMMIT overhead:
//
//   - Without batching: N entries → N transactions → N journal writes + N commits.
//   - With batching:    N entries → 1 transaction  → 1 journal write  + 1 commit.
//
// For a typical publish with 5 000 new files this saves ~4 999 unnecessary
// transaction round-trips.  The in-memory statistics delta is updated atomically
// after the single transaction commits, so a rollback (on error) never corrupts
// the delta.
//
// Mixed batches (upserts + replacements) are handled transparently: each entry
// is checked for an existing row and replaced if found, or inserted fresh if not.
// Errors from any single entry abort the entire transaction; no partial state
// is written to the catalog.
func (c *Catalog) BatchUpsert(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning batch upsert transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	// Collect per-entry tracking info so statistics can be applied atomically
	// after the transaction commits — updating post-commit means a rollback
	// cannot leave the in-memory delta in a permanently wrong state.
	type deltaRecord struct {
		hasExisting bool
		oldInfo     entryTrackInfo
		newInfo     entryTrackInfo
	}
	records := make([]deltaRecord, 0, len(entries))

	for _, e := range entries {
		hasExisting, oldInfo, newInfo, upsertErr := upsertOneInTx(tx, e)
		if upsertErr != nil {
			return upsertErr
		}
		records = append(records, deltaRecord{hasExisting, oldInfo, newInfo})
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("committing batch upsert (%d entries): %w", len(entries), commitErr)
	}

	for _, r := range records {
		if r.hasExisting {
			c.trackRemove(r.oldInfo)
		}
		c.trackAdd(r.newInfo)
	}
	return nil
}

// BatchInsert inserts multiple brand-new entries into a fresh (empty) catalog
// in a single SQLite transaction.  It is faster than BatchUpsert for the
// common case of building a new catalog from scratch because it eliminates the
// per-entry existence check that BatchUpsert performs:
//
//   - BatchUpsert:  N entries → N SELECT + N INSERT  (+ optional DELETE on hit)
//   - BatchInsert:  N entries → N INSERT  (no SELECT, no DELETE)
//
// For a publish with 10 000 files this removes 10 000 otherwise-wasted SELECT
// queries from the batch transaction.
//
// The caller guarantees that the catalog is empty (or at least contains no rows
// whose md5path_1/md5path_2 collides with any entry in the batch).  If a
// duplicate path is encountered the INSERT fails and the whole transaction is
// rolled back, returning an error.  Use BatchUpsert when entries may already
// exist and silent replacement is desired.
func (c *Catalog) BatchInsert(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning batch insert transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	infos := make([]entryTrackInfo, 0, len(entries))
	for _, e := range entries {
		info, insErr := insertOneInTx(tx, e)
		if insErr != nil {
			return insErr
		}
		infos = append(infos, info)
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("committing batch insert (%d entries): %w", len(entries), commitErr)
	}

	for _, info := range infos {
		c.trackAdd(info)
	}
	return nil
}

// Remove deletes an entry (and its chunks) from the catalog by its absolute path.
//
// The SELECT, catalog DELETE, and chunk DELETE are all wrapped in a single
// transaction so a crash midway cannot leave orphan chunks or a missing entry.
// Chunk DELETE errors are propagated rather than silently ignored.
// trackRemove is called only after the transaction commits successfully, so a
// rollback never leaves the in-memory delta permanently wrong.
func (c *Catalog) Remove(absPath string) error {
	p1, p2 := MD5Path(absPath)

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction for remove %s: %w", absPath, err)
	}
	defer tx.Rollback() // no-op after Commit

	// Fetch existing flags, size, and chunk count so trackRemove can correctly
	// decrement all counters (size and chunk counters were added in task #12).
	// Also read whether the xattr BLOB is non-NULL: FlagXattr is never stored
	// in the flags column, so we reconstruct it here for trackRemove.
	// If the entry is absent we return ErrNotFound so callers that treat deletion
	// as idempotent can distinguish "not found" from a genuine database error.
	var flags int
	var xattrPresent bool
	var size int64
	scanErr := tx.QueryRow(
		"SELECT flags, (xattr IS NOT NULL), size FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?",
		p1, p2,
	).Scan(&flags, &xattrPresent, &size)
	if errors.Is(scanErr, sql.ErrNoRows) {
		return ErrNotFound
	}
	if scanErr != nil {
		return fmt.Errorf("querying entry %s: %w", absPath, scanErr)
	}
	if xattrPresent {
		flags |= FlagXattr
	}
	var chunkCount int
	_ = tx.QueryRow(
		"SELECT COUNT(*) FROM chunks WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	).Scan(&chunkCount)

	// Delete catalog entry.
	if _, delErr := tx.Exec(
		"DELETE FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	); delErr != nil {
		return fmt.Errorf("removing %s: %w", absPath, delErr)
	}

	// Delete associated chunk rows — error is now propagated.
	if _, delErr := tx.Exec(
		"DELETE FROM chunks WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	); delErr != nil {
		return fmt.Errorf("removing chunks for %s: %w", absPath, delErr)
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("committing remove for %s: %w", absPath, commitErr)
	}

	// Update the in-memory statistics delta only after the transaction commits.
	c.trackRemove(entryTrackInfo{flags: flags, size: size, chunkCount: chunkCount})
	return nil
}

// Close releases the underlying database connection.  It is safe to call on a
// Catalog that has already been closed or finalised (returns nil in that case).
//
// closeOnce ensures that concurrent callers (e.g. a deferred Close and an
// error-path Close in a goroutine) each block until the first call completes
// and then return without performing a double-close.
func (c *Catalog) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		if c.db != nil {
			closeErr = c.db.Close()
			c.db = nil
		}
	})
	return closeErr
}

// AddNestedMount adds a nested catalog entry and updates the directory's FlagDirNestedMount.
//
// The nested_catalogs table uses the CVMFS native schema (catalog_sql.cc line 273):
//
//	path TEXT PRIMARY KEY, sha1 TEXT, size INTEGER
//
// sha1 is stored as a plain 40-char hex string WITHOUT the content-type suffix,
// matching WritableCatalog::InsertNestedCatalog which calls content_hash.ToString()
// (ToString() defaults to with_suffix=false — see hash.h line 241).
//
// The INSERT into nested_catalogs and the UPDATE of the directory entry's flags
// are wrapped in a single transaction so a crash between them cannot leave the
// catalog in a structurally inconsistent state.  c.delta.SelfNested is
// incremented only after the transaction commits, so a rollback never
// permanently corrupts the in-memory statistics delta.
func (c *Catalog) AddNestedMount(mountPath, hashHex string, size int64) error {
	p1, p2 := MD5Path(mountPath)
	parentPath, _ := ParentAbsPath(mountPath)
	pp1, pp2 := MD5Path(parentPath)

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction for nested mount at %s: %w", mountPath, err)
	}
	defer tx.Rollback() // no-op after Commit

	if _, err := tx.Exec(`
		INSERT INTO nested_catalogs (path, sha1, size) VALUES (?, ?, ?)
	`, mountPath, hashHex, size); err != nil {
		return fmt.Errorf("inserting nested catalog at %s: %w", mountPath, err)
	}

	// Guarantee the mount-point directory entry exists in the catalog table.
	//
	// The receiver's DiffRec walker calls Listing(parent) which queries
	// "SELECT … FROM catalog WHERE parent_1=? AND parent_2=?".  If the
	// mount-point row is absent the diff walker never sees the directory, never
	// calls GraftNestedCatalog, and the nested catalog is silently dropped.
	//
	// The path-routing step may or may not have already inserted this entry
	// (e.g. it is missing when the tar root "." entry was routed to the child
	// catalog rather than this parent).  INSERT OR IGNORE + UPDATE FLAGS is
	// therefore idempotent: it creates a minimal dir entry when none exists and
	// only OR-sets FlagDirNestedMount when one already does.
	name := filepath.Base(mountPath)
	dirMode := UnixMode(fs.ModeDir | 0o755)
	dirFlags := FlagDir | FlagDirNestedMount
	now := time.Now().Unix()
	if _, err := tx.Exec(`
		INSERT OR IGNORE INTO catalog (
			md5path_1, md5path_2, parent_1, parent_2,
			hardlinks, hash, size, mode, mtime, mtimens,
			flags, name, symlink, uid, gid, xattr
		) VALUES (?, ?, ?, ?, 1, NULL, 4096, ?, ?, 0, ?, ?, '', 0, 0, NULL)
	`, p1, p2, pp1, pp2, dirMode, now, dirFlags, name); err != nil {
		return fmt.Errorf("inserting mount dir entry for %s: %w", mountPath, err)
	}
	if _, err := tx.Exec(`
		UPDATE catalog SET flags = flags | ? WHERE md5path_1 = ? AND md5path_2 = ?
	`, FlagDirNestedMount, p1, p2); err != nil {
		return fmt.Errorf("updating directory flags for nested mount at %s: %w", mountPath, err)
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("committing nested mount for %s: %w", mountPath, commitErr)
	}

	// Update in-memory delta only after the transaction commits.
	c.delta.SelfNested++
	return nil
}

// FindNestedMount checks whether absPath is a nested catalog mount point in
// this catalog.  If found, it returns the compressed catalog hash (hex) and
// compressed size stored in the nested_catalogs table.
// Returns found=false (no error) when no row exists for absPath.
//
// nested_catalogs uses the CVMFS native schema: path TEXT PRIMARY KEY, sha1 TEXT.
// sha1 is the plain 40-char hex hash (no suffix) — returned directly.
func (c *Catalog) FindNestedMount(absPath string) (hashHex string, size int64, found bool, err error) {
	var sha1Hex string
	var sz int64
	row := c.db.QueryRow(
		"SELECT sha1, size FROM nested_catalogs WHERE path = ?", absPath)
	if scanErr := row.Scan(&sha1Hex, &sz); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return "", 0, false, nil
		}
		return "", 0, false, fmt.Errorf("querying nested catalog at %q: %w", absPath, scanErr)
	}
	return sha1Hex, sz, true, nil
}

// UpdateNestedMount replaces the hash and size of an existing nested catalog
// row identified by absPath.  Returns an error if no row is found (the caller
// must ensure the nested catalog entry was previously inserted via AddNestedMount).
//
// hashHex is the plain 40-char hex hash (no suffix), stored as TEXT — matching
// the CVMFS native nested_catalogs schema.
func (c *Catalog) UpdateNestedMount(absPath, hashHex string, size int64) error {
	res, err := c.db.Exec(
		"UPDATE nested_catalogs SET sha1 = ?, size = ? WHERE path = ?",
		hashHex, size, absPath)
	if err != nil {
		return fmt.Errorf("updating nested catalog at %q: %w", absPath, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("no nested_catalogs row found at %q", absPath)
	}
	return nil
}

// Finalize increments revision, sets last_modified, flushes statistics delta,
// compresses the database, writes it to destDir in CAS format, and returns the
// hash (plain hex, no suffix) and the delta that was flushed.
func (c *Catalog) Finalize(destDir string) (hashHex string, delta Statistics, err error) {
	now := time.Now().Unix()

	// Wrap the three metadata writes (revision bump, last_modified, statistics
	// flush) in a single transaction so a crash between any two cannot leave
	// the catalog partially updated.
	tx, err := c.db.Begin()
	if err != nil {
		return "", Statistics{}, fmt.Errorf("beginning finalize transaction: %w", err)
	}
	defer tx.Rollback() // no-op after Commit

	if _, err := tx.Exec(`
		UPDATE properties SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)
		WHERE key = 'revision'
	`); err != nil {
		return "", Statistics{}, fmt.Errorf("incrementing revision: %w", err)
	}

	if _, err := tx.Exec(`
		UPDATE properties SET value = ? WHERE key = 'last_modified'
	`, fmt.Sprintf("%d", now)); err != nil {
		return "", Statistics{}, fmt.Errorf("updating last_modified: %w", err)
	}

	// Flush the in-memory statistics delta to the key-value statistics table.
	// Each counter is a separate row: UPDATE statistics SET value = value + ?
	// WHERE counter = ?  This matches the (counter TEXT PRIMARY KEY, value INTEGER)
	// schema that cvmfs_receiver's SqlGetCounter expects.
	deltaMap := map[string]int64{
		"self_regular":               c.delta.SelfRegular,
		"self_symlink":               c.delta.SelfSymlink,
		"self_dir":                   c.delta.SelfDir,
		"self_nested":                c.delta.SelfNested,
		"self_xattr":                 c.delta.SelfXattr,
		"self_external":              c.delta.SelfExternal,
		"self_special":               c.delta.SelfSpecial,
		"self_chunked":               c.delta.SelfChunked,
		"self_chunks":                c.delta.SelfChunks,
		"self_file_size":             c.delta.SelfFileSize,
		"self_chunked_size":          c.delta.SelfChunkedSize,
		"self_external_file_size":    c.delta.SelfExternalFileSize,
		"subtree_regular":            c.delta.SubtreeRegular,
		"subtree_symlink":            c.delta.SubtreeSymlink,
		"subtree_dir":                c.delta.SubtreeDir,
		"subtree_nested":             c.delta.SubtreeNested,
		"subtree_xattr":              c.delta.SubtreeXattr,
		"subtree_external":           c.delta.SubtreeExternal,
		"subtree_special":            c.delta.SubtreeSpecial,
		"subtree_chunked":            c.delta.SubtreeChunked,
		"subtree_chunks":             c.delta.SubtreeChunks,
		"subtree_file_size":          c.delta.SubtreeFileSize,
		"subtree_chunked_size":       c.delta.SubtreeChunkedSize,
		"subtree_external_file_size": c.delta.SubtreeExternalFileSize,
	}
	for counter, delta := range deltaMap {
		if _, err := tx.Exec(
			`UPDATE statistics SET value = value + ? WHERE counter = ?`, delta, counter,
		); err != nil {
			return "", Statistics{}, fmt.Errorf("flushing statistics counter %s: %w", counter, err)
		}
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return "", Statistics{}, fmt.Errorf("committing finalize transaction: %w", commitErr)
	}
	savedDelta := c.delta

	// Force a full WAL checkpoint before closing, in case WAL mode was
	// enabled (e.g. via Open() on an existing catalog).  This ensures that
	// every changed page is written into the main .db file before we read it
	// with os.ReadFile below.  For DELETE-mode databases (the default for
	// Create()) this is a no-op.
	_, _ = c.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`) //nolint:errcheck // best-effort

	// Close database and nil the pointer so subsequent use panics loudly
	// rather than silently accessing a closed db handle.
	if err := c.db.Close(); err != nil {
		return "", Statistics{}, fmt.Errorf("closing database: %w", err)
	}
	c.db = nil

	// Read raw SQLite bytes
	raw, err := os.ReadFile(c.dbPath)
	if err != nil {
		return "", Statistics{}, fmt.Errorf("reading database: %w", err)
	}

	// Compress with zlib.
	//
	// Use DefaultCompression (level 6) instead of BestCompression (level 9).
	// BestCompression is 3–10× slower than DefaultCompression with only ~5–15%
	// smaller output.  The root catalog is re-compressed on every commit: its
	// size is O(total files in that catalog scope), so BestCompression makes
	// Finalize O(N × compression_factor) where N is the accumulated file count.
	// DefaultCompression keeps compression time proportional only to the amount
	// of data changed in this publish while still yielding good ratio.
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.DefaultCompression)
	if err != nil {
		return "", Statistics{}, fmt.Errorf("creating zlib writer: %w", err)
	}
	if _, err := w.Write(raw); err != nil {
		return "", Statistics{}, fmt.Errorf("compressing: %w", err)
	}
	if err := w.Close(); err != nil {
		return "", Statistics{}, fmt.Errorf("closing zlib writer: %w", err)
	}

	compressedBytes := buf.Bytes()

	// Hash compressed bytes (SHA-1 — CVMFS CAS convention)
	hash, _, err := cvmfshash.HashReader(bytes.NewReader(compressedBytes))
	if err != nil {
		return "", Statistics{}, fmt.Errorf("hashing: %w", err)
	}

	// Create CAS directory structure: data/XY/hashC
	casPath := cvmfshash.ObjectPath(hash) + "C"
	fullPath := filepath.Join(destDir, casPath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return "", Statistics{}, fmt.Errorf("creating directory: %w", err)
	}

	// Write compressed file
	if err := os.WriteFile(fullPath, compressedBytes, 0o644); err != nil {
		return "", Statistics{}, fmt.Errorf("writing compressed catalog: %w", err)
	}

	// Remove the temporary SQLite file now that its content has been durably
	// written to the CAS object.  This prevents staging directories from
	// accumulating one .db file per catalog per job.  Best-effort: a failure
	// here is non-fatal because the CAS object is already written and the
	// orchestrator removes the entire staging directory on job completion.
	_ = os.Remove(c.dbPath) //nolint:errcheck

	return hash, savedDelta, nil
}

// LookupFileHash returns the content hash and hash algorithm of a regular file
// stored at absPath in this catalog.  The hash algorithm is extracted from the
// entry's flags column.  Returns ("", 0, false, nil) when no entry exists for
// the path or when the stored entry has no content hash (e.g. directories or
// symlinks that were written without a hash).
func (c *Catalog) LookupFileHash(absPath string) (hashHex string, algo HashAlgo, found bool, err error) {
	p1, p2 := MD5Path(absPath)
	var hashBlob []byte
	var flags int
	scanErr := c.db.QueryRow(
		"SELECT hash, flags FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	).Scan(&hashBlob, &flags)
	if errors.Is(scanErr, sql.ErrNoRows) {
		return "", 0, false, nil
	}
	if scanErr != nil {
		return "", 0, false, fmt.Errorf("looking up %q: %w", absPath, scanErr)
	}
	if len(hashBlob) == 0 {
		return "", 0, false, nil
	}
	return hex.EncodeToString(hashBlob), HashAlgoFromFlags(flags), true, nil
}

// SchemaVersion returns the schema version.
func (c *Catalog) SchemaVersion() string {
	return "2.5"
}

// GetStatistics reads current statistics from the key-value statistics table.
// Each CVMFS counter is stored as a separate row: (counter TEXT, value INTEGER).
func (c *Catalog) GetStatistics() (*Statistics, error) {
	rows, err := c.db.Query(`SELECT counter, value FROM statistics`)
	if err != nil {
		return nil, fmt.Errorf("querying statistics: %w", err)
	}
	defer rows.Close()

	stats := &Statistics{}
	for rows.Next() {
		var counter string
		var value int64
		if err := rows.Scan(&counter, &value); err != nil {
			return nil, fmt.Errorf("scanning statistics row: %w", err)
		}
		switch counter {
		case "self_regular":
			stats.SelfRegular = value
		case "self_symlink":
			stats.SelfSymlink = value
		case "self_dir":
			stats.SelfDir = value
		case "self_nested":
			stats.SelfNested = value
		case "self_xattr":
			stats.SelfXattr = value
		case "self_external":
			stats.SelfExternal = value
		case "self_special":
			stats.SelfSpecial = value
		case "self_chunked":
			stats.SelfChunked = value
		case "self_chunks":
			stats.SelfChunks = value
		case "self_file_size":
			stats.SelfFileSize = value
		case "self_chunked_size":
			stats.SelfChunkedSize = value
		case "self_external_file_size":
			stats.SelfExternalFileSize = value
		case "subtree_regular":
			stats.SubtreeRegular = value
		case "subtree_symlink":
			stats.SubtreeSymlink = value
		case "subtree_dir":
			stats.SubtreeDir = value
		case "subtree_nested":
			stats.SubtreeNested = value
		case "subtree_xattr":
			stats.SubtreeXattr = value
		case "subtree_external":
			stats.SubtreeExternal = value
		case "subtree_special":
			stats.SubtreeSpecial = value
		case "subtree_chunked":
			stats.SubtreeChunked = value
		case "subtree_chunks":
			stats.SubtreeChunks = value
		case "subtree_file_size":
			stats.SubtreeFileSize = value
		case "subtree_chunked_size":
			stats.SubtreeChunkedSize = value
		case "subtree_external_file_size":
			stats.SubtreeExternalFileSize = value
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating statistics rows: %w", err)
	}
	return stats, nil
}
