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
// in the catalog.  Callers that treat deletion as idempotent (e.g. Merge)
// can check for this sentinel and continue rather than fail (Fix N4).
var ErrNotFound = errors.New("catalog: entry not found")

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
	closeOnce sync.Once // Fix N5
}

// Statistics holds all counter columns from the statistics table.
type Statistics struct {
	SelfRegular     int64
	SelfSymlink     int64
	SelfDir         int64
	SelfNested      int64
	SubtreeRegular  int64
	SubtreeSymlink  int64
	SubtreeDir      int64
	SubtreeNested   int64
	SelfXattr       int64
	SubtreeXattr    int64
	SelfExternal    int64
	SubtreeExternal int64
	SelfSpecial     int64
	SubtreeSpecial  int64
}

// Create creates a new CVMFS catalog database at dbPath with the given rootPrefix.
func Create(dbPath, rootPrefix string) (*Catalog, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Create all required tables for CVMFS schema 2.5.
	//
	// Fix N3/L3: UNIQUE constraints are expressed as explicit named indexes
	// (rather than inline column constraints) so that the same
	// CREATE UNIQUE INDEX IF NOT EXISTS statements can be issued in Open()
	// to enforce the constraints on catalogs created by older code.
	//
	// Fix N6: nested_catalogs also gets a UNIQUE constraint so that calling
	// AddNestedMount twice for the same path is rejected at the DB level.
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

	// Fix N3/L3/N6: enforce uniqueness via named indexes.
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
		"schema":           "2.5",
		"schema_revision":  "7",
		"root_prefix":      rootPrefix,
		"revision":         "0",
		"last_modified":    fmt.Sprintf("%d", now),
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

	// Insert root directory entry
	isNestedRoot := rootPrefix != ""
	rootEntry := Entry{
		FullPath:      "",
		Name:          "",
		Mode:          fs.ModeDir | 0o755,
		Size:          4096,
		Mtime:         now,
		MtimeNs:       0,
		UID:           0,
		GID:           0,
		LinkCount:     1,
		HashAlgo:      HashSha256,
		CompAlgo:      CompZlib,
		IsNestedRoot:  isNestedRoot,
	}

	if err := c.upsertEntry(rootEntry); err != nil {
		// Fix N8: use c.Close() so the closeOnce gate is respected and the
		// idempotent Close path is consistent with the rest of the package.
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

	// NOTE: applyUniqueIndexes is intentionally NOT called here.
	//
	// Open() is used for read-only access to existing catalogs — including
	// catalogs created by cvmfs_server mkfs / cvmfs_server publish, which use
	// the native CVMFS schema (nested_catalogs has "path TEXT PRIMARY KEY", not
	// our md5path_1/md5path_2 columns).  Attempting to create our custom indexes
	// on a foreign catalog fails with "no such column: md5path_1".
	//
	// Our unique indexes are write-path guards; they are applied in Create() where
	// we control the schema.  Catalogs we create already have the indexes on disk,
	// so reopening them via Open() requires no retrofitting.

	return &Catalog{
		db:         db,
		dbPath:     dbPath,
		rootPrefix: rootPrefix,
	}, nil
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

// trackAdd increments the appropriate self-count based on entry flags.
// FlagXattr is orthogonal to the file-type counters and is tracked separately.
func (c *Catalog) trackAdd(flags int) {
	switch {
	case flags&FlagDir != 0:
		c.delta.SelfDir++
	case flags&FlagLink != 0:
		c.delta.SelfSymlink++
	default:
		c.delta.SelfRegular++
	}
	if flags&FlagXattr != 0 {
		c.delta.SelfXattr++
	}
}

// trackRemove decrements the appropriate self-count based on entry flags.
// FlagXattr is orthogonal to the file-type counters and is tracked separately.
func (c *Catalog) trackRemove(flags int) {
	switch {
	case flags&FlagDir != 0:
		c.delta.SelfDir--
	case flags&FlagLink != 0:
		c.delta.SelfSymlink--
	default:
		c.delta.SelfRegular--
	}
	if flags&FlagXattr != 0 {
		c.delta.SelfXattr--
	}
}

// upsertEntry inserts or replaces a single entry in the catalog table.
//
// Fix C3: all catalog and chunk mutations are wrapped in a single transaction
// so that a crash between the DELETE and INSERT cannot leave the catalog in an
// inconsistent state.
//
// Fix C1: the statistics delta is updated only after the transaction commits
// successfully, so a rollback never leaves the in-memory delta permanently wrong.
func (c *Catalog) upsertEntry(e Entry) error {
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

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction for %s: %w", e.FullPath, err)
	}
	defer tx.Rollback() // no-op after Commit

	// Check for an existing entry so we can replace it atomically.
	// Read both the stored flags and whether the xattr BLOB is non-NULL so
	// that trackRemove can correctly decrement SelfXattr (FlagXattr is never
	// stored in the DB flags column; we reconstruct it from the BLOB).
	var existingFlags int
	var existingXattrPresent bool
	hasExisting := false
	if scanErr := tx.QueryRow(
		"SELECT flags, (xattr IS NOT NULL) FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	).Scan(&existingFlags, &existingXattrPresent); scanErr == nil {
		hasExisting = true
		if existingXattrPresent {
			existingFlags |= FlagXattr
		}
		if _, delErr := tx.Exec(
			"DELETE FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
		); delErr != nil {
			return fmt.Errorf("removing old entry %s: %w", e.FullPath, delErr)
		}
	}

	// Serialize xattr map to the CVMFS binary TLV BLOB.
	// cvmfsxattr.Marshal returns nil for an empty map, keeping the column NULL
	// for entries with no extended attributes.
	xattrBlob := cvmfsxattr.Marshal(e.Xattr)

	// Insert the new row using dbFlags (FlagXattr masked out).
	if _, insErr := tx.Exec(`
		INSERT INTO catalog (
			md5path_1, md5path_2, parent_1, parent_2, hardlinks, hash, size, mode,
			mtime, mtimens, flags, name, symlink, uid, gid, xattr
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p1, p2, parentP1, parentP2, hardlinks, e.Hash, e.Size,
		UnixMode(e.Mode), e.Mtime, e.MtimeNs, dbFlags, e.Name,
		e.Symlink, e.UID, e.GID, xattrBlob,
	); insErr != nil {
		return fmt.Errorf("inserting entry %s: %w", e.FullPath, insErr)
	}

	// Replace chunk rows atomically within the same transaction.
	if _, delErr := tx.Exec(
		"DELETE FROM chunks WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	); delErr != nil {
		return fmt.Errorf("clearing old chunks for %s: %w", e.FullPath, delErr)
	}
	for _, ch := range e.Chunks {
		if _, insErr := tx.Exec(
			"INSERT INTO chunks (md5path_1, md5path_2, offset, size, hash) VALUES (?, ?, ?, ?, ?)",
			p1, p2, ch.Offset, ch.Size, ch.Hash,
		); insErr != nil {
			return fmt.Errorf("inserting chunk at offset %d for %s: %w", ch.Offset, e.FullPath, insErr)
		}
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("committing upsert for %s: %w", e.FullPath, commitErr)
	}

	// Update the in-memory statistics delta only after the transaction has
	// committed.  Updating before commit would permanently corrupt the delta
	// if the transaction were subsequently rolled back.
	if hasExisting {
		c.trackRemove(existingFlags) // existingFlags already has FlagXattr set if BLOB was non-NULL
	}
	c.trackAdd(internalFlags) // internalFlags has FlagXattr set if new entry has xattrs
	return nil
}

// Upsert inserts or replaces an entry in the catalog.
func (c *Catalog) Upsert(e Entry) error {
	return c.upsertEntry(e)
}

// Remove deletes an entry (and its chunks) from the catalog by its absolute path.
//
// Fix C3: the SELECT, catalog DELETE, and chunk DELETE are wrapped in a single
// transaction so a crash midway cannot leave orphan chunks or a missing entry.
//
// Fix C1: trackRemove is called only after the transaction commits successfully,
// so a rollback never leaves the in-memory delta permanently wrong.
//
// Fix M2: chunk DELETE errors are now propagated instead of silently ignored.
func (c *Catalog) Remove(absPath string) error {
	p1, p2 := MD5Path(absPath)

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction for Remove %s: %w", absPath, err)
	}
	defer tx.Rollback() // no-op after Commit

	// Fetch existing flags so we can decrement the right counter after commit.
	// Also read whether the xattr BLOB is non-NULL: FlagXattr is never stored
	// in the flags column, so we reconstruct it here for trackRemove.
	// Fix N4: if the entry is absent we return ErrNotFound so callers that
	// treat deletion as idempotent (e.g. Merge) can distinguish "not found"
	// from a genuine database error.
	var flags int
	var xattrPresent bool
	scanErr := tx.QueryRow(
		"SELECT flags, (xattr IS NOT NULL) FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	).Scan(&flags, &xattrPresent)
	if errors.Is(scanErr, sql.ErrNoRows) {
		return ErrNotFound
	}
	if scanErr != nil {
		return fmt.Errorf("querying entry %s: %w", absPath, scanErr)
	}
	if xattrPresent {
		flags |= FlagXattr
	}

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
		return fmt.Errorf("committing Remove for %s: %w", absPath, commitErr)
	}

	// Update the in-memory statistics delta only after the transaction commits.
	c.trackRemove(flags)
	return nil
}

// Close releases the underlying database connection.  It is safe to call on a
// Catalog that has already been closed or finalised (returns nil in that case).
//
// Fix N5: closeOnce ensures that concurrent callers (e.g. a deferred Close and
// an error-path Close in a goroutine) each block until the first call completes
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
//   path TEXT PRIMARY KEY, sha1 TEXT, size INTEGER
// sha1 is stored as a plain 40-char hex string WITHOUT the content-type suffix,
// matching WritableCatalog::InsertNestedCatalog which calls content_hash.ToString()
// (ToString() defaults to with_suffix=false — see hash.h line 241).
//
// Fix N1/C3: the INSERT into nested_catalogs and the UPDATE of the directory
// entry's flags are wrapped in a single transaction so a crash between them
// cannot leave the catalog in a structurally inconsistent state.
//
// Fix N1/C1: c.delta.SelfNested is incremented only after the transaction
// commits successfully, so a rollback never permanently corrupts the in-memory
// statistics delta.
func (c *Catalog) AddNestedMount(mountPath, hashHex string, size int64) error {
	p1, p2 := MD5Path(mountPath)

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction for AddNestedMount %s: %w", mountPath, err)
	}
	defer tx.Rollback() // no-op after Commit

	if _, err := tx.Exec(`
		INSERT INTO nested_catalogs (path, sha1, size) VALUES (?, ?, ?)
	`, mountPath, hashHex, size); err != nil {
		return fmt.Errorf("inserting nested catalog at %s: %w", mountPath, err)
	}

	if _, err := tx.Exec(`
		UPDATE catalog SET flags = flags | ? WHERE md5path_1 = ? AND md5path_2 = ?
	`, FlagDirNestedMount, p1, p2); err != nil {
		return fmt.Errorf("updating directory flags for nested mount at %s: %w", mountPath, err)
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("committing AddNestedMount for %s: %w", mountPath, commitErr)
	}

	// Update in-memory delta only after the transaction commits (Fix N1/C1).
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

	// Fix N2: wrap the three metadata writes in a single transaction so a
	// crash between any two cannot leave the catalog partially updated
	// (e.g. revision bumped but statistics not flushed).
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
		"self_regular":           c.delta.SelfRegular,
		"self_symlink":           c.delta.SelfSymlink,
		"self_dir":               c.delta.SelfDir,
		"self_nested":            c.delta.SelfNested,
		"self_xattr":             c.delta.SelfXattr,
		"self_external":          c.delta.SelfExternal,
		"self_special":           c.delta.SelfSpecial,
		"self_chunked":           0,
		"self_chunks":            0,
		"self_file_size":         0,
		"self_chunked_size":      0,
		"self_external_file_size": 0,
		"subtree_regular":        c.delta.SubtreeRegular,
		"subtree_symlink":        c.delta.SubtreeSymlink,
		"subtree_dir":            c.delta.SubtreeDir,
		"subtree_nested":         c.delta.SubtreeNested,
		"subtree_xattr":          c.delta.SubtreeXattr,
		"subtree_external":       c.delta.SubtreeExternal,
		"subtree_special":        c.delta.SubtreeSpecial,
		"subtree_chunked":        0,
		"subtree_chunks":         0,
		"subtree_file_size":      0,
		"subtree_chunked_size":   0,
		"subtree_external_file_size": 0,
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

	// Close database and nil the pointer so subsequent use panics loudly
	// rather than silently accessing a closed db (Fix H1).
	if err := c.db.Close(); err != nil {
		return "", Statistics{}, fmt.Errorf("closing database: %w", err)
	}
	c.db = nil

	// Read raw SQLite bytes
	raw, err := os.ReadFile(c.dbPath)
	if err != nil {
		return "", Statistics{}, fmt.Errorf("reading database: %w", err)
	}

	// Compress with zlib
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
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

	// Hash compressed bytes (SHA-256)
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

	// VACUUM (optional but good practice)
	// We can't VACUUM after Close(), so skip for now.

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
		// self_chunked, self_chunks, self_file_size, self_chunked_size,
		// self_external_file_size and their subtree_ counterparts are tracked
		// for completeness but not mapped to Statistics fields yet.
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating statistics rows: %w", err)
	}
	return stats, nil
}
