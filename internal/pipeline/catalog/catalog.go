package catalog

import (
	"bytes"
	"compress/zlib"
	"context"
	"database/sql"
	"fmt"
	"os"

	"cvmfs.io/prepub/internal/pipeline/unpack"
	"cvmfs.io/prepub/pkg/cvmfshash"
	"cvmfs.io/prepub/pkg/observe"

	_ "modernc.org/sqlite"
)

type Builder struct {
	db     *sql.DB
	dbPath string // stored at construction; used by Finalize (no PRAGMA needed)
	obs    *observe.Provider
}

// New creates a new catalog builder backed by a SQLite database at dbPath.
func New(dbPath string, obs *observe.Provider) (*Builder, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Create catalog table
	schema := `
	CREATE TABLE IF NOT EXISTS catalog (
		path TEXT PRIMARY KEY,
		hash TEXT,
		size INTEGER,
		mode INTEGER,
		mtime INTEGER,
		uid INTEGER,
		gid INTEGER,
		symlink TEXT,
		flags INTEGER
	);
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating schema: %w", err)
	}

	return &Builder{
		db:     db,
		dbPath: dbPath, // Critical #7: remember path here, not via PRAGMA later
		obs:    obs,
	}, nil
}

// Add adds an entry to the catalog.
func (b *Builder) Add(ctx context.Context, e unpack.FileEntry, hash string) error {
	ctx, span := b.obs.Tracer.Start(ctx, "catalog.add")
	defer span.End()

	stmt, err := b.db.PrepareContext(ctx, `
		INSERT OR REPLACE INTO catalog
		(path, hash, size, mode, mtime, uid, gid, symlink, flags)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	linkTarget := ""
	if e.LinkTarget != "" {
		linkTarget = e.LinkTarget
	}

	_, err = stmt.ExecContext(ctx,
		e.Path,
		hash,
		e.Size,
		e.Mode,
		e.ModTime.Unix(),
		0, // uid
		0, // gid
		linkTarget,
		0, // flags
	)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("inserting row: %w", err)
	}

	return nil
}

// Finalize closes the database, compresses it, writes a <dbPath>.Z file alongside
// the original, and returns the compressed file path and its SHA-256 hash.
//
// Critical #7: The database path is stored in the Builder at construction time
// and used directly here — no PRAGMA database_list query is needed or performed.
func (b *Builder) Finalize(ctx context.Context) (compPath string, dbHash string, err error) {
	_, span := b.obs.Tracer.Start(ctx, "catalog.finalize")
	defer span.End()

	rawPath := b.dbPath // always valid; set in New()

	if err := b.db.Close(); err != nil {
		span.RecordError(err)
		return "", "", fmt.Errorf("closing database: %w", err)
	}

	// Read raw SQLite bytes.
	raw, err := os.ReadFile(rawPath)
	if err != nil {
		span.RecordError(err)
		return "", "", fmt.Errorf("reading catalog db: %w", err)
	}

	// Compress into a byte slice so we can both hash and write without re-reading.
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		span.RecordError(err)
		return "", "", fmt.Errorf("creating zlib writer: %w", err)
	}
	if _, err := w.Write(raw); err != nil {
		span.RecordError(err)
		return "", "", fmt.Errorf("compressing catalog: %w", err)
	}
	if err := w.Close(); err != nil {
		span.RecordError(err)
		return "", "", fmt.Errorf("finalizing zlib stream: %w", err)
	}

	// Snapshot bytes before HashReader drains the buffer.
	compressedBytes := buf.Bytes()

	// Hash the compressed bytes.
	hash, _, err := cvmfshash.HashReader(bytes.NewReader(compressedBytes))
	if err != nil {
		span.RecordError(err)
		return "", "", fmt.Errorf("hashing catalog: %w", err)
	}

	// Write compressed file alongside the original.
	compPath = rawPath + ".Z"
	if err := os.WriteFile(compPath, compressedBytes, 0600); err != nil {
		span.RecordError(err)
		return "", "", fmt.Errorf("writing compressed catalog: %w", err)
	}

	return compPath, hash, nil
}
