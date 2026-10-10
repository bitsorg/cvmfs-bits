// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Published catalogs are fetched from the root on every PathExists and
// ReadPublishedFile, and prepub runs several of those per job while holding the
// repository's commit lock. A catalog object never changes under its hash, so
// a downloaded catalog stays valid for as long as it is kept: only the root
// hash (the manifest) has to be read fresh. The cache keeps them on disk, by
// hash, up to a size limit, evicting the least recently used.
type diskCache struct {
	dir      string
	maxBytes int64
	mu       sync.Mutex // guards lookups, inserts and eviction
}

var catalogCache atomic.Pointer[diskCache]

// partSuffix marks a download in progress; leftovers are removed at startup.
const partSuffix = ".part"

// SetCatalogCache keeps downloaded catalogs in dir, at most maxBytes of them.
// An empty dir turns the cache off: each lookup then downloads into a
// temporary directory and removes it afterwards, as before.
func SetCatalogCache(dir string, maxBytes int64) error {
	if dir == "" {
		catalogCache.Store(nil)
		return nil
	}
	if maxBytes <= 0 {
		return fmt.Errorf("catalog cache %s: size limit must be positive", dir)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("catalog cache: %w", err)
	}
	parts, _ := filepath.Glob(filepath.Join(dir, "*"+partSuffix))
	for _, p := range parts {
		_ = os.Remove(p)
	}
	c := &diskCache{dir: dir, maxBytes: maxBytes}
	c.mu.Lock()
	c.evict("")
	c.mu.Unlock()
	catalogCache.Store(c)
	return nil
}

// openPublishedCatalog returns the published catalog hashHex opened for
// reading, and the function that releases it. A missing catalog object is
// ErrCatalogNotFound, unwrapped.
func openPublishedCatalog(ctx context.Context, client *http.Client, stratum0URL, repo, hashHex string) (*Catalog, func(), error) {
	if c := catalogCache.Load(); c != nil {
		return c.open(ctx, client, stratum0URL, repo, hashHex)
	}
	tmpDir, err := os.MkdirTemp("", "cvmfs-catalog-*")
	if err != nil {
		return nil, nil, fmt.Errorf("creating temp dir: %w", err)
	}
	dbPath := filepath.Join(tmpDir, hashHex+".db")
	if err := DownloadCatalog(ctx, client, stratum0URL, repo, hashHex, dbPath); err != nil {
		os.RemoveAll(tmpDir)
		if errors.Is(err, ErrCatalogNotFound) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("downloading catalog %s: %w", hashHex, err)
	}
	cat, err := Open(dbPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, nil, fmt.Errorf("opening catalog %s: %w", hashHex, err)
	}
	return cat, func() { cat.Close(); os.RemoveAll(tmpDir) }, nil
}

// open serves hashHex from the cache, downloading it on a miss. The catalog
// is opened while the lock is held, so eviction cannot remove the file
// between the lookup and the open; an open file outlives its unlink.
func (c *diskCache) open(ctx context.Context, client *http.Client, stratum0URL, repo, hashHex string) (*Catalog, func(), error) {
	path := filepath.Join(c.dir, hashHex+".db")
	c.mu.Lock()
	if _, err := os.Stat(path); err == nil {
		now := time.Now()
		_ = os.Chtimes(path, now, now) // most recently used
		cat, err := openReadOnly(path)
		if err != nil {
			os.Remove(path) // damaged (e.g. by a crash): fetch it again next time
		}
		c.mu.Unlock()
		if err != nil {
			return nil, nil, fmt.Errorf("opening cached catalog %s: %w", hashHex, err)
		}
		return cat, func() { cat.Close() }, nil
	}
	c.mu.Unlock()

	// Downloaded outside the lock, so a slow fetch does not hold up hits; two
	// concurrent misses for one hash both download, and the second rename
	// replaces an identical file.
	part, err := os.CreateTemp(c.dir, hashHex+"-*"+partSuffix)
	if err != nil {
		return nil, nil, fmt.Errorf("catalog cache: %w", err)
	}
	part.Close()
	if err := DownloadCatalog(ctx, client, stratum0URL, repo, hashHex, part.Name()); err != nil {
		os.Remove(part.Name())
		if errors.Is(err, ErrCatalogNotFound) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("downloading catalog %s: %w", hashHex, err)
	}
	// On disk before it is published under its hash: a crash must not leave a
	// truncated catalog that every later lookup would be served.
	if err := syncFile(part.Name()); err != nil {
		os.Remove(part.Name())
		return nil, nil, fmt.Errorf("catalog cache: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := os.Rename(part.Name(), path); err != nil {
		os.Remove(part.Name())
		return nil, nil, fmt.Errorf("catalog cache: %w", err)
	}
	cat, err := openReadOnly(path)
	if err != nil {
		os.Remove(path) // not a usable catalog: do not serve it again
		return nil, nil, fmt.Errorf("opening catalog %s: %w", hashHex, err)
	}
	c.evict(path)
	return cat, func() { cat.Close() }, nil
}

// evict removes the least recently used catalogs until the cache is within
// its limit, never keep (the one just added). Called with c.mu held.
func (c *diskCache) evict(keep string) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	type file struct {
		path  string
		size  int64
		mtime time.Time
	}
	var files []file
	var total int64
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		files = append(files, file{filepath.Join(c.dir, e.Name()), info.Size(), info.ModTime()})
		total += info.Size()
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mtime.Before(files[j].mtime) })
	for _, f := range files {
		if total <= c.maxBytes {
			break
		}
		if f.path == keep {
			continue
		}
		if os.Remove(f.path) == nil {
			total -= f.size
		}
	}
}

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// openReadOnly opens a catalog without writing to it: no WAL, no indexes, and
// no locking (immutable), since a cached catalog is shared and never changes.
// One connection, opened here, so the open file survives its eviction.
func openReadOnly(dbPath string) (*Catalog, error) {
	// As a URI, so the path is escaped: '#', '?' or '%' in it would otherwise
	// cut or alter the file name.
	dsn := (&url.URL{Scheme: "file", Path: dbPath, RawQuery: "mode=ro&immutable=1"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var rootPrefix string
	err = db.QueryRow("SELECT value FROM properties WHERE key = 'root_prefix'").Scan(&rootPrefix)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		db.Close()
		return nil, fmt.Errorf("reading root_prefix: %w", err)
	}
	return &Catalog{db: db, dbPath: dbPath, rootPrefix: rootPrefix}, nil
}
