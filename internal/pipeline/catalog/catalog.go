package catalog

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"

	"cvmfs.io/prepub/internal/pipeline/unpack"
	"cvmfs.io/prepub/pkg/cvmfscatalog"
	"cvmfs.io/prepub/pkg/observe"
)

// Builder collects tar entries as CVMFS catalog entries for later processing.
// The actual SQLite catalog is built during the Merge phase in pkg/cvmfscatalog.
type Builder struct {
	entries []*cvmfscatalog.Entry
	obs     *observe.Provider
}

// New creates a new catalog builder that collects entries.
func New(dbPath string, obs *observe.Provider) (*Builder, error) {
	return &Builder{
		entries: make([]*cvmfscatalog.Entry, 0),
		obs:     obs,
	}, nil
}

// Add collects an entry from the tar.
func (b *Builder) Add(ctx context.Context, e unpack.FileEntry, hash string) error {
	ctx, span := b.obs.Tracer.Start(ctx, "catalog.add")
	defer span.End()

	// Convert unpack.FileEntry to cvmfscatalog.Entry
	name := filepath.Base(e.Path)
	if e.Path == "" || e.Path == "/" {
		name = ""
	}
	entry := &cvmfscatalog.Entry{
		FullPath:  e.Path,
		Name:      name,
		Mode:      e.Mode,
		Size:      e.Size,
		Mtime:     e.ModTime.Unix(),
		MtimeNs:   0,
		UID:       e.UID,
		GID:       e.GID,
		LinkCount: 1,
	}

	// Set symlink target if this is a symlink
	if e.Mode&fs.ModeSymlink != 0 {
		entry.Symlink = e.LinkTarget
	}

	// Set hash if this is a regular file
	if e.Mode.IsRegular() && hash != "" {
		// For now, don't store hash in the entry collection
		// The hash will be computed during merge
		entry.HashAlgo = cvmfscatalog.HashSha256
		entry.CompAlgo = cvmfscatalog.CompZlib
	}

	// Copy user xattrs from the tar PAX headers.  Synthetic xattrs
	// (user.cvmfs.hash, user.cvmfs.compression, user.cvmfs.chunk_list) are
	// injected later in pipeline.RunFromReader once the content hash is known.
	if len(e.Xattrs) > 0 {
		entry.Xattr = make(map[string][]byte, len(e.Xattrs))
		for k, v := range e.Xattrs {
			entry.Xattr[k] = v
		}
	}

	b.entries = append(b.entries, entry)

	return nil
}

// Entries returns the collected entries for use by Merge.
func (b *Builder) Entries() []*cvmfscatalog.Entry {
	return b.entries
}

// Finalize is deprecated and returns an error.
// Use Merge in pkg/cvmfscatalog instead.
func (b *Builder) Finalize(ctx context.Context) (compPath string, dbHash string, err error) {
	return "", "", fmt.Errorf("catalog.Builder.Finalize is deprecated; use pkg/cvmfscatalog.Merge instead")
}
