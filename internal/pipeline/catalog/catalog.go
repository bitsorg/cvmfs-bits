// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

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
// The actual SQLite catalog is built by cvmfscatalog.BuildSubtree.
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

// Add converts a tar FileEntry to a catalog Entry and appends it to the
// builder's collection.
//
// Hash, Chunks, and synthetic xattrs (user.cvmfs.hash, user.cvmfs.compression,
// user.cvmfs.chunk_list) are NOT set here — they are injected by pipeline.go
// after the compress+upload stages complete, once the final CAS key is known.
// Only structural metadata (path, mode, size, timestamps, UID/GID, symlink
// target, and user-supplied xattrs from the tar PAX headers) is captured here.
func (b *Builder) Add(ctx context.Context, e unpack.FileEntry) error {
	ctx, span := b.obs.Tracer.Start(ctx, "catalog.add")
	defer span.End()

	// Convert unpack.FileEntry to cvmfscatalog.Entry.
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

	// Set symlink target.
	if e.Mode&fs.ModeSymlink != 0 {
		entry.Symlink = e.LinkTarget
	}

	// Mark the expected hash algorithm and compression for regular files so that
	// the pipeline.go post-processing loop can fill in entry.Hash without
	// needing to re-derive this decision.
	if e.Mode.IsRegular() {
		entry.HashAlgo = cvmfscatalog.HashSha1
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

// Entries returns the collected entries for use by BuildSubtree.
func (b *Builder) Entries() []*cvmfscatalog.Entry {
	return b.entries
}

// Finalize is deprecated and returns an error.
// Use BuildSubtree in pkg/cvmfscatalog instead.
func (b *Builder) Finalize(ctx context.Context) (compPath string, dbHash string, err error) {
	return "", "", fmt.Errorf("catalog.Builder.Finalize is deprecated; use cvmfscatalog.BuildSubtree instead")
}
