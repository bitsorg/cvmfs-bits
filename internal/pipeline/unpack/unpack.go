// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package unpack

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// content is a resolved tar entry body: inline bytes, or a spill file on disk.
type content struct {
	data []byte
	path string
	size int64
}

// spillEntry streams one tar entry body into SpillDir and returns its path and
// byte count. Files are named by sequence, not by tar path, so that a hostile
// or merely awkward path (traversal, absurd length, unicode) can never reach
// the filesystem.
func spillEntry(dir string, seq int, r io.Reader) (string, int64, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", 0, fmt.Errorf("creating spill dir: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("e%08d.bin", seq))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", 0, fmt.Errorf("creating spill file: %w", err)
	}
	n, cerr := io.Copy(f, r)
	if closeErr := f.Close(); cerr == nil {
		cerr = closeErr
	}
	if cerr != nil {
		_ = os.Remove(path)
		return "", 0, fmt.Errorf("spilling entry: %w", cerr)
	}
	return path, n, nil
}

// paxXattrPrefix is the standard prefix that GNU tar uses when encoding
// extended attributes in PAX records: SCHILY.xattr.<xattr-name>.
const paxXattrPrefix = "SCHILY.xattr."

// MaxFileSize is the default per-entry size limit (1 GiB).
// Callers may pass a custom limit via ExtractWithOptions.
const MaxFileSize int64 = 1 << 30 // 1 GiB

type FileEntry struct {
	Path       string
	Mode       fs.FileMode
	ModTime    time.Time
	Size       int64
	Data       []byte
	LinkTarget string // for symlinks
	UID        uint32
	GID        uint32
	// Xattrs contains extended attributes extracted from the tar PAX headers.
	// Keys are the bare xattr names (e.g. "user.myapp.tag"), values are the
	// raw attribute bytes.  Nil means no xattrs were present.
	Xattrs map[string][]byte

	// ContentPath, when non-empty, is a spill file on disk holding this entry's
	// bytes and Data is nil. Large entries are spilled so that neither the
	// hard-link table nor the sorted entry list keeps the whole package
	// resident: peak memory was previously the ENTIRE uncompressed package,
	// which OOM-killed the service on an 8 GB host.
	//
	// Use Open()/Bytes() rather than touching Data or ContentPath directly.
	ContentPath string
}

// Open returns a reader over the entry's content, from memory for small
// entries and from the spill file for large ones. The caller must Close it.
func (e FileEntry) Open() (io.ReadCloser, error) {
	if e.ContentPath == "" {
		return io.NopCloser(bytes.NewReader(e.Data)), nil
	}
	f, err := os.Open(e.ContentPath)
	if err != nil {
		return nil, fmt.Errorf("opening spilled content for %s: %w", e.Path, err)
	}
	return f, nil
}

// Bytes returns the entry's full content. It allocates for spilled entries, so
// prefer Open() on any path that can stream.
func (e FileEntry) Bytes() ([]byte, error) {
	if e.ContentPath == "" {
		return e.Data, nil
	}
	rc, err := e.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close() //nolint:errcheck // read-only
	return io.ReadAll(rc)
}

// Options controls extraction behaviour.
type Options struct {
	// MaxEntrySize caps the byte size of any single file entry.
	// Set to 0 to use MaxFileSize.
	MaxEntrySize int64

	// SpillDir, when non-empty, is a directory into which entries larger than
	// InlineMaxSize are written instead of being held in memory. The caller
	// owns the directory and must remove it when the job ends.
	// Empty keeps the previous all-in-memory behaviour.
	SpillDir string

	// InlineMaxSize is the threshold below which content stays in memory.
	// Small files dominate by COUNT in a software tree, so spilling every one
	// of them would trade memory for a syscall storm and an inode per file.
	// Zero uses DefaultInlineMaxSize.
	InlineMaxSize int64
}

// DefaultInlineMaxSize keeps small files in memory. 64 KiB covers the vast
// majority of files in a typical package while capping the in-memory total at
// roughly (file count x 64 KiB) worst case.
const DefaultInlineMaxSize = 64 * 1024

// Extract reads a tar from r and sends FileEntry to out using default options.
// It does NOT close out — the caller owns the channel and is responsible
// for closing it after Extract returns.
func Extract(ctx context.Context, r io.Reader, out chan<- FileEntry) error {
	return ExtractWithOptions(ctx, r, out, Options{})
}

// ExtractWithOptions is like Extract but accepts explicit options.
// It does NOT close out — the caller owns the channel.
//
// Hard links (TypeLink) are resolved to the data of the linked file.  The
// linked file must appear earlier in the tar stream (GNU tar guarantees this).
func ExtractWithOptions(ctx context.Context, r io.Reader, out chan<- FileEntry, opts Options) error {
	maxSize := opts.MaxEntrySize
	if maxSize <= 0 {
		maxSize = MaxFileSize
	}

	tr := tar.NewReader(r)

	// seenFiles tracks already-emitted regular files so hard links can be
	// resolved without a second disk read.  Keys are cleaned entry paths.
	// content is what a hard link resolves to: either inline bytes or a spill
	// file. Storing the PATH (not the bytes) is what stops this table from
	// holding the whole package.
	seenFiles := make(map[string]content)
	inlineMax := opts.InlineMaxSize
	if inlineMax <= 0 {
		inlineMax = DefaultInlineMaxSize
	}
	spillSeq := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		header, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading tar header: %w", err)
		}

		// Critical #2: Validate path to prevent Zip Slip / path traversal.
		if err := validatePath(header.Name); err != nil {
			return fmt.Errorf("invalid tar entry path %q: %w", header.Name, err)
		}

		cleanPath := filepath.Clean(header.Name)

		// Clamp negative UID/GID values (malformed tar) to 0.
		uid := header.Uid
		if uid < 0 {
			uid = 0
		}
		gid := header.Gid
		if gid < 0 {
			gid = 0
		}

		entry := FileEntry{
			Path:    cleanPath,
			Mode:    fs.FileMode(header.Mode),
			ModTime: header.ModTime,
			Size:    header.Size,
			UID:     uint32(uid),
			GID:     uint32(gid),
		}

		// Extract extended attributes from PAX records.
		// GNU tar encodes xattrs as "SCHILY.xattr.<name>" PAX entries.
		for k, v := range header.PAXRecords {
			name, ok := strings.CutPrefix(k, paxXattrPrefix)
			if !ok || name == "" {
				continue
			}
			if entry.Xattrs == nil {
				entry.Xattrs = make(map[string][]byte)
			}
			entry.Xattrs[name] = []byte(v)
		}

		switch header.Typeflag {
		case tar.TypeReg:
			// Bug fix: reject negative sizes from malformed tar headers before
			// any allocation or LimitReader arithmetic.
			if header.Size < 0 {
				return fmt.Errorf("tar entry %q has negative size: %d", header.Name, header.Size)
			}
			// Critical #3: Enforce per-entry size limit before allocating.
			if header.Size > maxSize {
				return fmt.Errorf("tar entry %q exceeds size limit: %d > %d bytes",
					header.Name, header.Size, maxSize)
			}
			// Spill large entries to disk rather than buffering them. The
			// LimitReader is a defence-in-depth guard for streaming tars where
			// header.Size may be 0 (e.g. some GNU sparse tar conventions).
			src := io.LimitReader(tr, maxSize+1)
			if opts.SpillDir != "" && header.Size > inlineMax {
				path, n, serr := spillEntry(opts.SpillDir, spillSeq, src)
				spillSeq++
				if serr != nil {
					return serr
				}
				if n > maxSize {
					return fmt.Errorf("tar entry %q body exceeds size limit (%d bytes)",
						header.Name, n)
				}
				entry.ContentPath = path
				entry.Size = n
				seenFiles[cleanPath] = content{path: path, size: n}
			} else {
				data, err := io.ReadAll(src)
				if err != nil {
					return fmt.Errorf("reading file %s: %w", header.Name, err)
				}
				if int64(len(data)) > maxSize {
					return fmt.Errorf("tar entry %q body exceeds size limit (%d bytes)",
						header.Name, maxSize)
				}
				entry.Data = data
				entry.Size = int64(len(data)) // use actual length, not header claim
				seenFiles[cleanPath] = content{data: data, size: int64(len(data))}
			}

		case tar.TypeLink:
			// Hard link: resolve to the data of the linked file, which must
			// have appeared earlier in the stream (GNU tar ordering guarantee).
			// Bug fix: previously silently skipped, causing data loss in the
			// catalog for hard-linked paths.
			if header.Linkname == "" {
				return fmt.Errorf("hard link %q has empty target", header.Name)
			}
			target := filepath.Clean(header.Linkname)
			c, ok := seenFiles[target]
			if !ok {
				return fmt.Errorf("hard link %q refers to unknown target %q (forward references not supported)",
					header.Name, header.Linkname)
			}
			// Reference the target's content; for a spilled target this shares
			// the same file rather than duplicating its bytes.
			entry.Data = c.data
			entry.ContentPath = c.path
			entry.Size = c.size
			entry.Mode = fs.FileMode(header.Mode)
			// Register the hard-linked path so subsequent links can resolve it too.
			seenFiles[cleanPath] = c

		case tar.TypeDir:
			entry.Mode = fs.FileMode(header.Mode) | fs.ModeDir

		case tar.TypeSymlink:
			// Bug fix #1: reject empty symlink targets (previously accepted).
			// Bug fix #2: validate the target in context of the symlink's own
			// directory so that valid relative references like ../sibling are
			// accepted while true escapes like ../../etc/passwd are rejected.
			if err := validateSymlinkTarget(cleanPath, header.Linkname); err != nil {
				return fmt.Errorf("invalid symlink target %q in entry %q: %w",
					header.Linkname, header.Name, err)
			}
			entry.LinkTarget = header.Linkname
			entry.Mode = fs.FileMode(header.Mode) | fs.ModeSymlink

		default:
			// Skip device nodes, FIFOs, etc.
			continue
		}

		select {
		case out <- entry:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// validatePath rejects file-entry paths that could escape the destination
// directory:
//   - absolute paths (start with /)
//   - paths whose cleaned form is or begins with ".."
func validatePath(p string) error {
	if p == "" {
		return nil
	}
	if filepath.IsAbs(p) {
		return fmt.Errorf("absolute path not allowed")
	}
	cleaned := filepath.Clean(p)
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path traversal not allowed")
	}
	// Check raw components to catch multi-segment tricks before Clean collapses them.
	for _, part := range strings.Split(filepath.ToSlash(p), "/") {
		if part == ".." {
			return fmt.Errorf("path traversal component '..' not allowed")
		}
	}
	return nil
}

// validateSymlinkTarget checks that a symlink's target, when resolved relative
// to the symlink's own directory within the archive, does not escape the
// archive root.
//
// Unlike validatePath (which is used for entry names and rejects all ".."
// components), this function allows relative traversal that stays within the
// root — e.g. "a/b/link → ../c/file" resolves to "a/c/file" and is valid.
// Absolute targets and targets that resolve above the root are rejected.
func validateSymlinkTarget(entryCleanPath, target string) error {
	if target == "" {
		return fmt.Errorf("empty symlink target not allowed")
	}
	if filepath.IsAbs(target) {
		return fmt.Errorf("absolute symlink target not allowed")
	}
	// Resolve the target relative to the symlink's parent directory.
	dir := filepath.Dir(entryCleanPath) // "." when the symlink is at the root level
	resolved := filepath.Clean(filepath.Join(dir, target))
	if resolved == ".." || strings.HasPrefix(resolved, ".."+string(filepath.Separator)) {
		return fmt.Errorf("symlink target escapes archive root")
	}
	return nil
}
