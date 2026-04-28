package unpack

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"time"
)

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
	Xattrs     map[string][]byte
}

// Options controls extraction behaviour.
type Options struct {
	// MaxEntrySize caps the byte size of any single file entry.
	// Set to 0 to use MaxFileSize.
	MaxEntrySize int64
}

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
	seenFiles := make(map[string][]byte)

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
			// LimitReader is a defence-in-depth guard for streaming tars where
			// header.Size may be 0 (e.g. some GNU sparse tar conventions).
			data, err := io.ReadAll(io.LimitReader(tr, maxSize+1))
			if err != nil {
				return fmt.Errorf("reading file %s: %w", header.Name, err)
			}
			if int64(len(data)) > maxSize {
				return fmt.Errorf("tar entry %q body exceeds size limit (%d bytes)",
					header.Name, maxSize)
			}
			entry.Data = data
			entry.Size = int64(len(data)) // use actual length, not header claim
			seenFiles[cleanPath] = data

		case tar.TypeLink:
			// Hard link: resolve to the data of the linked file, which must
			// have appeared earlier in the stream (GNU tar ordering guarantee).
			// Bug fix: previously silently skipped, causing data loss in the
			// catalog for hard-linked paths.
			if header.Linkname == "" {
				return fmt.Errorf("hard link %q has empty target", header.Name)
			}
			target := filepath.Clean(header.Linkname)
			data, ok := seenFiles[target]
			if !ok {
				return fmt.Errorf("hard link %q refers to unknown target %q (forward references not supported)",
					header.Name, header.Linkname)
			}
			entry.Data = data
			entry.Size = int64(len(data))
			entry.Mode = fs.FileMode(header.Mode)
			// Register the hard-linked path so subsequent links can resolve it too.
			seenFiles[cleanPath] = data

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
