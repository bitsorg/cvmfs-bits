// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"archive/tar"
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

// BatchMember is one package of a batched ingest: its spooled tar, and where
// it goes relative to the batch's base directory (e.g. "Packages/X/1-1").
type BatchMember struct {
	TarPath string
	RelPath string
}

// catalogMarker in a directory makes `cvmfs_swissknife ingest` give it its own
// nested catalog, as `ingest -c` does for the base of a single package.
const catalogMarker = ".cvmfscatalog"

// WriteBatchTar writes the members' tars to w as one tar, each under its RelPath
// with a catalog marker at its root unless it has one. All are checked first; after
// a later error w holds a partial archive, so kill its reader rather than close it.
func WriteBatchTar(w io.Writer, members []BatchMember) error {
	if err := CheckBatch(members); err != nil {
		return err
	}
	tw := tar.NewWriter(w)
	for _, m := range members {
		if err := walkBatchMember(m, tw); err != nil {
			return fmt.Errorf("batch member %q: %w", m.RelPath, err)
		}
	}
	return tw.Close()
}

// CheckBatch is WriteBatchTar's check of the members and their tars, which
// reads only the headers.
func CheckBatch(members []BatchMember) error {
	if err := checkBatchMembers(members); err != nil {
		return err
	}
	for _, m := range members {
		if err := walkBatchMember(m, nil); err != nil {
			return fmt.Errorf("batch member %q: %w", m.RelPath, err)
		}
	}
	return nil
}

// checkBatchMembers refuses paths that are not clean and relative, and members
// that are the same as, or inside, another member.
func checkBatchMembers(members []BatchMember) error {
	if len(members) == 0 {
		return errors.New("batch has no members")
	}
	for i, m := range members {
		p := m.RelPath
		if p == "" || p == "." || path.IsAbs(p) || path.Clean(p) != p || p == ".." || strings.HasPrefix(p, "../") {
			return fmt.Errorf("batch member path %q is not a clean relative path", p)
		}
		for _, o := range members[:i] {
			if p == o.RelPath || strings.HasPrefix(p, o.RelPath+"/") || strings.HasPrefix(o.RelPath, p+"/") {
				return fmt.Errorf("batch members %q and %q overlap", o.RelPath, p)
			}
		}
	}
	return nil
}

// walkBatchMember renames a member's entries and, with tw, writes them and the
// marker; without tw it only checks them (the file is then read by seeking).
func walkBatchMember(m BatchMember, tw *tar.Writer) error {
	f, err := os.Open(m.TarPath)
	if err != nil {
		return err
	}
	defer f.Close()
	var src io.Reader = f
	if tw != nil {
		src = bufio.NewReaderSize(f, 1<<20)
	}
	tr := tar.NewReader(src)
	marked, rootTime := false, time.Now()
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading %s: %w", m.TarPath, err)
		}
		// A global header would apply to every later member too.
		if hdr.Typeflag == tar.TypeXGlobalHeader {
			continue
		}
		if hdr.Typeflag == tar.TypeGNUSparse {
			return fmt.Errorf("tar entry %q: sparse files are not batched", hdr.Name)
		}
		if hdr.Name, err = batchEntryPath(m.RelPath, hdr.Name); err != nil {
			return err
		}
		if hdr.Name == m.RelPath {
			if hdr.Typeflag != tar.TypeDir {
				return fmt.Errorf("tar entry %q: the package root is not a directory", hdr.Name)
			}
			rootTime = hdr.ModTime
		}
		if hdr.Typeflag == tar.TypeLink { // hard links name an entry of the archive
			if hdr.Linkname, err = batchEntryPath(m.RelPath, hdr.Linkname); err != nil {
				return err
			}
			if hdr.Linkname == m.RelPath {
				return fmt.Errorf("tar entry %q: hard link to the package root", hdr.Name)
			}
		}
		marked = marked || hdr.Name == m.RelPath+"/"+catalogMarker
		if tw == nil {
			continue
		}
		hdr.Format = tar.FormatUnknown // the writer picks what the longer names need
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := io.Copy(tw, tr); err != nil {
			return err
		}
	}
	if marked || tw == nil {
		return nil
	}
	return tw.WriteHeader(&tar.Header{Name: m.RelPath + "/" + catalogMarker,
		Typeflag: tar.TypeReg, Mode: 0o644, ModTime: rootTime})
}

// batchEntryPath maps an entry name of a member's tar to its name in the batch,
// refusing absolute names and names that leave the member's directory.
func batchEntryPath(rel, name string) (string, error) {
	if path.IsAbs(name) {
		return "", fmt.Errorf("tar entry %q is absolute", name)
	}
	c := path.Clean(name)
	if c == ".." || strings.HasPrefix(c, "../") {
		return "", fmt.Errorf("tar entry %q leaves the package directory", name)
	}
	if c == "." {
		return rel, nil
	}
	return rel + "/" + c, nil
}
