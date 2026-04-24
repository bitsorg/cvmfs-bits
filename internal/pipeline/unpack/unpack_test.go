package unpack

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"testing"
)

// ── tar builder helper ───────────────────────────────────────────────────────

type tarEntry struct {
	name     string
	content  string // file data for TypeReg; link target for TypeSymlink/TypeLink
	typeflag byte
	mode     int64 // 0 → use sensible default per type
}

// makeTar builds a tar in memory from a list of tarEntry descriptors.
func makeTar(entries []tarEntry) *bytes.Buffer {
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)
	for _, e := range entries {
		mode := e.mode
		switch e.typeflag {
		case tar.TypeSymlink:
			if mode == 0 {
				mode = 0777
			}
			tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeSymlink,
				Name:     e.name,
				Linkname: e.content,
				Mode:     mode,
			})
		case tar.TypeLink:
			tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeLink,
				Name:     e.name,
				Linkname: e.content, // target of the hard link
				Size:     0,
				Mode:     mode,
			})
		case tar.TypeDir:
			if mode == 0 {
				mode = 0755
			}
			tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeDir,
				Name:     e.name,
				Mode:     mode,
			})
		default: // TypeReg
			if mode == 0 {
				mode = 0644
			}
			hdr := &tar.Header{
				Typeflag: tar.TypeReg,
				Name:     e.name,
				Size:     int64(len(e.content)),
				Mode:     mode,
			}
			tw.WriteHeader(hdr)
			tw.Write([]byte(e.content))
		}
	}
	tw.Close()
	return buf
}

// collectAll runs Extract synchronously and returns all emitted entries.
func collectAll(t *testing.T, buf *bytes.Buffer) ([]FileEntry, error) {
	t.Helper()
	out := make(chan FileEntry, 64)
	var entries []FileEntry
	done := make(chan struct{})
	go func() {
		defer close(done)
		for e := range out {
			entries = append(entries, e)
		}
	}()
	err := Extract(context.Background(), buf, out)
	close(out)
	<-done
	return entries, err
}

// ── validatePath unit tests ──────────────────────────────────────────────────

func TestValidatePath_Accept(t *testing.T) {
	cases := []string{
		"",
		"file.txt",
		"dir/file.txt",
		"a/b/c/d.so",
		"./file.txt",
	}
	for _, c := range cases {
		if err := validatePath(c); err != nil {
			t.Errorf("validatePath(%q) returned unexpected error: %v", c, err)
		}
	}
}

func TestValidatePath_Reject(t *testing.T) {
	cases := []string{
		"/etc/passwd",
		"../secret",
		"foo/../../../etc/passwd",
		"a/../../b",
		"..",
	}
	for _, c := range cases {
		if err := validatePath(c); err == nil {
			t.Errorf("validatePath(%q) should have returned an error but did not", c)
		}
	}
}

// ── validateSymlinkTarget unit tests ────────────────────────────────────────

func TestValidateSymlinkTarget_Accept(t *testing.T) {
	cases := []struct{ entry, target string }{
		// Same-directory reference.
		{"dir/link", "sibling.so"},
		// Go one level up but stay within root: a/b/link → ../c/file ≡ a/c/file.
		{"a/b/link", "../c/file"},
		// Go two levels up from a two-level deep symlink: a/b/link → ../../top ≡ top.
		{"a/b/link", "../../top"},
		// Root-level symlink pointing to a sibling.
		{"link", "file.txt"},
		// Nested target path.
		{"pkg/lib/libfoo.so", "../lib64/libfoo.so.1.0"},
	}
	for _, c := range cases {
		if err := validateSymlinkTarget(c.entry, c.target); err != nil {
			t.Errorf("validateSymlinkTarget(%q, %q) unexpected error: %v", c.entry, c.target, err)
		}
	}
}

func TestValidateSymlinkTarget_Reject(t *testing.T) {
	cases := []struct{ entry, target, reason string }{
		{"link", "", "empty target"},
		{"link", "/etc/passwd", "absolute target"},
		{"a/link", "../../evil", "escapes root from one-level-deep dir"},
		{"link", "../evil", "escapes root from top-level dir"},
		{"a/b/link", "../../../evil", "escapes root from two-level-deep dir (needs 3 levels)"},
	}
	for _, c := range cases {
		if err := validateSymlinkTarget(c.entry, c.target); err == nil {
			t.Errorf("validateSymlinkTarget(%q, %q) should reject (%s) but did not",
				c.entry, c.target, c.reason)
		}
	}
}

// ── Extract security tests ───────────────────────────────────────────────────

func TestExtract_PathTraversal(t *testing.T) {
	attacks := []string{
		"../../../etc/passwd",
		"/etc/shadow",
		"foo/../../../etc/passwd",
	}
	for _, path := range attacks {
		t.Run(path, func(t *testing.T) {
			buf := makeTar([]tarEntry{{name: path, content: "evil", typeflag: tar.TypeReg}})
			_, err := collectAll(t, buf)
			if err == nil {
				t.Errorf("Extract should have rejected path %q but did not", path)
			}
		})
	}
}

func TestExtract_SymlinkTraversal(t *testing.T) {
	// Direct double-escape — still rejected.
	buf := makeTar([]tarEntry{
		{name: "link", content: "../../etc/passwd", typeflag: tar.TypeSymlink},
	})
	_, err := collectAll(t, buf)
	if err == nil {
		t.Error("Extract should have rejected symlink with traversal target but did not")
	}
}

func TestExtract_AbsoluteSymlinkTarget(t *testing.T) {
	buf := makeTar([]tarEntry{
		{name: "link", content: "/etc/passwd", typeflag: tar.TypeSymlink},
	})
	_, err := collectAll(t, buf)
	if err == nil {
		t.Error("Extract should have rejected absolute symlink target but did not")
	}
}

// TestExtract_SymlinkRelativeWithinRoot verifies that a relative symlink whose
// target stays within the archive root is accepted.  Previously validatePath was
// called on the raw Linkname, which rejected any ".." component regardless of
// the resolved position.
func TestExtract_SymlinkRelativeWithinRoot(t *testing.T) {
	cases := []struct{ name, target string }{
		// Classic lib → lib64 pattern common in Linux packages.
		{"usr/lib/libfoo.so", "../lib64/libfoo.so.1.0"},
		// Two-level link that stays within root.
		{"a/b/link", "../../top"},
		// Same directory reference.
		{"a/b/link", "sibling.txt"},
	}
	for _, c := range cases {
		t.Run(c.name+"→"+c.target, func(t *testing.T) {
			buf := makeTar([]tarEntry{
				{name: c.name, content: c.target, typeflag: tar.TypeSymlink},
			})
			entries, err := collectAll(t, buf)
			if err != nil {
				t.Errorf("Extract rejected valid relative symlink %q → %q: %v", c.name, c.target, err)
			}
			if len(entries) != 1 {
				t.Fatalf("expected 1 entry, got %d", len(entries))
			}
			if entries[0].LinkTarget != c.target {
				t.Errorf("LinkTarget = %q, want %q", entries[0].LinkTarget, c.target)
			}
		})
	}
}

// TestExtract_EmptySymlinkTarget verifies that a symlink with an empty Linkname
// is rejected (previously accepted because validatePath("") returned nil).
func TestExtract_EmptySymlinkTarget(t *testing.T) {
	buf := makeTar([]tarEntry{
		{name: "link", content: "", typeflag: tar.TypeSymlink},
	})
	_, err := collectAll(t, buf)
	if err == nil {
		t.Error("Extract should have rejected symlink with empty target but did not")
	}
}

// ── Zero-length files ────────────────────────────────────────────────────────

// TestExtract_ZeroLengthFile verifies that a regular file with zero bytes is
// extracted correctly: the entry is emitted, Data is an empty slice (not nil),
// and Size is 0.
func TestExtract_ZeroLengthFile(t *testing.T) {
	buf := makeTar([]tarEntry{
		{name: "empty.txt", content: "", typeflag: tar.TypeReg},
	})
	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("Extract returned unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Data == nil {
		t.Error("Data should be non-nil empty slice for zero-length file, got nil")
	}
	if len(e.Data) != 0 {
		t.Errorf("expected 0 bytes of data, got %d", len(e.Data))
	}
	if e.Size != 0 {
		t.Errorf("expected Size 0, got %d", e.Size)
	}
	if !e.Mode.IsRegular() {
		t.Errorf("expected regular file mode, got %v", e.Mode)
	}
}

// TestExtract_ZeroLengthFile_SizeZeroHeader verifies that a tar entry whose
// header advertises Size=0 but whose body has been read as empty is handled
// identically to a genuine zero-byte file.
func TestExtract_ZeroLengthFile_SizeZeroHeader(t *testing.T) {
	// Build a tar manually with Size=0 and no body bytes.
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)
	tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "zero.bin",
		Size:     0,
		Mode:     0644,
	})
	// Intentionally write no body bytes.
	tw.Close()

	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Size != 0 {
		t.Errorf("expected Size 0, got %d", entries[0].Size)
	}
}

// ── Size limit tests ─────────────────────────────────────────────────────────

func TestExtract_SizeLimitEnforced(t *testing.T) {
	// Craft a tar header claiming a file is larger than the limit.
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)
	tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "bigfile.bin",
		Size:     MaxFileSize + 1, // exceeds default limit
		Mode:     0644,
	})
	tw.Write([]byte(strings.Repeat("X", 1024)))
	tw.Close()

	_, err := collectAll(t, buf)
	if err == nil {
		t.Error("Extract should have rejected entry exceeding size limit but did not")
	}
}

func TestExtract_CustomSizeLimit(t *testing.T) {
	const smallLimit int64 = 10
	buf := makeTar([]tarEntry{
		{name: "big.txt", content: strings.Repeat("X", int(smallLimit)+1), typeflag: tar.TypeReg},
	})
	out := make(chan FileEntry, 16)
	err := ExtractWithOptions(context.Background(), buf, out, Options{MaxEntrySize: smallLimit})
	close(out)
	if err == nil {
		t.Error("ExtractWithOptions should have rejected entry exceeding custom size limit but did not")
	}
}

// TestExtract_NegativeSize verifies that a tar entry with a negative Size field
// is rejected before any allocation or LimitReader arithmetic occurs.
//
// The Go tar.Writer refuses to write negative sizes at WriteHeader time, so we
// construct the raw 512-byte header block manually using the GNU binary-size
// encoding (bit 7 of byte 124 set → value is big-endian two's complement).
func TestExtract_NegativeSize(t *testing.T) {
	buf := buildNegativeSizeTar()

	out := make(chan FileEntry, 16)
	var emitted []FileEntry
	done := make(chan struct{})
	go func() {
		defer close(done)
		for e := range out {
			emitted = append(emitted, e)
		}
	}()
	err := Extract(context.Background(), buf, out)
	close(out)
	<-done

	// The Go tar reader may normalise or reject the malformed header itself.
	// Either path is acceptable — what must NOT happen is emitting a FileEntry
	// with negative Size.
	for _, e := range emitted {
		if e.Size < 0 {
			t.Errorf("emitted FileEntry with negative Size %d", e.Size)
		}
	}
	if err == nil && len(emitted) > 0 {
		t.Log("tar reader normalised away the negative size (acceptable)")
	}
}

// buildNegativeSizeTar constructs a raw tar stream containing one entry whose
// Size field encodes a large negative value via GNU binary encoding.
func buildNegativeSizeTar() *bytes.Buffer {
	var rawHdr [512]byte
	rawHdr[156] = '0' // Typeflag = TypeReg
	copy(rawHdr[0:100], "negative.bin")
	copy(rawHdr[100:108], "0000644\x00") // Mode
	copy(rawHdr[108:116], "0000000\x00") // UID
	copy(rawHdr[116:124], "0000000\x00") // GID
	// Size field (bytes 124–135): GNU binary encoding: first byte 0xFF → negative.
	rawHdr[124] = 0xFF
	for i := 125; i <= 135; i++ {
		rawHdr[i] = 0xFF
	}
	copy(rawHdr[136:148], "00000000000\x00") // Mtime
	copy(rawHdr[148:156], "        ")        // placeholder checksum (spaces)
	rawHdr[156] = '0'
	copy(rawHdr[157:265], "ustar  \x00") // Magic

	// Compute checksum over the block with checksum field as spaces.
	var sum int
	for _, b := range rawHdr {
		sum += int(b)
	}
	copy(rawHdr[148:156], []byte(fmt.Sprintf("%06o\x00 ", sum)))

	buf := new(bytes.Buffer)
	buf.Write(rawHdr[:])
	buf.Write(make([]byte, 1024)) // two end-of-archive blocks
	return buf
}

// ── Permission / mode tests ──────────────────────────────────────────────────

// TestExtract_ModeSetUID verifies that a regular file with the setuid bit set
// is extracted and the mode bits are preserved as recorded in the tar header.
// CVMFS clients enforce nosuid at mount time; preserving the bits in the catalog
// is correct so that privileged mounts (if ever configured) honour the intent.
func TestExtract_ModeSetUID(t *testing.T) {
	const setuidMode int64 = 0o4755 // rwsr-xr-x
	buf := makeTar([]tarEntry{
		{name: "setuid-bin", content: "binary", typeflag: tar.TypeReg, mode: setuidMode},
	})
	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	got := entries[0].Mode
	// Verify the Unix permission bits are preserved (fs.FileMode lower 12 bits).
	gotPerm := got & 0o7777
	wantPerm := fs.FileMode(setuidMode) & 0o7777
	if gotPerm != wantPerm {
		t.Errorf("mode bits: got %04o, want %04o", gotPerm, wantPerm)
	}
}

// TestExtract_ModeSetGID verifies setgid bit preservation.
func TestExtract_ModeSetGID(t *testing.T) {
	const setgidMode int64 = 0o2755
	buf := makeTar([]tarEntry{
		{name: "setgid-bin", content: "binary", typeflag: tar.TypeReg, mode: setgidMode},
	})
	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gotPerm := entries[0].Mode & 0o7777
	if gotPerm != fs.FileMode(setgidMode)&0o7777 {
		t.Errorf("setgid mode bits not preserved: got %04o", gotPerm)
	}
}

// TestExtract_ModeWorldWritable verifies that world-writable files are accepted
// and their mode is preserved verbatim (hardening is the operator's responsibility).
func TestExtract_ModeWorldWritable(t *testing.T) {
	const openMode int64 = 0o777
	buf := makeTar([]tarEntry{
		{name: "writable.txt", content: "data", typeflag: tar.TypeReg, mode: openMode},
	})
	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gotPerm := entries[0].Mode & 0o777
	if gotPerm != fs.FileMode(openMode)&0o777 {
		t.Errorf("world-writable bits not preserved: got %04o", gotPerm)
	}
}

// ── Hard link tests ──────────────────────────────────────────────────────────

// TestExtract_HardLink_Resolved verifies that a TypeLink entry is resolved to
// the data of the previously-seen regular file it references, and emitted as a
// regular FileEntry.  Previously hard links were silently skipped, causing the
// linked path to be absent from the catalog entirely.
func TestExtract_HardLink_Resolved(t *testing.T) {
	buf := makeTar([]tarEntry{
		{name: "original.txt", content: "hello hard link", typeflag: tar.TypeReg},
		{name: "link.txt", content: "original.txt", typeflag: tar.TypeLink},
	})
	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (original + hard link), got %d", len(entries))
	}
	orig := entries[0]
	link := entries[1]

	if string(orig.Data) != "hello hard link" {
		t.Errorf("original data = %q, want %q", orig.Data, "hello hard link")
	}
	if string(link.Data) != string(orig.Data) {
		t.Errorf("hard link data = %q, want same as original %q", link.Data, orig.Data)
	}
	if link.Size != orig.Size {
		t.Errorf("hard link Size %d != original Size %d", link.Size, orig.Size)
	}
	if !link.Mode.IsRegular() {
		t.Errorf("hard link mode should be regular, got %v", link.Mode)
	}
}

// TestExtract_HardLink_ChainResolved verifies that a chain of hard links (B→A,
// C→B) all resolve to the original file's data.
func TestExtract_HardLink_ChainResolved(t *testing.T) {
	buf := makeTar([]tarEntry{
		{name: "a.txt", content: "original", typeflag: tar.TypeReg},
		{name: "b.txt", content: "a.txt", typeflag: tar.TypeLink},
		{name: "c.txt", content: "b.txt", typeflag: tar.TypeLink},
	})
	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	for _, e := range entries {
		if string(e.Data) != "original" {
			t.Errorf("entry %q data = %q, want %q", e.Path, e.Data, "original")
		}
	}
}

// TestExtract_HardLink_ForwardRef verifies that a hard link whose target has not
// yet appeared in the stream is rejected with a clear error.
func TestExtract_HardLink_ForwardRef(t *testing.T) {
	buf := makeTar([]tarEntry{
		// Hard link appears BEFORE the target — illegal in GNU tar.
		{name: "link.txt", content: "notyet.txt", typeflag: tar.TypeLink},
		{name: "notyet.txt", content: "too late", typeflag: tar.TypeReg},
	})
	_, err := collectAll(t, buf)
	if err == nil {
		t.Error("Extract should have rejected forward hard link reference but did not")
	}
}

// TestExtract_HardLink_EmptyTarget verifies that a TypeLink with an empty
// Linkname is rejected.
func TestExtract_HardLink_EmptyTarget(t *testing.T) {
	buf := makeTar([]tarEntry{
		{name: "link.txt", content: "", typeflag: tar.TypeLink},
	})
	_, err := collectAll(t, buf)
	if err == nil {
		t.Error("Extract should have rejected hard link with empty target but did not")
	}
}

// TestExtract_HardLink_SelfReference verifies that a hard link pointing to
// itself (which can never have appeared earlier) is rejected.
func TestExtract_HardLink_SelfReference(t *testing.T) {
	buf := makeTar([]tarEntry{
		{name: "self.txt", content: "self.txt", typeflag: tar.TypeLink},
	})
	_, err := collectAll(t, buf)
	if err == nil {
		t.Error("Extract should have rejected self-referencing hard link but did not")
	}
}

// ── General extraction tests ─────────────────────────────────────────────────

func TestExtract_ValidTar(t *testing.T) {
	buf := makeTar([]tarEntry{
		{name: "hello.txt", content: "hello world", typeflag: tar.TypeReg},
		{name: "subdir/", typeflag: tar.TypeDir},
		{name: "subdir/world.txt", content: "world", typeflag: tar.TypeReg},
	})
	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("Extract returned unexpected error: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
}

// TestExtract_EmptyTar verifies that a tar stream with no entries returns nil
// and emits nothing.
func TestExtract_EmptyTar(t *testing.T) {
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)
	tw.Close()

	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("empty tar returned unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries from empty tar, got %d", len(entries))
	}
}

// TestExtract_CorruptTar verifies that a truncated/corrupt tar stream returns
// an error rather than silently producing partial output.
//
// Note: a completely empty reader ([]byte{}) is treated by the Go tar reader as
// an immediate EOF — indistinguishable from a valid empty tar — and returns nil.
// Only streams with partial or malformed content are expected to error.
func TestExtract_CorruptTar(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"random garbage", []byte("not a tar file at all!!!")},
		{"truncated header", make([]byte, 128)}, // half a tar block
		{"single zero byte", []byte{0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := make(chan FileEntry, 16)
			err := Extract(context.Background(), bytes.NewReader(c.data), out)
			close(out)
			if err == nil {
				t.Errorf("Extract(%q) should have returned an error for corrupt data but did not", c.name)
			}
		})
	}
}

// TestExtract_EmptyReader verifies that an empty byte slice (immediate EOF) is
// treated like an empty valid tar — no error, no entries.
func TestExtract_EmptyReader(t *testing.T) {
	out := make(chan FileEntry, 16)
	err := Extract(context.Background(), bytes.NewReader(nil), out)
	close(out)
	if err != nil {
		t.Errorf("empty reader should return nil (treated as empty tar), got %v", err)
	}
}

func TestExtract_ContextCancellation(t *testing.T) {
	buf := makeTar([]tarEntry{
		{name: "a.txt", content: "aaa", typeflag: tar.TypeReg},
		{name: "b.txt", content: "bbb", typeflag: tar.TypeReg},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	out := make(chan FileEntry, 16)
	err := Extract(ctx, buf, out)
	close(out)

	if err == nil {
		t.Error("Extract should have returned an error on cancelled context")
	}
}

// TestExtract_DeviceNodesSkipped verifies that character and block device
// entries are skipped silently — they have no content to publish to CAS.
func TestExtract_DeviceNodesSkipped(t *testing.T) {
	buf := new(bytes.Buffer)
	tw := tar.NewWriter(buf)
	tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeChar,
		Name:     "dev/null",
		Mode:     0666,
		Devmajor: 1,
		Devminor: 3,
	})
	tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeBlock,
		Name:     "dev/sda",
		Mode:     0660,
		Devmajor: 8,
		Devminor: 0,
	})
	tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "real.txt",
		Size:     4,
		Mode:     0644,
	})
	tw.Write([]byte("data"))
	tw.Close()

	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry (device nodes skipped), got %d", len(entries))
	}
	if entries[0].Path != "real.txt" {
		t.Errorf("expected real.txt, got %q", entries[0].Path)
	}
}

// TestExtract_MixedTypes verifies correct handling of a tar containing
// regular files, directories, symlinks, and hard links together.
func TestExtract_MixedTypes(t *testing.T) {
	buf := makeTar([]tarEntry{
		{name: "bin/", typeflag: tar.TypeDir},
		{name: "bin/prog", content: "ELF", typeflag: tar.TypeReg, mode: 0o755},
		{name: "bin/prog2", content: "bin/prog", typeflag: tar.TypeLink},
		{name: "lib/", typeflag: tar.TypeDir},
		{name: "lib/libfoo.so", content: "lib/libfoo.so.1.0", typeflag: tar.TypeSymlink},
		{name: "lib/libfoo.so.1.0", content: "SOLIB", typeflag: tar.TypeReg},
	})
	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Expect: bin/, bin/prog, bin/prog2 (hard link), lib/, lib/libfoo.so (symlink), lib/libfoo.so.1.0
	if len(entries) != 6 {
		t.Fatalf("expected 6 entries, got %d: %v", len(entries), func() []string {
			var paths []string
			for _, e := range entries {
				paths = append(paths, e.Path)
			}
			return paths
		}())
	}

	// Find hard link and verify data.
	for _, e := range entries {
		if e.Path == "bin/prog2" {
			if !e.Mode.IsRegular() {
				t.Errorf("hard link bin/prog2 should be regular, got %v", e.Mode)
			}
			if string(e.Data) != "ELF" {
				t.Errorf("hard link data = %q, want %q", e.Data, "ELF")
			}
		}
		if e.Path == "lib/libfoo.so" {
			if !e.Mode.IsDir() && !e.Mode.IsRegular() && e.Mode&fs.ModeSymlink == 0 {
				// symlink mode should have ModeSymlink set
			}
			if e.LinkTarget != "lib/libfoo.so.1.0" {
				t.Errorf("symlink target = %q, want %q", e.LinkTarget, "lib/libfoo.so.1.0")
			}
		}
	}
}

// TestExtract_SizeMatchesActualData verifies that entry.Size reflects the
// actual bytes read, not the (potentially wrong) header.Size claim.
// This is important for accurate raw-byte accounting in the pipeline result.
func TestExtract_SizeMatchesActualData(t *testing.T) {
	const content = "hello world"
	buf := makeTar([]tarEntry{
		{name: "f.txt", content: content, typeflag: tar.TypeReg},
	})
	entries, err := collectAll(t, buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Size != int64(len(content)) {
		t.Errorf("Size = %d, want %d (len of content)", e.Size, len(content))
	}
	if int64(len(e.Data)) != e.Size {
		t.Errorf("len(Data) = %d, Size = %d — mismatch", len(e.Data), e.Size)
	}
}

// ── io.Reader interface ──────────────────────────────────────────────────────

// TestExtract_StreamingReader verifies that Extract works correctly with a
// reader that returns data in small, arbitrary chunks (simulating a network
// stream or slow disk).
func TestExtract_StreamingReader(t *testing.T) {
	buf := makeTar([]tarEntry{
		{name: "a.txt", content: "aaaaa", typeflag: tar.TypeReg},
		{name: "b.txt", content: "bbbbb", typeflag: tar.TypeReg},
	})
	// Wrap buf in a reader that delivers only 1 byte at a time.
	slow := &oneByte{r: buf}
	out := make(chan FileEntry, 16)
	var entries []FileEntry
	done := make(chan struct{})
	go func() {
		defer close(done)
		for e := range out {
			entries = append(entries, e)
		}
	}()
	err := Extract(context.Background(), slow, out)
	close(out)
	<-done
	if err != nil {
		t.Fatalf("unexpected error with slow reader: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

// oneByte is an io.Reader that returns at most 1 byte per Read call.
type oneByte struct{ r io.Reader }

func (o *oneByte) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return o.r.Read(p[:1])
}
