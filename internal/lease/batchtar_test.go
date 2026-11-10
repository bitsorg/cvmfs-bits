// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type batchTestEntry struct {
	hdr  tar.Header
	body string
}

func writeTestTar(t *testing.T, entries []batchTestEntry) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "payload.tar")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(f)
	for _, e := range entries {
		h := e.hdr
		if h.Typeflag == tar.TypeReg {
			h.Size = int64(len(e.body))
		}
		if h.Mode == 0 && h.Typeflag != tar.TypeXGlobalHeader {
			h.Mode = 0o644
		}
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(tw, e.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return p
}

func readTestTar(t *testing.T, data []byte) map[string]batchTestEntry {
	t.Helper()
	got := map[string]batchTestEntry{}
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return got
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, dup := got[h.Name]; dup {
			t.Errorf("entry %q written twice", h.Name)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		got[h.Name] = batchTestEntry{hdr: *h, body: string(b)}
	}
}

func batchDir(name string) batchTestEntry {
	return batchTestEntry{hdr: tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: 0o755}}
}
func batchFile(name, body string) batchTestEntry {
	return batchTestEntry{hdr: tar.Header{Name: name, Typeflag: tar.TypeReg}, body: body}
}

// Each member lands under its path with a marker at its root; file contents,
// symlink targets and hard links (renamed with their member) survive.
func TestWriteBatchTar_TwoMembers(t *testing.T) {
	long := "./" + strings.Repeat("d", 90) + "/" + strings.Repeat("f", 120) // PAX in the batch
	a := writeTestTar(t, []batchTestEntry{batchDir("./"), batchDir("./bin/"), batchFile("./bin/tool", "A"),
		{hdr: tar.Header{Name: "./bin/alias", Typeflag: tar.TypeSymlink, Linkname: "tool"}},
		{hdr: tar.Header{Name: "./bin/copy", Typeflag: tar.TypeLink, Linkname: "./bin/tool"}},
		batchFile(long, "L")})
	b := writeTestTar(t, []batchTestEntry{batchDir("./"), batchFile("lib/x.so", "B")})

	var out bytes.Buffer
	err := WriteBatchTar(&out, []BatchMember{{TarPath: a, RelPath: "Packages/A/1-1"}, {TarPath: b, RelPath: "Modules/modulefiles/B"}})
	if err != nil {
		t.Fatal(err)
	}
	got := readTestTar(t, out.Bytes())

	want := map[string]string{
		"Packages/A/1-1/bin/tool": "A", "Modules/modulefiles/B/lib/x.so": "B",
		"Packages/A/1-1/" + long[2:]:   "L",
		"Packages/A/1-1/.cvmfscatalog": "", "Modules/modulefiles/B/.cvmfscatalog": "",
	}
	for name, body := range want {
		e, ok := got[name]
		if !ok || e.body != body || e.hdr.Typeflag != tar.TypeReg {
			t.Errorf("%s: present=%v body=%q type=%c", name, ok, e.body, e.hdr.Typeflag)
		}
	}
	if e := got["Packages/A/1-1"]; e.hdr.Typeflag != tar.TypeDir {
		t.Errorf("member root directory missing: %+v", e.hdr)
	}
	if e := got["Packages/A/1-1/bin/alias"]; e.hdr.Linkname != "tool" {
		t.Errorf("symlink target %q, want it unchanged", e.hdr.Linkname)
	}
	if e := got["Packages/A/1-1/bin/copy"]; e.hdr.Linkname != "Packages/A/1-1/bin/tool" {
		t.Errorf("hard link to %q, want Packages/A/1-1/bin/tool", e.hdr.Linkname)
	}
	if len(got) != 10 {
		t.Errorf("%d entries, want 10: %v", len(got), got)
	}
}

// A member that already has a marker at its root gets no second one, and a
// global header, which would apply to the members after it, is dropped.
func TestWriteBatchTar_OwnMarkerAndGlobalHeader(t *testing.T) {
	a := writeTestTar(t, []batchTestEntry{
		{hdr: tar.Header{Typeflag: tar.TypeXGlobalHeader, PAXRecords: map[string]string{"comment": "x"}}},
		batchFile("./.cvmfscatalog", ""), batchFile("./f", "x")})
	var out bytes.Buffer
	if err := WriteBatchTar(&out, []BatchMember{{TarPath: a, RelPath: "P/a"}}); err != nil {
		t.Fatal(err)
	}
	got := readTestTar(t, out.Bytes())
	for name, e := range got {
		if e.hdr.Typeflag == tar.TypeXGlobalHeader || e.hdr.PAXRecords["comment"] != "" {
			t.Errorf("global header carried into the batch: %s %+v", name, e.hdr)
		}
	}
	if _, ok := got["P/a/.cvmfscatalog"]; !ok || len(got) != 2 || got["P/a/f"].body != "x" {
		t.Errorf("entries %v, want the member's own marker and f", got)
	}
}

// A refusal comes before anything is written, so the caller can fall back
// with nothing half-sent.
func TestWriteBatchTar_RefusesBeforeWriting(t *testing.T) {
	good := writeTestTar(t, []batchTestEntry{batchFile("./f", "x")})
	bad := writeTestTar(t, []batchTestEntry{batchFile("../escape", "x")})
	var out bytes.Buffer
	if err := WriteBatchTar(&out, []BatchMember{{TarPath: good, RelPath: "P/a"}, {TarPath: bad, RelPath: "P/b"}}); err == nil || out.Len() != 0 {
		t.Errorf("err=%v, %d bytes written; want an error and nothing written", err, out.Len())
	}
}

// Anything that would put an entry outside its member's directory is refused,
// so the caller publishes that package alone instead.
func TestWriteBatchTar_Refuses(t *testing.T) {
	ok := writeTestTar(t, []batchTestEntry{batchFile("./f", "x")})
	cases := map[string][]BatchMember{
		"dotdot entry":   {{TarPath: writeTestTar(t, []batchTestEntry{batchFile("../escape", "x")}), RelPath: "P/a"}},
		"absolute entry": {{TarPath: writeTestTar(t, []batchTestEntry{batchFile("/etc/passwd", "x")}), RelPath: "P/a"}},
		"hard link out":  {{TarPath: writeTestTar(t, []batchTestEntry{{hdr: tar.Header{Name: "l", Typeflag: tar.TypeLink, Linkname: "../../x"}}}), RelPath: "P/a"}},
		"no members":     nil,
		"empty path":     {{TarPath: ok, RelPath: ""}},
		"absolute path":  {{TarPath: ok, RelPath: "/P/a"}},
		"unclean path":   {{TarPath: ok, RelPath: "P/x/../a"}},
		"same path":      {{TarPath: ok, RelPath: "P/a"}, {TarPath: ok, RelPath: "P/a"}},
		"nested members": {{TarPath: ok, RelPath: "P/a"}, {TarPath: ok, RelPath: "P/a/sub"}},
		"missing tar":    {{TarPath: filepath.Join(t.TempDir(), "none.tar"), RelPath: "P/a"}},
		"file as root":   {{TarPath: writeTestTar(t, []batchTestEntry{batchFile(".", "x")}), RelPath: "P/a"}},
		"hard link root": {{TarPath: writeTestTar(t, []batchTestEntry{{hdr: tar.Header{Name: "l", Typeflag: tar.TypeLink, Linkname: "."}}}), RelPath: "P/a"}},
		"sparse":         {{TarPath: gnuSparseTar(t), RelPath: "P/a"}},
		"not a tar":      {{TarPath: writeRaw(t, "this is not a tar archive, just some text that is long enough"), RelPath: "P/a"}},
	}
	for name, members := range cases {
		if err := WriteBatchTar(io.Discard, members); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := WriteBatchTar(io.Discard, []BatchMember{{TarPath: ok, RelPath: "P/a"}, {TarPath: ok, RelPath: "P/ab"}}); err != nil {
		t.Errorf("siblings with a common name prefix refused: %v", err)
	}
}

func writeRaw(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "raw.tar")
	if err := os.WriteFile(p, []byte(strings.Repeat(content, 20)), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// gnuSparseTar is `tar --sparse --format=gnu` of a 1 MiB hole and 4 bytes:
// Go reads it as type 'S' with the holes filled, and would write it back
// without its sparse map.
func gnuSparseTar(t *testing.T) string {
	t.Helper()
	gz, err := base64.StdEncoding.DecodeString("H4sIAAAAAAACA+3QwQnDMAyFYY3iEWJXSgbpBIYGcggYXHf/iF4S2pIcQguF/7sI3hMINJV5lC/rXK/6nO51OpVoadCYLKnn0fqLSbjKDzzuLdcQpJbS9vaO+lPf0Q8fWXPd5N2ZA/pe3nLLAgAAAAAAAAAAAAAAAAD4Owug3RMmACgAAA==")
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "sparse.tar")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// A name that fits the member's ustar header no longer does with the member's
// path in front: the batch must switch format rather than fail.
func TestWriteBatchTar_UstarNameGrows(t *testing.T) {
	name := strings.Repeat("d", 150) + "/" + strings.Repeat("f", 90)
	a := writeTestTar(t, []batchTestEntry{{hdr: tar.Header{Name: name, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}}})
	var out bytes.Buffer
	if err := WriteBatchTar(&out, []BatchMember{{TarPath: a, RelPath: "Packages/A/1-1"}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := readTestTar(t, out.Bytes())["Packages/A/1-1/"+name]; !ok {
		t.Errorf("renamed entry missing")
	}
}
