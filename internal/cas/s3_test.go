// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cas

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

// ── config parsing ────────────────────────────────────────────────────────────

func TestParseS3Upstream(t *testing.T) {
	cases := []struct {
		in        string
		alias     string
		conf      string
		expectErr bool
	}{
		{"S3,/var/spool/cvmfs/repo/tmp,myrepo@/etc/cvmfs/s3.conf", "myrepo", "/etc/cvmfs/s3.conf", false},
		{"myrepo@/etc/cvmfs/s3.conf", "myrepo", "/etc/cvmfs/s3.conf", false},
		{"  S3,/tmp,alias@/x/y.conf  ", "alias", "/x/y.conf", false},
		// A local-storage repository must be rejected with a clear message
		// rather than silently mis-parsed.
		{"local,/srv/cvmfs/repo,/srv/cvmfs/repo", "", "", true},
		{"S3,/tmp,no-at-sign", "", "", true},
	}
	for _, c := range cases {
		alias, conf, err := ParseS3Upstream(c.in)
		if c.expectErr {
			if err == nil {
				t.Errorf("ParseS3Upstream(%q): expected error, got %q@%q", c.in, alias, conf)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseS3Upstream(%q): %v", c.in, err)
			continue
		}
		if alias != c.alias || conf != c.conf {
			t.Errorf("ParseS3Upstream(%q) = %q,%q; want %q,%q", c.in, alias, conf, c.alias, c.conf)
		}
	}
}

func TestLoadS3SettingsFromServerConf(t *testing.T) {
	dir := t.TempDir()
	s3conf := filepath.Join(dir, "s3.conf")
	if err := os.WriteFile(s3conf, []byte(`
# CVMFS S3 configuration
CVMFS_S3_HOST=s3.cern.ch
CVMFS_S3_PORT=8080
CVMFS_S3_BUCKET="my-bucket"
CVMFS_S3_ACCESS_KEY=AKIA
CVMFS_S3_SECRET_KEY='sekrit'
CVMFS_S3_REGION=cern
CVMFS_S3_USE_HTTPS=yes
`), 0o600); err != nil {
		t.Fatal(err)
	}
	serverConf := filepath.Join(dir, "server.conf")
	if err := os.WriteFile(serverConf, []byte(
		"CVMFS_UPSTREAM_STORAGE=S3,/var/spool/cvmfs/x/tmp,alias.cern.ch@"+s3conf+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	st, err := LoadS3SettingsFromServerConf(serverConf)
	if err != nil {
		t.Fatalf("LoadS3SettingsFromServerConf: %v", err)
	}
	if st.RepoAlias != "alias.cern.ch" || st.Bucket != "my-bucket" || st.SecretKey != "sekrit" {
		t.Errorf("bad settings: %s", st) // String() redacts the secret
	}
	if got := st.Endpoint(); got != "https://s3.cern.ch:8080" {
		t.Errorf("Endpoint() = %q, want https://s3.cern.ch:8080", got)
	}
	if st.EffectiveRegion() != "cern" {
		t.Errorf("EffectiveRegion() = %q", st.EffectiveRegion())
	}
	// The ACL default matters: objects are served over HTTP straight from the
	// bucket, so an unset ACL must NOT mean "private".
	if st.ACL != "public-read" {
		t.Errorf("ACL default = %q, want public-read (upload_s3.cc:60)", st.ACL)
	}
	// DNS buckets unset => virtual-host, exactly as upload_s3.cc:49 defaults.
	if !st.DNSBuckets {
		t.Error("DNSBuckets must default to TRUE, mirroring upload_s3.cc:49")
	}
	if !st.PeekBeforePut {
		t.Error("PeekBeforePut must default to TRUE, mirroring upload_s3.cc:54")
	}
}

func TestLoadS3SettingsRejectsNonS3Upstream(t *testing.T) {
	dir := t.TempDir()
	serverConf := filepath.Join(dir, "server.conf")
	if err := os.WriteFile(serverConf,
		[]byte("CVMFS_UPSTREAM_STORAGE=local,/srv/cvmfs/repo,/srv/cvmfs/repo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadS3SettingsFromServerConf(serverConf); err == nil {
		t.Fatal("expected an error for a local-storage repository")
	} else if !strings.Contains(err.Error(), "not S3") {
		t.Errorf("error should say the upstream is not S3, got: %v", err)
	}
}

// ── fake S3 ───────────────────────────────────────────────────────────────────

// fakeS3 is a minimal path-style S3 implementation: PUT/GET/HEAD/DELETE on
// /<bucket>/<key> plus ListObjectsV2 with prefix and continuation.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	acls    map[string]string
	puts    int
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string][]byte{}, acls: map[string]string{}}
}

func (f *fakeS3) handler(bucket string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()

		trimmed := strings.TrimPrefix(r.URL.Path, "/")
		if r.URL.Query().Has("list-type") && trimmed == bucket {
			f.list(w, r)
			return
		}
		key := strings.TrimPrefix(trimmed, bucket+"/")

		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			f.objects[key] = body
			f.acls[key] = r.Header.Get("x-amz-acl")
			f.puts++
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			b, ok := f.objects[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(b)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			b, ok := f.objects[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write(b)
		case http.MethodDelete:
			delete(f.objects, key)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

func (f *fakeS3) list(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>`)
	for _, k := range keys {
		fmt.Fprintf(&sb, "<Contents><Key>%s</Key><Size>%d</Size></Contents>", k, len(f.objects[k]))
	}
	sb.WriteString(`<IsTruncated>false</IsTruncated></ListBucketResult>`)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(sb.String()))
}

func newTestS3(t *testing.T, alias string) (*S3, *fakeS3) {
	t.Helper()
	fake := newFakeS3()
	srv := httptest.NewServer(fake.handler("cvmfs-bucket"))
	t.Cleanup(srv.Close)

	host := strings.TrimPrefix(srv.URL, "http://")
	st := S3Settings{
		RepoAlias:     alias,
		Host:          host,
		Bucket:        "cvmfs-bucket",
		Region:        "us-east-1",
		AccessKey:     "key",
		SecretKey:     "secret",
		ACL:           "public-read",
		PeekBeforePut: true,
		// The fake serves /<bucket>/<key>; DNSBuckets=false selects path-style.
		DNSBuckets: false,
	}
	b, err := NewS3(context.Background(), st)
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}
	return b, fake
}

// ── key layout ────────────────────────────────────────────────────────────────

// TestS3KeyMatchesCVMFSLayout is the load-bearing test: the client resolves
// object URLs straight from catalogs, so our keys must equal what the C++
// uploader writes — upload_s3.cc:478
//
//	final_path = repository_alias_ + "/data/" + content_hash.MakePath()
//
// A wrong prefix or a dropped suffix yields catalogs whose objects 404, which
// surfaces to users as EIO rather than as an upload error.
func TestS3KeyMatchesCVMFSLayout(t *testing.T) {
	b, _ := newTestS3(t, "atlas.cern.ch")

	cases := map[string]string{
		// plain object
		"abcdef0123456789abcdef0123456789abcdef01": "atlas.cern.ch/data/ab/cdef0123456789abcdef0123456789abcdef01",
		// catalog object: the 'C' suffix is part of the hash string and must
		// survive into the key
		"abcdef0123456789abcdef0123456789abcdef01C": "atlas.cern.ch/data/ab/cdef0123456789abcdef0123456789abcdef01C",
		// chunk object: 'P' (kSuffixPartial)
		"0123456789abcdef0123456789abcdef01234567P": "atlas.cern.ch/data/01/23456789abcdef0123456789abcdef01234567P",
	}
	for hash, want := range cases {
		if got := b.key(hash); got != want {
			t.Errorf("key(%q)\n got %q\nwant %q", hash, got, want)
		}
	}
}

// ── round-trip behaviour ──────────────────────────────────────────────────────

func TestS3RoundTrip(t *testing.T) {
	ctx := context.Background()
	b, fake := newTestS3(t, "repo.cern.ch")
	hash := "aabbccddeeff00112233445566778899aabbccdd"
	content := []byte("hello cvmfs")

	ok, err := b.Exists(ctx, hash)
	if err != nil || ok {
		t.Fatalf("Exists before Put = %v, %v; want false, nil", ok, err)
	}
	if err := b.Put(ctx, hash, strings.NewReader(string(content)), int64(len(content))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	ok, err = b.Exists(ctx, hash)
	if err != nil || !ok {
		t.Fatalf("Exists after Put = %v, %v; want true, nil", ok, err)
	}
	size, err := b.Size(ctx, hash)
	if err != nil || size != int64(len(content)) {
		t.Fatalf("Size = %d, %v; want %d", size, err, len(content))
	}
	rc, err := b.Get(ctx, hash)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != string(content) {
		t.Errorf("Get = %q, want %q", got, content)
	}

	// The ACL must be sent, or the object is unreadable over HTTP (403 → EIO).
	if acl := fake.acls[b.key(hash)]; acl != "public-read" {
		t.Errorf("x-amz-acl = %q, want public-read", acl)
	}

	if err := b.Delete(ctx, hash); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ok, _ := b.Exists(ctx, hash); ok {
		t.Error("object still present after Delete")
	}
	// Deleting an absent object is not an error (idempotent cleanup).
	if err := b.Delete(ctx, hash); err != nil {
		t.Errorf("Delete of missing object: %v", err)
	}
}

// TestS3PutIsIdempotentAndNeverOverwrites guards the CAS invariant: a key
// already holds content hashing to that key, so re-uploading gains nothing and
// risks disturbing an object a published catalog already references.
func TestS3PutIsIdempotentAndNeverOverwrites(t *testing.T) {
	ctx := context.Background()
	b, fake := newTestS3(t, "repo.cern.ch")
	hash := "1111111111111111111111111111111111111111"

	if err := b.Put(ctx, hash, strings.NewReader("first"), 5); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := b.Put(ctx, hash, strings.NewReader("second"), 6); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if fake.puts != 1 {
		t.Errorf("PutObject called %d times, want 1 (second Put must be a no-op)", fake.puts)
	}
	rc, err := b.Get(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "first" {
		t.Errorf("object was overwritten: %q", got)
	}
}

func TestS3ListReturnsHashes(t *testing.T) {
	ctx := context.Background()
	b, fake := newTestS3(t, "repo.cern.ch")

	want := []string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbC",
		"ccccccccccccccccccccccccccccccccccccccccP",
	}
	for _, h := range want {
		if err := b.Put(ctx, h, strings.NewReader("x"), 1); err != nil {
			t.Fatal(err)
		}
	}
	// Foreign keys under the same bucket must not leak into the hash list.
	fake.objects["other.cern.ch/data/ff/eeee"] = []byte("x")
	fake.objects["repo.cern.ch/.cvmfspublished"] = []byte("x")

	got, err := b.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("List = %v, want %v", got, want)
	}
}

// The dedup stage should Bloom-filter before hitting S3, unlike LocalFS.
func TestS3ExistsIsNotNative(t *testing.T) {
	b, _ := newTestS3(t, "repo.cern.ch")
	if b.ExistsIsNative() {
		t.Error("S3.ExistsIsNative() must be false — every Exists is a network round trip")
	}
}

// ── regression guards from the review ─────────────────────────────────────────

// A hash must never be able to escape the "<alias>/data/" prefix. S3 keys have
// no root, so an unvalidated "../" would let a caller write the repository
// manifest itself.
func TestS3RejectsHashEscapingThePrefix(t *testing.T) {
	ctx := context.Background()
	b, fake := newTestS3(t, "repo.cern.ch")

	evil := []string{
		"aa/../../repo.cern.ch/.cvmfspublished",
		"../../../etc/passwd",
		"aabbccddeeff00112233445566778899aabbcc/d/",
		"",
		"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
	}
	for _, h := range evil {
		if err := b.Put(ctx, h, strings.NewReader("x"), 1); err == nil {
			t.Errorf("Put(%q) was accepted; must be rejected", h)
		}
		if _, err := b.Exists(ctx, h); err == nil {
			t.Errorf("Exists(%q) was accepted; must be rejected", h)
		}
	}
	if len(fake.objects) != 0 {
		t.Errorf("a rejected hash still wrote objects: %v", fake.objects)
	}
}

// CVMFS_S3_PORT that does not parse must be a hard error, never a silent
// fallback to port 80 (a different service entirely).
func TestBadPortIsFatal(t *testing.T) {
	dir := t.TempDir()
	s3conf := filepath.Join(dir, "s3.conf")
	if err := os.WriteFile(s3conf, []byte(
		"CVMFS_S3_HOST=h\nCVMFS_S3_BUCKET=b\nCVMFS_S3_ACCESS_KEY=a\nCVMFS_S3_SECRET_KEY=s\nCVMFS_S3_PORT=80x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	serverConf := filepath.Join(dir, "server.conf")
	if err := os.WriteFile(serverConf, []byte("CVMFS_UPSTREAM_STORAGE=S3,/tmp,a@"+s3conf+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadS3SettingsFromServerConf(serverConf); err == nil {
		t.Fatal("a non-numeric CVMFS_S3_PORT must be fatal")
	}
}

// CVMFS templates @fqrn@/@org@ appear in real configs; reading them literally
// would point prepub at a bucket that does not exist. Shell constructs we do
// not evaluate must fail loudly rather than be used verbatim.
func TestTemplateExpansionAndShellRejection(t *testing.T) {
	write := func(t *testing.T, bucket string) (string, error) {
		t.Helper()
		dir := t.TempDir()
		s3conf := filepath.Join(dir, "s3.conf")
		if err := os.WriteFile(s3conf, []byte(
			"export CVMFS_S3_HOST=h  # inline comment\nCVMFS_S3_BUCKET="+bucket+
				"\nCVMFS_S3_ACCESS_KEY=a\nCVMFS_S3_SECRET_KEY=s\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		serverConf := filepath.Join(dir, "server.conf")
		if err := os.WriteFile(serverConf,
			[]byte("CVMFS_UPSTREAM_STORAGE=S3,/tmp,atlas.cern.ch@"+s3conf+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		st, err := LoadS3SettingsFromServerConf(serverConf)
		return st.Bucket, err
	}

	got, err := write(t, "@fqrn@-data")
	if err != nil {
		t.Fatalf("template config rejected: %v", err)
	}
	if got != "atlas.cern.ch-data" {
		t.Errorf("bucket = %q, want atlas.cern.ch-data (@fqrn@ expanded)", got)
	}

	got, err = write(t, "@org@-data")
	if err != nil {
		t.Fatalf("@org@ config rejected: %v", err)
	}
	if got != "atlas-data" {
		t.Errorf("bucket = %q, want atlas-data (@org@ expanded)", got)
	}

	if _, err := write(t, "${REPO}-data"); err == nil {
		t.Error("a shell-expanded value must be rejected, not used literally")
	}
}

// "export KEY=value" and inline comments are common in these files.
func TestExportPrefixAndInlineComment(t *testing.T) {
	dir := t.TempDir()
	s3conf := filepath.Join(dir, "s3.conf")
	if err := os.WriteFile(s3conf, []byte(
		"export CVMFS_S3_HOST=s3.example.org\nCVMFS_S3_PORT=8080  # rgw\n"+
			"CVMFS_S3_BUCKET=b\nCVMFS_S3_ACCESS_KEY=a\nCVMFS_S3_SECRET_KEY=s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	serverConf := filepath.Join(dir, "server.conf")
	if err := os.WriteFile(serverConf, []byte("CVMFS_UPSTREAM_STORAGE=S3,/tmp,r@"+s3conf+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := LoadS3SettingsFromServerConf(serverConf)
	if err != nil {
		t.Fatalf("LoadS3SettingsFromServerConf: %v", err)
	}
	if st.Host != "s3.example.org" {
		t.Errorf("Host = %q — 'export ' prefix not stripped", st.Host)
	}
	if st.Port != 8080 {
		t.Errorf("Port = %d — inline comment not stripped", st.Port)
	}
}

// A host that already carries a scheme must not produce "http://https://…".
func TestEndpointHandlesSchemeAndIPv6(t *testing.T) {
	cases := []struct {
		in   S3Settings
		want string
	}{
		{S3Settings{Host: "https://s3.example.org"}, "https://s3.example.org"},
		{S3Settings{Host: "s3.example.org", Port: 8080}, "http://s3.example.org:8080"},
		{S3Settings{Host: "s3.example.org:9000", Port: 8080}, "http://s3.example.org:9000"},
		{S3Settings{Host: "[::1]", Port: 9000}, "http://[::1]:9000"},
		{S3Settings{Host: "[::1]:9000", Port: 8080}, "http://[::1]:9000"},
		{S3Settings{Host: "s3.example.org", UseHTTPS: true}, "https://s3.example.org"},
	}
	for _, c := range cases {
		if got := c.in.Endpoint(); got != c.want {
			t.Errorf("Endpoint(%q) = %q, want %q", c.in.Host, got, c.want)
		}
	}
}

// The secret key must not appear in any formatted representation.
func TestSettingsRedactSecret(t *testing.T) {
	st := S3Settings{RepoAlias: "r", Host: "h", Bucket: "b", AccessKey: "AK", SecretKey: "TOPSECRET"}
	for _, rendered := range []string{
		fmt.Sprintf("%v", st), fmt.Sprintf("%+v", st), fmt.Sprintf("%s", st),
	} {
		if strings.Contains(rendered, "TOPSECRET") {
			t.Errorf("secret key leaked into %q", rendered)
		}
	}
}

// The permission gate must ALLOW 0640 (root:service-account, group-readable) —
// that is the documented way to give the service account access to a
// root-owned config, and the error message itself recommends it. It must still
// reject world access and group-write.
func TestS3ConfPermissionGate(t *testing.T) {
	mk := func(t *testing.T, mode os.FileMode) error {
		t.Helper()
		dir := t.TempDir()
		s3conf := filepath.Join(dir, "s3.conf")
		if err := os.WriteFile(s3conf, []byte(
			"CVMFS_S3_HOST=h\nCVMFS_S3_BUCKET=b\nCVMFS_S3_ACCESS_KEY=a\nCVMFS_S3_SECRET_KEY=s\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(s3conf, mode); err != nil {
			t.Fatal(err)
		}
		serverConf := filepath.Join(dir, "server.conf")
		if err := os.WriteFile(serverConf, []byte("CVMFS_UPSTREAM_STORAGE=S3,/tmp,r@"+s3conf+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadS3SettingsFromServerConf(serverConf)
		return err
	}

	for _, mode := range []os.FileMode{0o600, 0o640} {
		if err := mk(t, mode); err != nil {
			t.Errorf("mode %#o must be accepted, got: %v", mode, err)
		}
	}
	for _, mode := range []os.FileMode{0o644, 0o660, 0o666, 0o604} {
		if err := mk(t, mode); err == nil {
			t.Errorf("mode %#o must be rejected (world access or group-write)", mode)
		}
	}
}

// The startup probe must be read-only on S3: writing a sentinel into a
// production bucket on every restart leaves objects no catalog references.
func TestS3ProbeIsReadOnly(t *testing.T) {
	ctx := context.Background()
	b, fake := newTestS3(t, "repo.cern.ch")

	var _ Prober = b // must satisfy the interface, or probe.go silently write-probes

	if err := b.Probe(ctx); err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if fake.puts != 0 || len(fake.objects) != 0 {
		t.Errorf("Probe wrote to the bucket: puts=%d objects=%d", fake.puts, len(fake.objects))
	}
}

// The probe hash used by the generic write-probe must be a valid CVMFS key,
// or a validating backend rejects it at startup (a 64-char SHA-256 did).
func TestProbeHashShapeIsValid(t *testing.T) {
	if err := validHashKey("e3b0c44298fc1c149afbf4c8996fb92427ae41e4"); err != nil {
		t.Errorf("the probe hash must be a valid CAS key: %v", err)
	}
	if err := validHashKey("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"); err == nil {
		t.Error("a 64-char SHA-256 must be rejected — it is not in the CVMFS hash enum")
	}
}
