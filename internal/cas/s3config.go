// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cas

// s3config.go — read the S3 settings the CVMFS server itself uses.
//
// The prepub writes objects into the SAME storage the repository is served
// from, so its S3 settings must not be an independent copy: a bucket, alias or
// credential that drifts from the repository's own configuration produces
// catalogs referencing objects nobody can fetch.  We therefore read the
// repository's existing configuration rather than duplicating it.
//
// Chain, mirroring the C++ uploader (cvmfs/upload_s3.cc):
//
//	/etc/cvmfs/repositories.d/<repo>/server.conf
//	  CVMFS_UPSTREAM_STORAGE=S3,<tmpdir>,<repo_alias>@<s3_config_path>
//	                                     └── ParseSpoolerDefinition splits on '@'
//	                                         (upload_s3.cc:117-128)
//	<s3_config_path>
//	  CVMFS_S3_HOST / _PORT / _BUCKET / _ACCESS_KEY / _SECRET_KEY /
//	  _REGION / _USE_HTTPS / _DNS_BUCKETS

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// S3Settings is the resolved configuration for the S3 CAS backend.
type S3Settings struct {
	// RepoAlias is the key prefix every object lives under. upload_s3.cc:478
	// builds "<repository_alias>/data/<hash-path>", so this must match the
	// repository's own alias exactly or the client fetches 404s.
	RepoAlias string

	Host      string // CVMFS_S3_HOST (may already include :port)
	Port      int    // CVMFS_S3_PORT (0 = unset)
	Bucket    string // CVMFS_S3_BUCKET
	Region    string // CVMFS_S3_REGION (empty = "us-east-1", what Ceph/RGW expects)
	AccessKey string // CVMFS_S3_ACCESS_KEY
	SecretKey string // CVMFS_S3_SECRET_KEY
	UseHTTPS  bool   // CVMFS_S3_USE_HTTPS
	// DNSBuckets selects virtual-host addressing (bucket.host).
	//
	// This mirrors the C++ uploader EXACTLY — default true, disabled only by
	// the literal "false" (upload_s3.cc:49 dns_buckets_(true), :164-167). We
	// must address the bucket the same way as the uploader that owns the
	// repository; inventing a different default here would send requests to a
	// URL shape the endpoint may not serve, and a 404 from virtual-host
	// addressing is indistinguishable from "object absent".
	DNSBuckets bool
	// PeekBeforePut mirrors CVMFS_S3_PEEK_BEFORE_PUT (default true,
	// upload_s3.cc:54): HEAD before PUT so an existing object is never
	// rewritten.
	PeekBeforePut bool
	// ACL is the canned ACL applied to every uploaded object
	// (CVMFS_S3_X_AMZ_ACL). It defaults to "public-read" exactly as the C++
	// uploader does (upload_s3.cc:60), because CVMFS objects are served
	// directly over HTTP from the bucket: uploading them without a readable
	// ACL yields 403 on every fetch, which surfaces to users as EIO rather
	// than as a permission error. Set to "-" to send no ACL header at all
	// (buckets with a blanket policy, or providers that reject the header).
	ACL string
}

// Endpoint returns the base URL for the S3 service.
//
// CVMFS_S3_HOST is a bare host[:port]; operators nonetheless write a scheme
// there regularly, which used to yield "http://https://s3.example.org" and an
// SDK error naming neither cause. An IPv6 literal is also handled: only a colon
// after the closing bracket counts as a port separator.
func (s S3Settings) Endpoint() string {
	scheme := "http"
	if s.UseHTTPS {
		scheme = "https"
	}
	host := s.Host
	if i := strings.Index(host, "://"); i >= 0 {
		// Honour an explicit scheme in the host over the derived one: it is
		// what the operator visibly asked for.
		scheme = host[:i]
		host = host[i+3:]
	}
	host = strings.TrimSuffix(host, "/")

	hasPort := false
	if j := strings.LastIndexByte(host, ']'); j >= 0 {
		hasPort = strings.IndexByte(host[j:], ':') >= 0 // [::1]:9000
	} else {
		hasPort = strings.IndexByte(host, ':') >= 0
	}
	if s.Port > 0 && !hasPort {
		host = fmt.Sprintf("%s:%d", host, s.Port)
	}
	return scheme + "://" + host
}

// LogValue redacts the secret key so an S3Settings can never leak a publish
// credential through a log line or a %+v in a test failure.
func (s S3Settings) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("alias", s.RepoAlias),
		slog.String("endpoint", s.Endpoint()),
		slog.String("bucket", s.Bucket),
		slog.String("region", s.EffectiveRegion()),
		slog.String("access_key", s.AccessKey),
		slog.String("secret_key", "[REDACTED]"),
		slog.String("acl", s.ACL),
		slog.Bool("dns_buckets", s.DNSBuckets),
	)
}

// String keeps fmt verbs (%v/%+v/%s) from printing the secret key.
func (s S3Settings) String() string {
	return fmt.Sprintf("S3Settings{alias:%s endpoint:%s bucket:%s region:%s acl:%s dns_buckets:%t secret:[REDACTED]}",
		s.RepoAlias, s.Endpoint(), s.Bucket, s.EffectiveRegion(), s.ACL, s.DNSBuckets)
}

// EffectiveRegion returns the region to sign with. SigV4 requires a non-empty
// region; Ceph RGW and MinIO conventionally accept "us-east-1".
func (s S3Settings) EffectiveRegion() string {
	if s.Region == "" {
		return "us-east-1"
	}
	return s.Region
}

// Validate reports the first missing mandatory field.
func (s S3Settings) Validate() error {
	switch {
	case s.RepoAlias == "":
		return fmt.Errorf("repository alias is empty (check CVMFS_UPSTREAM_STORAGE)")
	case s.Host == "":
		return fmt.Errorf("CVMFS_S3_HOST is not set")
	case s.Bucket == "":
		return fmt.Errorf("CVMFS_S3_BUCKET is not set")
	case s.AccessKey == "":
		return fmt.Errorf("CVMFS_S3_ACCESS_KEY is not set")
	case s.SecretKey == "":
		return fmt.Errorf("CVMFS_S3_SECRET_KEY is not set")
	}
	return nil
}

// LoadS3SettingsFromServerConf resolves the S3 settings for a repository from
// its server.conf, following CVMFS_UPSTREAM_STORAGE to the S3 config file.
func LoadS3SettingsFromServerConf(serverConfPath string) (S3Settings, error) {
	var out S3Settings

	kv, err := parseCVMFSConf(serverConfPath)
	if err != nil {
		return out, fmt.Errorf("reading %s: %w", serverConfPath, err)
	}
	upstream := kv["CVMFS_UPSTREAM_STORAGE"]
	if upstream == "" {
		return out, fmt.Errorf("%s: CVMFS_UPSTREAM_STORAGE is not set", serverConfPath)
	}
	alias, s3ConfPath, err := ParseS3Upstream(upstream)
	if err != nil {
		return out, err
	}

	s3kv, err := parseCVMFSConf(s3ConfPath)
	if err != nil {
		return out, fmt.Errorf("reading S3 config %s: %w", s3ConfPath, err)
	}
	// The S3 config holds CVMFS_S3_SECRET_KEY. Refuse to read it if it is
	// exposed beyond owner+group: a leaked publish credential is a repository
	// takeover, and this is the one moment we can cheaply notice.
	//
	// GROUP READ IS ALLOWED (0640) — it is how the service account is granted
	// access to a root-owned file, and is exactly what the error below tells
	// the operator to do. Rejecting it (perm&0o077) made the documented fix
	// impossible. What must not happen is any world access, or group WRITE,
	// which would let the group rewrite the endpoint and redirect publishes.
	if fi, serr := os.Stat(s3ConfPath); serr == nil {
		perm := fi.Mode().Perm()
		if perm&0o007 != 0 || perm&0o020 != 0 {
			return out, fmt.Errorf(
				"S3 config %s is mode %#o: it holds CVMFS_S3_SECRET_KEY and must not be "+
					"world-accessible or group-writable — run: "+
					"chown root:%s %s && chmod 0640 %s",
				s3ConfPath, perm, "<service-account>", s3ConfPath, s3ConfPath)
		}
	}
	// Expand @fqrn@/@org@ and reject anything else we do not evaluate.
	for k, v := range s3kv {
		if !strings.HasPrefix(k, "CVMFS_S3_") {
			continue
		}
		ev := expandTemplates(v, alias)
		if cerr := checkNoUnexpanded(s3ConfPath, k, ev); cerr != nil {
			return out, cerr
		}
		s3kv[k] = ev
	}

	out = S3Settings{
		RepoAlias: alias,
		Host:      s3kv["CVMFS_S3_HOST"],
		Bucket:    s3kv["CVMFS_S3_BUCKET"],
		Region:    s3kv["CVMFS_S3_REGION"],
		AccessKey: s3kv["CVMFS_S3_ACCESS_KEY"],
		SecretKey: s3kv["CVMFS_S3_SECRET_KEY"],
		UseHTTPS:  isOn(s3kv["CVMFS_S3_USE_HTTPS"]),
		// Mirror upload_s3.cc:164-167 exactly: default true, off only on the
		// literal "false", so prepub and the C++ uploader always agree on the
		// URL shape for the same config file.
		DNSBuckets: !strings.EqualFold(strings.TrimSpace(s3kv["CVMFS_S3_DNS_BUCKETS"]), "false"),
		ACL:        s3kv["CVMFS_S3_X_AMZ_ACL"],
	}
	if out.ACL == "" {
		// Same default as upload_s3.cc:60. Do NOT leave this empty: objects
		// uploaded without a readable ACL are served as 403 and the client
		// reports EIO.
		out.ACL = "public-read"
	}
	if p := s3kv["CVMFS_S3_PORT"]; p != "" {
		n, cerr := strconv.Atoi(p)
		if cerr != nil {
			// Never fall through to "no port": that silently sends every
			// request to :80, which may be an entirely different service, and
			// Validate() would still pass.
			return out, fmt.Errorf("%s: CVMFS_S3_PORT %q is not a number: %w", s3ConfPath, p, cerr)
		}
		out.Port = n
	}
	// CVMFS_S3_PEEK_BEFORE_PUT defaults to true in the C++ uploader
	// (upload_s3.cc:54). The pipeline already does its own CAS.Exists before
	// calling Put, so leaving this on costs a second HEAD per new object;
	// operators can turn it off to halve the round trips on upload-heavy
	// publishes.
	out.PeekBeforePut = true
	if v, ok := s3kv["CVMFS_S3_PEEK_BEFORE_PUT"]; ok && v != "" {
		out.PeekBeforePut = isOn(v)
	}
	return out, out.Validate()
}

// ParseS3Upstream splits a CVMFS_UPSTREAM_STORAGE value into the repository
// alias and the S3 config path.
//
// Accepted forms (the leading "S3," and temp-dir field are optional so callers
// may pass either the whole upstream line or just the spooler configuration):
//
//	S3,/var/spool/cvmfs/repo/tmp,myrepo@/etc/cvmfs/s3.conf
//	myrepo@/etc/cvmfs/s3.conf
func ParseS3Upstream(upstream string) (alias, configPath string, err error) {
	spec := strings.TrimSpace(upstream)
	if fields := strings.Split(spec, ","); len(fields) > 1 {
		if !strings.EqualFold(strings.TrimSpace(fields[0]), "S3") {
			return "", "", fmt.Errorf(
				"upstream storage is %q, not S3 — the s3 CAS backend cannot serve this repository",
				strings.TrimSpace(fields[0]))
		}
		spec = strings.TrimSpace(fields[len(fields)-1])
	}
	parts := strings.SplitN(spec, "@", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf(
			"cannot parse S3 spooler configuration %q; expected <repo_alias>@/path/to/s3.conf", spec)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

// expandTemplates substitutes CVMFS's config templates. The C++ side parses
// these files with a DefaultOptionsTemplateManager, which registers @fqrn@ and
// @org@ (options.cc:504-512), so a perfectly ordinary production config such as
//
//	CVMFS_S3_BUCKET=@fqrn@-data
//
// must expand before use. Reading the value literally would produce a bucket
// named "@fqrn@-data" — the exact configuration drift this file exists to
// prevent, merely relocated into the parser.
func expandTemplates(val, fqrn string) string {
	org := fqrn
	if i := strings.IndexByte(fqrn, '.'); i > 0 {
		org = fqrn[:i]
	}
	val = strings.ReplaceAll(val, "@fqrn@", fqrn)
	val = strings.ReplaceAll(val, "@org@", org)
	return val
}

// checkNoUnexpanded rejects values still containing shell or template
// constructs we do not evaluate. Failing loudly beats connecting to a host
// literally named "${S3_HOST}".
func checkNoUnexpanded(path, key, val string) error {
	if strings.ContainsAny(val, "$`") || strings.Contains(val, "@") {
		return fmt.Errorf(
			"%s: %s=%q contains an unsupported shell/template construct; "+
				"only @fqrn@ and @org@ are expanded", path, key, val)
	}
	return nil
}

// parseCVMFSConf reads a CVMFS shell-style config file into a map. Lines are
// KEY=value; comments and blanks are skipped, and surrounding quotes stripped.
func parseCVMFSConf(path string) (map[string]string, error) {
	f, err := os.Open(path) //nolint:gosec // operator-supplied config path
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read-only

	out := make(map[string]string)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// "export KEY=value" is common in these files; without stripping it the
		// key becomes "export CVMFS_S3_HOST" and the setting silently vanishes.
		line = strings.TrimPrefix(line, "export ")

		key, val, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)

		quoted := false
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') ||
				(val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
				quoted = true
			}
		}
		// Trailing comment on an unquoted value: "8080  # rgw" must not become
		// part of the port.
		if !quoted {
			if i := strings.IndexByte(val, '#'); i >= 0 {
				val = strings.TrimSpace(val[:i])
			}
		}
		out[key] = val
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// isOn mirrors CVMFS's OptionsManager::IsOn — "yes"/"on"/"true"/"1".
func isOn(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "yes", "on", "true", "1":
		return true
	}
	return false
}
