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
	// DNSBuckets selects virtual-host addressing (bucket.host). CVMFS defaults
	// it OFF, and Ceph RGW deployments are normally path-style, so the backend
	// uses path-style unless this is explicitly on.
	DNSBuckets bool
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
func (s S3Settings) Endpoint() string {
	scheme := "http"
	if s.UseHTTPS {
		scheme = "https"
	}
	host := s.Host
	// CVMFS_S3_HOST may already carry a port; only append when it does not.
	if s.Port > 0 && !strings.Contains(host, ":") {
		host = fmt.Sprintf("%s:%d", host, s.Port)
	}
	return scheme + "://" + host
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

	out = S3Settings{
		RepoAlias: alias,
		Host:      s3kv["CVMFS_S3_HOST"],
		Bucket:    s3kv["CVMFS_S3_BUCKET"],
		Region:    s3kv["CVMFS_S3_REGION"],
		AccessKey: s3kv["CVMFS_S3_ACCESS_KEY"],
		SecretKey: s3kv["CVMFS_S3_SECRET_KEY"],
		UseHTTPS:  isOn(s3kv["CVMFS_S3_USE_HTTPS"]),
		// CVMFS_S3_DNS_BUCKETS defaults to ON upstream, but every deployment we
		// target (Ceph RGW, MinIO) is path-style; require it to be explicitly
		// on rather than inferring, so a missing key cannot silently produce
		// unreachable virtual-host URLs.
		DNSBuckets: isOn(s3kv["CVMFS_S3_DNS_BUCKETS"]),
		ACL:        s3kv["CVMFS_S3_X_AMZ_ACL"],
	}
	if out.ACL == "" {
		// Same default as upload_s3.cc:60. Do NOT leave this empty: objects
		// uploaded without a readable ACL are served as 403 and the client
		// reports EIO.
		out.ACL = "public-read"
	}
	if p := s3kv["CVMFS_S3_PORT"]; p != "" {
		if n, cerr := strconv.Atoi(p); cerr == nil {
			out.Port = n
		}
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
		key, val, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') ||
				(val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
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
