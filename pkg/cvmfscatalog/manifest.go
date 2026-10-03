// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// defaultClient is the fallback for nil *http.Client parameters: unlike
// http.DefaultClient it carries a timeout, so a stalled Stratum0/gateway can
// never hang a caller indefinitely (JobTimeout defaults to disabled).
var defaultClient = &http.Client{Timeout: 60 * time.Second}

// ErrCatalogNotFound is returned by DownloadCatalog when the server responds
// with HTTP 404.  Callers use this sentinel to distinguish "catalog missing /
// repository not yet initialised" from other I/O errors.
var ErrCatalogNotFound = errors.New("catalog not found (404)")

// Manifest represents a parsed .cvmfspublished manifest.
type Manifest struct {
	RootHash string   // hex digest without the algorithm suffix
	HashAlgo HashAlgo // algorithm inferred from the suffix on the C field
	RepoName string
	Revision uint64
	TTL      int // TTL in seconds
}

// ParseManifest parses the text content of a .cvmfspublished file
// (everything before the "--" separator).
func ParseManifest(data []byte) (*Manifest, error) {
	m := &Manifest{}

	// Find the "--" separator and take everything before it
	sep := []byte("--")
	parts := bytes.SplitN(data, sep, 2)
	if len(parts) < 1 {
		return nil, fmt.Errorf("no manifest content found")
	}

	lines := strings.Split(string(parts[0]), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if len(line) < 2 {
			continue
		}

		key := line[0]
		value := strings.TrimSpace(line[1:])

		switch key {
		case 'C':
			// The C field is the hex digest followed by the CVMFS algorithm
			// suffix ("" for SHA-1, "-rmd160", "-shake128").
			m.HashAlgo = HashSha1
			m.RootHash = value
			for _, a := range []HashAlgo{HashRipeMD160, HashShake128} {
				if s := HashSuffix(a); strings.HasSuffix(value, s) {
					m.HashAlgo = a
					m.RootHash = strings.TrimSuffix(value, s)
					break
				}
			}
		case 'N':
			m.RepoName = value
		case 'S':
			// S is the revision (confusingly named in CVMFS)
			rev, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parsing revision: %w", err)
			}
			m.Revision = rev
		case 'D':
			// TTL in seconds
			ttl, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("parsing TTL: %w", err)
			}
			m.TTL = ttl
		}
	}

	if m.RootHash == "" {
		return nil, fmt.Errorf("no root hash found in manifest")
	}
	if m.RepoName == "" {
		return nil, fmt.Errorf("no repo name found in manifest")
	}

	return m, nil
}

// FetchManifestRootHash performs a lightweight GET of the .cvmfspublished
// manifest for the given repository and returns the current root catalog hash
// with its algorithm suffix and the CVMFS catalog content-type suffix 'C'
// appended (e.g. "abc123...C", 41 chars for SHA-1; "abc123...-rmd160C").
//
// Returns ("", nil) when the repository has never been published (HTTP 404).
// This is used by the orchestrator to obtain old_root_hash after acquiring a
// gateway lease but before committing, without downloading the full root
// catalog SQLite (BuildSubtree does not require a local catalog copy).
//
// The HTTP request is made with the provided context so that job cancellation
// and per-lease heartbeat timeouts propagate correctly.  client may be nil,
// in which case http.DefaultClient is used.
func FetchManifestRootHash(ctx context.Context, client *http.Client, stratum0URL, repo string) (string, error) {
	if client == nil {
		client = defaultClient
	}
	manifestURL := stratum0URL + "/" + repo + "/.cvmfspublished"
	req, err := http.NewRequestWithContext(ctx, "GET", manifestURL, nil)
	if err != nil {
		return "", fmt.Errorf("creating manifest request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetching manifest: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", nil // first publish — no existing manifest
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("manifest http %d: %s", resp.StatusCode, manifestURL)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading manifest: %w", err)
	}

	manifest, err := ParseManifest(data)
	if err != nil {
		return "", fmt.Errorf("parsing manifest: %w", err)
	}

	if manifest.RootHash == "" {
		return "", nil
	}
	// Append 'C' (CVMFS catalog content-type suffix) so the receiver's
	// LoadCatalogByHash assertion (assert kSuffixCatalog == effective_hash.suffix)
	// is satisfied.  The algorithm suffix goes before it, as in CVMFS hash
	// strings and CAS paths.
	return manifest.RootHash + HashSuffix(manifest.HashAlgo) + "C", nil
}

// DownloadObject fetches and decompresses a regular content object (NOT a
// catalog) from a stratum0 HTTP CAS.  hashHex is the plain hex hash without
// any suffix; algo is the hash algorithm used to construct the URL suffix
// ("" for SHA-1, "-rmd160", "-shake128" — see HashSuffix).
// The function decompresses the zlib-compressed payload and returns the raw
// bytes.  This is used, for example, to retrieve the .cvmfsdirtab file stored
// in an existing repository so its split rules can be applied to new entries.
func DownloadObject(ctx context.Context, client *http.Client, stratum0URL, repoName, hashHex string, algo HashAlgo) ([]byte, error) {
	return fetchObject(ctx, client, stratum0URL, repoName, hashHex+HashSuffix(algo))
}

// fetchObject downloads and decompresses the CAS object named by name: the
// hex digest followed by its algorithm and content-type suffixes.
func fetchObject(ctx context.Context, client *http.Client, stratum0URL, repoName, name string) ([]byte, error) {
	if client == nil {
		client = defaultClient
	}
	if len(name) < 3 {
		return nil, fmt.Errorf("invalid object name %q: too short", name)
	}
	// CVMFS CAS path: data/<first2>/<rest>, matching shash::MakePath().
	url := stratum0URL + "/" + repoName + "/data/" + name[:2] + "/" + name[2:]

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching object: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, url)
	}

	zr, err := zlib.NewReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("creating zlib reader for object: %w", err)
	}
	defer zr.Close()

	data, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("reading decompressed object: %w", err)
	}
	return data, nil
}

// DownloadCatalog fetches and decompresses a catalog from a stratum0 HTTP CAS.
// hashHex is the plain hex hash (without suffix). The "C" suffix is appended to the filename.
// The result is written to destPath as a plain (decompressed) SQLite file.
func DownloadCatalog(ctx context.Context, client *http.Client, stratum0URL, repoName, hashHex, destPath string) error {
	if client == nil {
		client = defaultClient
	}

	// Construct the CAS path: data/<first2>/<remaining38>C
	// The filename is hash[2:], NOT the full hash — matching shash::MakePath().
	casPath := hashHex[:2] + "/" + hashHex[2:] + "C"
	url := stratum0URL + "/" + repoName + "/data/" + casPath

	// Fetch the file
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("fetching catalog: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrCatalogNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http %d: %s", resp.StatusCode, url)
	}

	// Decompress with zlib
	zr, err := zlib.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("creating zlib reader: %w", err)
	}
	defer zr.Close()

	// Write decompressed data to destPath
	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, zr); err != nil {
		return fmt.Errorf("writing decompressed catalog: %w", err)
	}

	return nil
}
