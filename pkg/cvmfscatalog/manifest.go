package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// Manifest represents a parsed .cvmfspublished manifest.
type Manifest struct {
	RootHash string   // plain hex hash (no suffix)
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
			// Detect algorithm from suffix before stripping it.
			switch {
			case strings.HasSuffix(value, "-"):
				m.HashAlgo = HashSha256
			case strings.HasSuffix(value, "~"):
				m.HashAlgo = HashRipeMD160
			default:
				m.HashAlgo = HashSha1
			}
			m.RootHash = strings.TrimSuffix(strings.TrimSuffix(value, "-"), "~")
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

// DownloadObject fetches and decompresses a regular content object (NOT a
// catalog) from a stratum0 HTTP CAS.  hashHex is the plain hex hash without
// any suffix; algo is the hash algorithm used to construct the URL suffix
// ("" for SHA-1, "-" for SHA-256, "~" for RipeMD-160 — matching HashSuffix).
// The function decompresses the zlib-compressed payload and returns the raw
// bytes.  This is used, for example, to retrieve the .cvmfsdirtab file stored
// in an existing repository so its split rules can be applied to new entries.
func DownloadObject(ctx context.Context, client *http.Client, stratum0URL, repoName, hashHex string, algo HashAlgo) ([]byte, error) {
	if client == nil {
		client = http.DefaultClient
	}
	suffix := HashSuffix(algo)
	casPath := hashHex[:2] + "/" + hashHex + suffix
	url := stratum0URL + "/" + repoName + "/data/" + casPath

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
		client = http.DefaultClient
	}

	// Construct the CAS path: data/XY/hashC
	casPath := hashHex[:2] + "/" + hashHex + "C"
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
