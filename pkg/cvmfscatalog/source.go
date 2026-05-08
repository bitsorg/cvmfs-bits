package cvmfscatalog

import (
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

// CatalogSource abstracts access to a CVMFS Stratum 0's content-addressable
// store.  Implementations may read over HTTP (the default), directly from a
// locally mounted Stratum 0 data directory, or from an object store such as S3.
//
// All three methods must honour context cancellation.
type CatalogSource interface {
	// Manifest returns the raw .cvmfspublished bytes for the given repo.
	// A nil slice (with a nil error) means the repository has never been
	// published (HTTP 404 / file-not-found equivalent).
	Manifest(ctx context.Context, repo string) ([]byte, error)

	// Catalog fetches and decompresses the catalog object identified by
	// hashHex and writes the raw SQLite bytes to destPath.
	// Returns ErrCatalogNotFound when the object does not exist.
	Catalog(ctx context.Context, repo, hashHex, destPath string) error

	// Object fetches and decompresses the content object identified by
	// hashHex (using the hash algorithm algo) and returns the raw bytes.
	Object(ctx context.Context, repo, hashHex string, algo HashAlgo) ([]byte, error)
}

// ── HTTPSource ────────────────────────────────────────────────────────────────

// HTTPSource implements CatalogSource by fetching objects over HTTP from a
// Stratum 0 web endpoint.  This is the default when no Source is configured.
type HTTPSource struct {
	// BaseURL is the HTTP base URL of the Stratum 0, e.g.
	// "http://stratum0.example.org".  No trailing slash.
	BaseURL string
	// Client is the HTTP client to use.  nil uses http.DefaultClient.
	Client *http.Client
}

func (s *HTTPSource) httpClient() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	return http.DefaultClient
}

// Manifest fetches .cvmfspublished over HTTP.
func (s *HTTPSource) Manifest(ctx context.Context, repo string) ([]byte, error) {
	url := s.BaseURL + "/" + repo + "/.cvmfspublished"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating manifest request: %w", err)
	}
	resp, err := s.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching manifest: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // first publish — no manifest yet
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("manifest http %d: %s", resp.StatusCode, url)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading manifest body: %w", err)
	}
	return data, nil
}

// Catalog delegates to DownloadCatalog.
func (s *HTTPSource) Catalog(ctx context.Context, repo, hashHex, destPath string) error {
	return DownloadCatalog(ctx, s.httpClient(), s.BaseURL, repo, hashHex, destPath)
}

// Object delegates to DownloadObject.
func (s *HTTPSource) Object(ctx context.Context, repo, hashHex string, algo HashAlgo) ([]byte, error) {
	return DownloadObject(ctx, s.httpClient(), s.BaseURL, repo, hashHex, algo)
}

// ── LocalFSSource ─────────────────────────────────────────────────────────────

// LocalFSSource implements CatalogSource by reading directly from a locally
// accessible Stratum 0 data directory.  It eliminates HTTP round-trips when
// the Stratum 0 CAS is mounted on the same host or via NFS/FUSE.
//
// DataRoot is the parent directory that contains per-repo sub-directories.
// For example, if repos live at "/srv/cvmfs/<repo>/data/...", DataRoot is
// "/srv/cvmfs".  Within each repo directory the standard CVMFS CAS layout is
// expected:
//
//	<DataRoot>/<repo>/.cvmfspublished       — manifest (plain text)
//	<DataRoot>/<repo>/data/<XY>/<hash[2:]>C — zlib-compressed catalog
//	<DataRoot>/<repo>/data/<XY>/<hash[2:]>  — zlib-compressed regular object
type LocalFSSource struct {
	DataRoot string
}

// Manifest reads the .cvmfspublished file directly from disk.
func (s *LocalFSSource) Manifest(_ context.Context, repo string) ([]byte, error) {
	path := filepath.Join(s.DataRoot, repo, ".cvmfspublished")
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil // repository not yet published
		}
		return nil, fmt.Errorf("reading local manifest %s: %w", path, err)
	}
	return data, nil
}

// Catalog decompresses the zlib-compressed catalog object from local disk and
// writes the raw SQLite bytes to destPath.
func (s *LocalFSSource) Catalog(_ context.Context, repo, hashHex, destPath string) error {
	// CVMFS CAS path: data/<XY>/<hash[2:]>C  (C = catalog content-type suffix)
	if len(hashHex) < 2 {
		return fmt.Errorf("invalid catalog hash: %q", hashHex)
	}
	casPath := filepath.Join(s.DataRoot, repo, "data", hashHex[:2], hashHex[2:]+"C")
	return zlibFileToPath(casPath, destPath, true)
}

// Object decompresses the zlib-compressed content object from local disk and
// returns the raw bytes.
func (s *LocalFSSource) Object(_ context.Context, repo, hashHex string, algo HashAlgo) ([]byte, error) {
	if len(hashHex) < 2 {
		return nil, fmt.Errorf("invalid object hash: %q", hashHex)
	}
	suffix := HashSuffix(algo)
	casPath := filepath.Join(s.DataRoot, repo, "data", hashHex[:2], hashHex[2:]+suffix)
	return zlibFileToBytes(casPath)
}

// ── zlib helpers ──────────────────────────────────────────────────────────────

// zlibFileToPath opens src, decompresses its zlib content, and writes the
// result to dst.  If isCatalog is true and src is missing, ErrCatalogNotFound
// is returned instead of the raw os.ErrNotExist.
func zlibFileToPath(src, dst string, isCatalog bool) error {
	f, err := os.Open(src)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if isCatalog {
				return ErrCatalogNotFound
			}
			return fmt.Errorf("object not found: %s", src)
		}
		return fmt.Errorf("opening %s: %w", src, err)
	}
	defer f.Close()

	zr, err := zlib.NewReader(f)
	if err != nil {
		return fmt.Errorf("creating zlib reader for %s: %w", src, err)
	}
	defer zr.Close()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("creating %s: %w", dst, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, zr); err != nil {
		return fmt.Errorf("decompressing %s → %s: %w", src, dst, err)
	}
	return nil
}

// zlibFileToBytes opens src, decompresses its zlib content, and returns the
// raw bytes.
func zlibFileToBytes(src string) ([]byte, error) {
	f, err := os.Open(src)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", src, err)
	}
	defer f.Close()

	zr, err := zlib.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("creating zlib reader for %s: %w", src, err)
	}
	defer zr.Close()

	data, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("reading decompressed %s: %w", src, err)
	}
	return data, nil
}

// ── sourceFromConfig ──────────────────────────────────────────────────────────

// sourceFromConfig returns the CatalogSource to use for a MergeConfig.
// If cfg.Source is non-nil it is returned directly.  Otherwise, if
// cfg.Stratum0URL is non-empty an HTTPSource is constructed from the URL and
// optional HTTP client.  Returns nil when neither is configured.
func sourceFromConfig(cfg MergeConfig) CatalogSource {
	if cfg.Source != nil {
		return cfg.Source
	}
	if cfg.Stratum0URL != "" {
		return &HTTPSource{BaseURL: cfg.Stratum0URL, Client: cfg.HTTPClient}
	}
	return nil
}
