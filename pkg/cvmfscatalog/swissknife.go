// Package cvmfscatalog — swissknife.go
//
// SwissKnifeIngest delegates an entire publish job to the cvmfs_swissknife
// ingest command.  It is a drop-in alternative to the Go pipeline
// (dedup + compress + CAS write) + BuildSubtree + gateway HTTP commit.
//
// Performance rationale
// ─────────────────────
// The gateway-native ingest path (cvmfs_server ingest, which internally runs
// cvmfs_swissknife ingest) typically finishes 30–60 % faster than the
// equivalent Go pipeline for large publishes because:
//
//   • The C++ zlib and SHA-1 implementations have SIMD intrinsics that the
//     Go runtime cannot use (Go's compress/zlib delegates to a pure-Go
//     flate implementation).
//   • cvmfs_swissknife pipelines decompression, hashing, and uploading in
//     a single C++ pass; our Go pipeline re-reads compressed bytes from
//     memory for the CAS write after they are produced by compress.Run.
//   • The SQLite catalog is built with the same C++ CVMFS catalog code
//     used by the receiver, avoiding any schema mismatch risk.
//
// Trade-offs
// ──────────
// Using cvmfs_swissknife disables:
//   • Go-side Bloom-filter dedup (swissknife does its own dedup if -e is set)
//   • Stratum 1 pre-warming via DistManager (no per-object CAS records)
//   • Per-object provenance / Rekor hashes (no ObjectHashes recorded)
//   • Observability: pipeline metrics, per-phase durations still emit, but
//     internal swissknife phases are opaque.
//
// Enable via --swissknife (publisher flag) and configure with
// --swissknife-binary, --swissknife-repo-keys, and optionally
// --swissknife-concurrency / --swissknife-extra-args.

package cvmfscatalog

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// SwissKnifeConfig holds configuration for invoking cvmfs_swissknife ingest
// as an alternative to the Go pipeline + BuildSubtree + gateway commit.
//
// All fields have safe zero values; set only the ones relevant to your
// deployment.  The RepoKeysDir field is required — cvmfs_swissknife uses it
// to verify the repository manifest signature during the commit phase.
type SwissKnifeConfig struct {
	// Binary is the path to the cvmfs_swissknife executable.
	// Empty string resolves to "cvmfs_swissknife" via PATH.
	Binary string

	// GatewayURL is the cvmfs_gateway HTTP API base URL
	// (e.g. "http://localhost:4929").  Pass this to use the gateway for
	// lease acquisition and commit (-g flag); leave empty for direct
	// Stratum 0 write (local mode, -s flag path, rarely used with bits).
	GatewayURL string

	// RepoKeysDir is the path to the directory containing repository
	// public-key files (e.g. "/etc/cvmfs/keys").  cvmfs_swissknife reads
	// <RepoKeysDir>/<repo>.pub (or auto-discovers keys in the directory)
	// to verify the manifest signature.  Required — swissknife will fail
	// to start without a valid key path.
	RepoKeysDir string

	// Concurrency is the number of internal upload threads (-q flag).
	// 0 means "do not set; use the swissknife default" (typically 4).
	Concurrency int

	// ExtraArgs are additional flags appended verbatim to every invocation.
	// Examples: []string{"-e"} to enable swissknife-side dedup,
	// []string{"-x"} to delete files absent from the tar,
	// []string{"--compression-alg", "zstd"} on supported builds.
	ExtraArgs []string
}

// SwissKnifeIngest runs cvmfs_swissknife ingest to publish the tar at
// tarPath into leasePath within repo.  It replaces the Go pipeline
// (compress + dedup + CAS write), the Go catalog builder (BuildSubtree),
// and the gateway commit in a single optimised C++ call.
//
// stratum0URL is the base Stratum 0 HTTP URL WITHOUT a trailing repo
// component (e.g. "http://stratum0/cvmfs"); the function appends the
// repository name automatically to produce the URL that swissknife
// expects for its -u flag.
//
// logFn receives each line of combined stdout+stderr as it arrives.
// It is called sequentially from a single goroutine; no locking needed.
//
// The equivalent shell command is:
//
//	cvmfs_swissknife ingest \
//	  -r <repo> \
//	  -u <stratum0URL>/<repo> \
//	  -k <cfg.RepoKeysDir> \
//	  -t <tarPath> \
//	  -b <leasePath> \
//	  [-g <cfg.GatewayURL>] \
//	  [-q <cfg.Concurrency>] \
//	  [cfg.ExtraArgs...]
func SwissKnifeIngest(
	ctx context.Context,
	cfg SwissKnifeConfig,
	stratum0URL, tarPath, repo, leasePath string,
	logFn func(string),
) error {
	binary := cfg.Binary
	if binary == "" {
		binary = "cvmfs_swissknife"
	}

	// Construct the per-repo Stratum 0 URL.
	// Our convention: stratum0URL = "http://stratum0/cvmfs" (no repo suffix).
	// cvmfs_swissknife -u expects the full repo URL:
	// "http://stratum0/cvmfs/<repo>".
	repoURL := strings.TrimRight(stratum0URL, "/") + "/" + repo

	args := []string{
		"ingest",
		"-r", repo,
		"-u", repoURL,
		"-k", cfg.RepoKeysDir,
		"-t", tarPath,
		"-b", leasePath,
	}
	if cfg.GatewayURL != "" {
		args = append(args, "-g", cfg.GatewayURL)
	}
	if cfg.Concurrency > 0 {
		args = append(args, "-q", strconv.Itoa(cfg.Concurrency))
	}
	args = append(args, cfg.ExtraArgs...)

	cmd := exec.CommandContext(ctx, binary, args...)

	// Route combined stdout+stderr through logFn line-by-line.
	// A single writer serialises both streams so lines don't interleave.
	lw := &swissKnifeLineWriter{fn: logFn}
	cmd.Stdout = lw
	cmd.Stderr = lw

	if err := cmd.Run(); err != nil {
		lw.flush() // emit any partial last line
		return fmt.Errorf("exit: %w", err)
	}
	lw.flush()
	return nil
}

// swissKnifeLineWriter adapts a line-handler function to io.Writer,
// buffering bytes until a newline and calling fn for each complete line.
// It is not safe for concurrent use; the caller must not share it across
// goroutines (exec.Cmd assigns Stdout and Stderr to separate goroutines,
// but only one of them is active at a time in practice — and our use case
// assigns both Stdout and Stderr to the SAME instance so cmd routes them
// sequentially through the OS pipe).
type swissKnifeLineWriter struct {
	fn  func(string)
	buf []byte
}

func (lw *swissKnifeLineWriter) Write(p []byte) (int, error) {
	lw.buf = append(lw.buf, p...)
	for {
		i := bytes.IndexByte(lw.buf, '\n')
		if i < 0 {
			break
		}
		line := strings.TrimRight(string(lw.buf[:i]), "\r")
		lw.buf = lw.buf[i+1:]
		if line != "" {
			lw.fn(line)
		}
	}
	return len(p), nil
}

// flush emits any bytes remaining in the buffer that were not terminated
// by a newline (e.g. the last line of output if the process exited without
// a trailing newline).
func (lw *swissKnifeLineWriter) flush() {
	if len(lw.buf) > 0 {
		lw.fn(strings.TrimRight(string(lw.buf), "\r"))
		lw.buf = nil
	}
}
