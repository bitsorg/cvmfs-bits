package lease

import (
	"archive/tar"
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// LocalBackend implements Backend by running cvmfs_server subprocesses.
// It is designed for single-host deployments where the cvmfs-prepub service
// and the CVMFS Stratum-0 run on the same machine without cvmfs_gateway.
//
// Concurrency: at most one active transaction per repository is allowed.
// A second Acquire on the same repo returns an error immediately (fail-fast),
// mirroring the gateway's lease-conflict behaviour.
type LocalBackend struct {
	// cvmfsMount is the filesystem root where CVMFS repositories are mounted.
	// Defaults to "/cvmfs" if empty at construction time.
	cvmfsMount string
	// obs provides structured logging.
	obs *observe.Provider
	// repoSems holds a per-repository semaphore that prevents concurrent
	// transactions.  Values are *repoSem.
	repoSems sync.Map
}

// NewLocalBackend constructs a LocalBackend.  cvmfsMount is the root path
// under which CVMFS repositories appear (typically "/cvmfs").
func NewLocalBackend(cvmfsMount string, obs *observe.Provider) *LocalBackend {
	if cvmfsMount == "" {
		cvmfsMount = "/cvmfs"
	}
	return &LocalBackend{cvmfsMount: cvmfsMount, obs: obs}
}

// repoSem is a single-slot semaphore that serialises transactions per repo.
// It uses atomic operations so Unlock is safe to call even when not locked
// (e.g. during crash recovery when the lock was never held in this process).
type repoSem struct {
	locked int32 // 0 = free, 1 = held
}

func (s *repoSem) tryLock() bool { return atomic.CompareAndSwapInt32(&s.locked, 0, 1) }
func (s *repoSem) unlock()       { atomic.StoreInt32(&s.locked, 0) }

// evict removes the semaphore for repo from the map after unlocking, so the
// map does not grow without bound across many short-lived repository operations.
// It uses CompareAndDelete rather than Delete to avoid a race where a new caller
// has already LoadOrStore'd a fresh entry for the same repo.
func (b *LocalBackend) evict(repo string, s *repoSem) {
	s.unlock()
	b.repoSems.CompareAndDelete(repo, s)
}

func (b *LocalBackend) semFor(repo string) *repoSem {
	// Fast path: the entry usually exists (we're inside a transaction).
	if v, ok := b.repoSems.Load(repo); ok {
		return v.(*repoSem)
	}
	// Slow path: first contact for this repo — race to store a new entry.
	v, _ := b.repoSems.LoadOrStore(repo, &repoSem{})
	return v.(*repoSem)
}

// Acquire opens a CVMFS transaction on the given repository.  It returns
// the repository name as the opaque token (sufficient for single-transaction-
// per-repo semantics).
//
// Returns an error immediately if the repo is already in a transaction so
// callers do not block — identical to the gateway's 409 Conflict response.
func (b *LocalBackend) Acquire(ctx context.Context, repo, _ string) (string, error) {
	sem := b.semFor(repo)
	if !sem.tryLock() {
		return "", fmt.Errorf("local backend: repository %q already in a transaction — only one active transaction per repo is allowed", repo)
	}

	if err := b.cvmfsServer(ctx, "transaction", repo); err != nil {
		sem.unlock() // release immediately on failure
		return "", fmt.Errorf("cvmfs_server transaction %q: %w", repo, err)
	}

	b.obs.Logger.Info("local backend: transaction opened", "repo", repo)
	return repo, nil // token == repo name
}

// Heartbeat is a no-op for the local backend: CVMFS transactions do not
// carry a server-side expiry, so no periodic renewal is needed.
func (b *LocalBackend) Heartbeat(_ context.Context, _ string, _ time.Duration, _ context.CancelFunc) func() {
	return func() {} // no-op cancel
}

// Commit extracts the tar at req.TarPath into req.CVMFSDir and then runs
// cvmfs_server publish on the repository identified by req.Token.
//
// State after return:
//   - nil error:                  publish succeeded; semaphore is released.
//   - ErrCommittedNotRemounted:   catalog committed but FUSE remount failed;
//     semaphore is released.  Caller should treat as published.
//   - any other error:            publish failed before or during cvmfs_server;
//     semaphore is NOT released so Abort can still abort the open transaction.
func (b *LocalBackend) Commit(ctx context.Context, req CommitRequest) error {
	repo := req.Token // for local backend, token == repo name

	// Ensure the target directory exists inside the CVMFS transaction.
	if err := os.MkdirAll(req.CVMFSDir, 0755); err != nil {
		return fmt.Errorf("creating CVMFS target dir %q: %w", req.CVMFSDir, err)
	}

	// Extract tar — this is the only file I/O before publish.
	b.obs.Logger.Info("local backend: extracting tar", "tar", req.TarPath, "dest", req.CVMFSDir)
	if err := extractTar(ctx, req.TarPath, req.CVMFSDir, b.obs); err != nil {
		return fmt.Errorf("extracting tar into CVMFS transaction: %w", err)
	}

	// Publish.  Capture combined output so we can detect the committed-but-
	// not-remounted case before reporting a failure.
	b.obs.Logger.Info("local backend: publishing", "repo", repo)
	out, pubErr := b.cvmfsServerOutput(ctx, "publish", repo)

	// logOut caps the output embedded in log fields and error strings so that
	// a verbose publish run does not produce huge log entries.  Marker detection
	// (strings.Contains below) always uses the full `out`.
	logOut := out
	if len(logOut) > maxCvmfsLogBytes {
		logOut = logOut[:maxCvmfsLogBytes] + " …[truncated]"
	}

	if pubErr != nil {
		if strings.Contains(out, "Exporting repository manifest") {
			// Phase 1 (catalog commit) succeeded; only the FUSE remount failed.
			b.obs.Logger.Warn("local backend: catalog committed but FUSE remount failed",
				"repo", repo, "output", logOut)
			b.evict(repo, b.semFor(repo)) // transaction is closed; release and evict
			return ErrCommittedNotRemounted
		}
		return fmt.Errorf("cvmfs_server publish %q: %w\noutput: %s", repo, pubErr, logOut)
	}

	b.obs.Logger.Info("local backend: publish succeeded", "repo", repo)
	b.evict(repo, b.semFor(repo))
	return nil
}

// Abort aborts the open CVMFS transaction without publishing and releases the
// per-repo semaphore.  Safe to call even when no transaction is active (the
// cvmfs_server abort -f failure is logged but not returned, matching the
// gateway's behaviour where stale-lease releases are best-effort).
func (b *LocalBackend) Abort(ctx context.Context, token string) error {
	repo := token
	sem := b.semFor(repo)
	defer b.evict(repo, sem) // always release and evict the semaphore entry

	if err := b.cvmfsServer(ctx, "abort", "-f", repo); err != nil {
		b.obs.Logger.Warn("local backend: cvmfs_server abort returned error (transaction may not have been open)",
			"repo", repo, "error", err)
		// Return the error so the orchestrator can log it, but the semaphore
		// is released regardless (via defer).
		return fmt.Errorf("cvmfs_server abort %q: %w", repo, err)
	}

	b.obs.Logger.Info("local backend: transaction aborted", "repo", repo)
	return nil
}

// NeedsPipeline returns false — the local backend unpacks the raw spool tar
// in Commit and does not consume compressed CAS objects.
func (b *LocalBackend) NeedsPipeline() bool { return false }

// Probe verifies that the cvmfs_server binary is reachable on PATH.
func (b *LocalBackend) Probe(ctx context.Context) error {
	path, err := exec.LookPath("cvmfs_server")
	if err != nil {
		return fmt.Errorf("cvmfs_server binary not found on PATH: %w", err)
	}
	b.obs.Logger.Info("local backend probe: cvmfs_server found", "path", path)
	return nil
}

// ── subprocess helpers ────────────────────────────────────────────────────────

// cvmfsServer runs  cvmfs_server <args...>  and returns a non-nil error if
// the command exits with a non-zero status.  Combined output is logged at
// Debug level.
func (b *LocalBackend) cvmfsServer(ctx context.Context, args ...string) error {
	_, err := b.cvmfsServerOutput(ctx, args...)
	return err
}

// maxCvmfsLogBytes is the maximum number of bytes written to the structured
// log for a single cvmfs_server invocation.  Callers that do programmatic
// marker detection (e.g. Commit checking for "Exporting repository manifest")
// always receive the FULL output string — only the log line is capped.
//
// We deliberately do NOT truncate the returned string: cvmfs_server publish
// may emit the commit-phase marker after several kilobytes of progress output,
// and truncating the return value would cause the orchestrator to misidentify
// a committed-but-not-remounted publish as a full failure and abort an already-
// committed catalog.  cvmfs_server is a trusted local binary whose output is
// bounded in practice; we accept the full buffer in memory.
const maxCvmfsLogBytes = 8192

// cvmfsServerOutput runs  cvmfs_server <args...>  and returns the full
// combined stdout+stderr output (trimmed) plus any error.  The output is
// always returned regardless of exit status so callers can inspect specific
// markers; only the structured-log entry is capped at maxCvmfsLogBytes.
func (b *LocalBackend) cvmfsServerOutput(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "cvmfs_server", args...)
	raw, err := cmd.CombinedOutput()
	out := strings.TrimSpace(string(raw))
	if out != "" {
		logOut := out
		if len(logOut) > maxCvmfsLogBytes {
			logOut = logOut[:maxCvmfsLogBytes] + " …[truncated]"
		}
		b.obs.Logger.Debug("cvmfs_server", "args", args, "output", logOut)
	}
	return out, err
}

// ── tar extraction ────────────────────────────────────────────────────────────

// extractTar extracts the tar archive at tarPath into destDir.
//
// Supported entry types: regular files (TypeReg), hard links materialised as
// independent copies (TypeLink), directories (TypeDir), and symbolic links
// (TypeSymlink).  All other types (devices, FIFOs, etc.) are skipped with a
// debug log — CVMFS does not support them.
//
// Security:
//   - Entry names that escape destDir via ".." sequences are rejected.
//   - Hard-link source paths (hdr.Linkname) that escape destDir are rejected.
//   - Absolute symlink targets are allowed: CVMFS repositories legitimately
//     contain symlinks into host paths (e.g. /lib64/ld-linux.so.2).  A
//     warning is logged so operators can audit if needed.
//
// Timestamps: ModTime and AccessTime from the tar header are applied to each
// regular file and hard-link copy via os.Chtimes.
//
// I/O: the tar file is read through a 1 MiB buffered reader to reduce syscall
// overhead when processing multi-gigabyte payloads.
func extractTar(ctx context.Context, tarPath, destDir string, obs *observe.Provider) error {
	// Normalise destDir so prefix checks work on any platform.
	destDir = filepath.Clean(destDir)
	prefix := destDir + string(os.PathSeparator)

	f, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("opening tar %q: %w", tarPath, err)
	}
	defer f.Close()

	tr := tar.NewReader(bufio.NewReaderSize(f, 1<<20)) // 1 MiB read buffer
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar header: %w", err)
		}

		// Resolve target path and reject traversal attempts.
		target := filepath.Join(destDir, filepath.FromSlash(hdr.Name))
		if target != destDir && !strings.HasPrefix(target, prefix) {
			return fmt.Errorf("tar entry %q escapes destination directory", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, hdr.FileInfo().Mode()); err != nil {
				return fmt.Errorf("mkdir %q: %w", target, err)
			}

		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return fmt.Errorf("mkdir parent for %q: %w", target, err)
			}
			if err := writeFile(target, hdr, tr); err != nil {
				return fmt.Errorf("writing %q: %w", hdr.Name, err)
			}

		case tar.TypeLink:
			// Hard-link entries carry no payload in the tar reader — reading from
			// tr returns EOF immediately.  Materialise as an independent copy of
			// the already-extracted source file; CVMFS deduplicates internally.
			src := filepath.Join(destDir, filepath.FromSlash(hdr.Linkname))
			if src != destDir && !strings.HasPrefix(src, prefix) {
				return fmt.Errorf("hard link %q source %q escapes destination directory",
					hdr.Name, hdr.Linkname)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return fmt.Errorf("mkdir parent for hard link %q: %w", target, err)
			}
			if err := copyFile(src, target, hdr); err != nil {
				return fmt.Errorf("copying hard link %q from %q: %w",
					hdr.Name, hdr.Linkname, err)
			}

		case tar.TypeSymlink:
			// Absolute targets are allowed: CVMFS repos legitimately contain
			// symlinks to host-path libraries (e.g. /lib64/ld-linux.so.2).
			// Log a warning so operators can audit payloads if needed.
			if filepath.IsAbs(hdr.Linkname) && obs != nil {
				obs.Logger.Warn("extractTar: absolute symlink target",
					"entry", hdr.Name, "target", hdr.Linkname)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return fmt.Errorf("mkdir parent for symlink %q: %w", target, err)
			}
			// Remove any pre-existing entry so os.Symlink does not fail.
			_ = os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return fmt.Errorf("symlink %q -> %q: %w", target, hdr.Linkname, err)
			}

		default:
			// Devices, FIFOs, etc. — skip silently.
		}
	}
	return nil
}

// writeFile creates or truncates the file at path, streams src into it, then
// applies mode and timestamps from hdr.
//
// The file is initially created with mode 0600 so the content is never
// readable by other users during the write.  Mode and timestamps are applied
// after the file descriptor is closed.
func writeFile(path string, hdr *tar.Header, src io.Reader) error {
	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, src); err != nil {
		out.Close()
		os.Remove(path)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(path)
		return err
	}
	if err := os.Chmod(path, hdr.FileInfo().Mode()); err != nil {
		os.Remove(path)
		return err
	}
	// Preserve the original modification and access times so that CVMFS
	// catalog hashes are reproducible across re-publishes of the same content.
	if err := os.Chtimes(path, hdr.AccessTime, hdr.ModTime); err != nil {
		os.Remove(path)
		return err
	}
	return nil
}

// copyFile copies the regular file at src to dst using writeFile, applying
// mode and timestamps from hdr.  Used to materialise hard-link tar entries
// as independent file copies.
func copyFile(src, dst string, hdr *tar.Header) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("opening hard link source %q: %w", src, err)
	}
	defer in.Close()
	return writeFile(dst, hdr, in)
}
