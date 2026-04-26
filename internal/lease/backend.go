package lease

import (
	"context"
	"errors"
	"time"
)

// ErrCommittedNotRemounted is returned by Backend.Commit when the underlying
// publish command durably committed the catalog to the repository backend but
// the subsequent read-only FUSE remount failed.  The repository content is
// correct and visible to Stratum 1s; only the local read-only mount is absent.
//
// Callers should log a warning and treat the job as successfully published
// rather than rolling back — the catalog cannot be un-committed.
//
// Recovery: run  mount <cvmfs_mount>/<repo>  on the Stratum-0 host.
var ErrCommittedNotRemounted = errors.New(
	"catalog committed but FUSE remount failed — restore with: mount <cvmfs_mount>/<repo>",
)

// CommitRequest carries the data needed to finalise a publish transaction.
//
// Gateway mode reads CatalogHash and ObjectHashes; the local backend reads
// TarPath and CVMFSDir.  Fields irrelevant to the active backend are silently
// ignored, so callers can populate the full struct without branching.
type CommitRequest struct {
	// Token is the opaque identifier returned by Acquire.
	Token string

	// ── Gateway mode ─────────────────────────────────────────────────────────

	// CatalogHash is the SHA-256 hash of the CVMFS catalog (gateway mode).
	CatalogHash string
	// ObjectHashes are the CAS object hashes to register with the gateway.
	ObjectHashes []string

	// ── Local mode ───────────────────────────────────────────────────────────

	// TarPath is the absolute path to the spool tar file to unpack (local mode).
	TarPath string
	// CVMFSDir is the absolute path inside the CVMFS transaction directory
	// where the tar contents should be extracted (local mode).
	// Typically: <cvmfsMount>/<repo>/<path>
	CVMFSDir string
}

// Backend abstracts CVMFS publish transaction management so the orchestrator
// can operate identically in gateway and single-host deployments.
//
// Two implementations are provided:
//   - *Client      — talks to the cvmfs_gateway HTTP API (gateway mode).
//   - *LocalBackend — runs cvmfs_server subprocesses on the local host.
type Backend interface {
	// Acquire opens an exclusive publish lock on repo/path and returns an
	// opaque token used in all subsequent calls for this transaction.
	Acquire(ctx context.Context, repo, path string) (token string, err error)

	// Heartbeat starts a goroutine that keeps the lock alive at the given
	// interval.  onExpire is called when the lock is detected to have lapsed
	// (e.g. after maxConsecutiveHeartbeatFailures renewal errors for the
	// gateway backend).  The returned cancel func stops the goroutine; it is
	// safe to call multiple times.  Local implementations return a no-op.
	Heartbeat(ctx context.Context, token string, interval time.Duration, onExpire context.CancelFunc) func()

	// Commit finalises the publish transaction described by req:
	//
	//   Gateway: sends req.CatalogHash + req.ObjectHashes to the gateway via
	//            SubmitPayload, then releases the lease with commit=true.
	//
	//   Local:   extracts req.TarPath into req.CVMFSDir, then runs
	//            cvmfs_server publish on the repository named by req.Token.
	//
	// If the underlying publish committed the catalog but the final FUSE
	// remount failed, Commit returns ErrCommittedNotRemounted.  Callers should
	// treat that as a successful publish with a manual recovery step, not as a
	// reason to abort.
	Commit(ctx context.Context, req CommitRequest) error

	// Abort rolls back the transaction and releases the lock without publishing.
	// Safe to call even when the lock is no longer held (idempotent).
	Abort(ctx context.Context, token string) error

	// NeedsPipeline reports whether the orchestrator must run the full
	// compress / dedup / CAS-upload pipeline before calling Commit.
	//
	//   Gateway: true  — the gateway expects pre-compressed CAS objects.
	//   Local:   false — Commit extracts the raw spool tar directly.
	NeedsPipeline() bool

	// Probe performs a quick sanity check at service startup.
	//
	//   Gateway: acquires and immediately releases a sentinel lease to confirm
	//            the gateway API is reachable and accepting requests.
	//   Local:   verifies that the cvmfs_server binary is on PATH.
	//
	// A failed Probe aborts service startup.
	Probe(ctx context.Context) error
}
