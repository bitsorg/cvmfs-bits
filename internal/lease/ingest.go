// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// IngestBackend implements Backend by handing the spooled tar straight to
// `cvmfs_server ingest`, letting the CVMFS gateway do the chunking, dedup,
// storage and catalog work that the prepub pipeline would otherwise do itself
// (ADR-0008 D7, "relay mode").
//
// It relies on MOUNTLESS ingest: the host registers once with
//
//	cvmfs_server connect-gw -P -K -u <gateway>/api/v1 -w <stratum0>/<repo> \
//	                        -o <owner> <repo>
//
// after which `cvmfs_server ingest` opens its own gateway lease, streams the
// tar through `cvmfs_swissknife ingest`, uploads the objects and closes the
// lease — no FUSE mount, no overlay, no privileged container. Where the
// publisher additionally has /etc/cvmfs/<repo>.s3.conf, data chunks go straight
// to S3 and only catalogs pass through the gateway.
//
// What this trades away, deliberately: no pre-warming (Stratum 1s see the
// content only after the commit), no local dedup, and one gateway transaction
// PER PACKAGE rather than one per build. It is the right choice when the
// priority is releasing the build node and minimising how much CVMFS format
// logic prepub owns; it is not the faster path.
//
// Concurrency: one ingest at a time per repository. Unlike LocalBackend, which
// fails fast when a repo is busy, Acquire BLOCKS — a relay publisher's whole
// purpose is to absorb a burst from the producer and feed the gateway steadily,
// so queuing is the correct behaviour and the caller's context bounds the wait.
// Different repositories proceed in parallel.
type IngestBackend struct {
	// cvmfsMount is the nominal repository root ("/cvmfs"). Nothing is mounted
	// there in mountless mode; the path is what `ingest -b` expects, and it is
	// how the repository name and sub-path are conveyed.
	cvmfsMount string
	// nestedCatalog requests a nested catalog at each extraction point
	// (`ingest -c`). One catalog per published package keeps the root catalog
	// small, and it is also what stops ingest from failing on a path that
	// already contains nested sub-catalogs.
	nestedCatalog bool
	// owner, when set, is passed as `ingest -u <owner>` so ingested files are
	// owned by the repository owner rather than by whoever the tar says.
	owner string
	// skipAncestorDirs disables the parent-directory materialisation in
	// ensureAncestors. The zero value materialises, because not doing it turns
	// a publish into a wasted upload plus a gateway panic.
	skipAncestorDirs bool
	obs              *observe.Provider

	mu    sync.Mutex
	repos map[string]*repoSlot // repo → 1-slot queue
	seq   atomic.Uint64        // makes each acquisition token unique
}

// repoSlot is a one-at-a-time queue for a repository.  The channel is the
// semaphore; holder identifies WHO holds it, so that a release from a job that
// has already let go cannot free the slot of the job that took it next.
type repoSlot struct {
	sem    chan struct{} // capacity 1
	holder string        // token of the current holder, "" when free
}

// IngestOptions configures an IngestBackend.
type IngestOptions struct {
	// CVMFSMount defaults to "/cvmfs".
	CVMFSMount string
	// NestedCatalog passes -c to cvmfs_server ingest. Default true.
	NestedCatalog bool
	// Owner passes -u to cvmfs_server ingest. Optional.
	Owner string
	// SkipAncestorDirs disables creating the parent directory chain of a
	// publish target that does not exist yet (see ensureAncestors). Off by
	// default — the zero value materialises — because the failure it prevents
	// is a full payload upload followed by a gateway panic. Set it only where
	// the prefixes are known to be created by something else.
	SkipAncestorDirs bool
}

// NewIngestBackend constructs an IngestBackend.
func NewIngestBackend(opt IngestOptions, obs *observe.Provider) *IngestBackend {
	mount := opt.CVMFSMount
	if mount == "" {
		mount = "/cvmfs"
	}
	return &IngestBackend{
		cvmfsMount:       mount,
		nestedCatalog:    opt.NestedCatalog,
		owner:            opt.Owner,
		skipAncestorDirs: opt.SkipAncestorDirs,
		obs:              obs,
		repos:            make(map[string]*repoSlot),
	}
}

// queueFor returns the slot for a repository, creating it on first use.
func (b *IngestBackend) queueFor(repo string) *repoSlot {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.repos[repo]
	if !ok {
		s = &repoSlot{sem: make(chan struct{}, 1)}
		b.repos[repo] = s
	}
	return s
}

// tokenSep separates the repository from the per-acquisition suffix in a token.
// Repository names are DNS-like (broker.ValidateRepo rejects "/", "+", "#" and
// NUL), so "#" cannot occur in the repository part.
const tokenSep = "#"

// repoOf extracts the repository from an acquisition token.
func repoOf(token string) string {
	if repo, _, ok := strings.Cut(token, tokenSep); ok {
		return repo
	}
	return token
}

// Acquire takes the repository's single ingest slot, waiting until it is free
// or the context is cancelled.
//
// There is no gateway lease at this point: `cvmfs_server ingest` opens and
// closes its own lease inside Commit. The slot exists so that this publisher
// never asks the gateway for two concurrent leases on one repository, which
// would simply be refused with path_busy.
//
// The token identifies THIS acquisition, not just the repository. Both Commit
// and Abort can run for one job (Commit fails, the orchestrator then aborts),
// so a release keyed only on the repository would free whichever job had taken
// the slot in between — putting two `cvmfs_server ingest` runs on one
// repository, which is precisely what the slot exists to prevent.
func (b *IngestBackend) Acquire(ctx context.Context, repo, _ string) (string, error) {
	s := b.queueFor(repo)
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return "", fmt.Errorf("ingest backend: waiting for repository %q: %w", repo, ctx.Err())
	}
	token := repo + tokenSep + strconv.FormatUint(b.seq.Add(1), 10)
	b.mu.Lock()
	s.holder = token
	b.mu.Unlock()
	b.obs.Logger.Info("ingest backend: slot acquired", "repo", repo)
	return token, nil
}

// release frees the slot IF this token still holds it. Safe to call more than
// once, and safe to call for a token that never held the slot.
func (b *IngestBackend) release(token string) {
	s := b.queueFor(repoOf(token))
	b.mu.Lock()
	if s.holder != token {
		b.mu.Unlock()
		return // already released, or the slot belongs to someone else now
	}
	s.holder = ""
	b.mu.Unlock()
	select {
	case <-s.sem:
	default:
	}
}

// Heartbeat is a no-op: no lease is held between Acquire and Commit, and the
// lease that `cvmfs_server ingest` opens internally is its own to renew.
func (b *IngestBackend) Heartbeat(_ context.Context, _ string, _ time.Duration, _ context.CancelFunc) func() {
	return func() {}
}

// Commit publishes req.TarPath at req.CVMFSDir by running
//
//	cvmfs_server ingest -t <tar> -b /cvmfs/<repo>/<path> [-c] [-u <owner>]
//
// The repository slot is released on both success and failure: unlike the
// transaction-based backends there is nothing left open to abort, because
// ingest closes its own lease before returning.
func (b *IngestBackend) Commit(ctx context.Context, req CommitRequest) error {
	defer b.release(req.Token)
	repo := repoOf(req.Token)

	if req.TarPath == "" {
		return fmt.Errorf("ingest backend: no tar payload for repository %q", repo)
	}
	if req.CVMFSDir == "" {
		return fmt.Errorf("ingest backend: no target path for repository %q", repo)
	}
	// Guard the invariant rather than silently publishing into the wrong
	// repository if CVMFSDir was built from something other than this lease.
	wantPrefix := path.Join(b.cvmfsMount, repo)
	if req.CVMFSDir != wantPrefix && !strings.HasPrefix(req.CVMFSDir, wantPrefix+"/") {
		return fmt.Errorf("ingest backend: target %q is not under %q", req.CVMFSDir, wantPrefix)
	}
	// Pass the base directory REPO-RELATIVE. cvmfs_server accepts an absolute
	// /cvmfs/<repo>/<path> too, but then warns on every publish and only uses
	// it to recover a repository name we are about to state explicitly.
	base := strings.TrimPrefix(strings.TrimPrefix(req.CVMFSDir, wantPrefix), "/")
	if base == "" {
		base = "/" // publish at the repository root
	}

	// Do this BEFORE the ingest, not after a failure: the payload is already
	// on disk and the gateway would otherwise accept the whole upload and only
	// then panic during the commit merge.
	if err := b.ensureAncestors(ctx, repo, req.CVMFSDir); err != nil {
		return err
	}

	// Argument order matters and is not forgiving: cvmfs_server's option loop
	// runs `while [ "$2" != "" ]` and then takes `$1` as the repository name,
	// so the repository MUST be the final argument. Put it anywhere else and
	// the last flag is consumed as the repository name instead — with no owner
	// the "-c" would silently become the repo and the publish would die in
	// load_repo_config.
	args := []string{"ingest", "-t", req.TarPath, "-b", base}
	if b.nestedCatalog {
		args = append(args, "-c")
	}
	if b.owner != "" {
		args = append(args, "-u", b.owner)
	}
	args = append(args, repo)

	b.obs.Logger.Info("ingest backend: publishing", "repo", repo, "base", base, "tar", req.TarPath)
	start := time.Now()
	out, err := b.cvmfsServerOutput(ctx, args...)
	if err != nil {
		return fmt.Errorf("cvmfs_server ingest into %q: %w (output: %s)",
			base, err, truncateLog(out))
	}
	b.obs.Logger.Info("ingest backend: published",
		"repo", repo, "base", base, "duration", time.Since(start).String())
	return nil
}

// ensureAncestors creates the parent directory chain of cvmfsDir when it does
// not exist yet.
//
// `cvmfs_server ingest -b <base>` creates <base> itself but NOT the directories
// above it, and the receiver requires them. WritableCatalogManager, in the
// cvmfs source deployed on the gateway:
//
//	GraftNestedCatalog  "The mountpoint directory must not yet exist. Its
//	                     parent directory, however must exist."
//	AddDirectory        PANIC when FindCatalog(parent_path) misses
//
// So publishing into a prefix whose ancestors are absent uploads the entire
// payload, waits out the commit, and only then dies on the GATEWAY with
//
//	PANIC: catalog_mgr_rw.cc : 1076  failed to graft nested catalog '<path>'  (with -c)
//	PANIC: catalog_mgr_rw.cc :  496  catalog for directory '<path>' cannot be found
//
// The cost is a full transfer per package, discarded. A 174-job ALICE O2
// publish did exactly this: every payload uploaded, every commit panicked, and
// the lease was cancelled — for eight minutes of transfer and nothing else.
//
// The chain is created in its own short transaction taken on the DEEPEST
// EXISTING ancestor rather than on the repository root, so materialising one
// leaf prefix does not lock every other path in the repository meanwhile.
func (b *IngestBackend) ensureAncestors(ctx context.Context, repo, cvmfsDir string) error {
	if b.skipAncestorDirs {
		return nil
	}
	root := path.Join(b.cvmfsMount, repo)
	parent := path.Dir(cvmfsDir)

	// Publishing directly under the repository root needs nothing: the root
	// always exists. The prefix test also rejects a parent that escaped the
	// repository, which Commit's own guard should already have caught.
	if parent == root || !strings.HasPrefix(parent, root+"/") {
		return nil
	}
	if isDir(parent) {
		return nil
	}
	// No repository mount: this is a MOUNTLESS publisher.
	//
	// `cvmfs_server connect-gw -P` registers a repository for gateway publishing
	// without mounting it — that is the whole point of the -P registration, and
	// the testbed's native publisher container runs that way with no /cvmfs at
	// all. There is then nothing to stat and nothing to mkdir into, because
	// creating a directory needs a writable union mount.
	//
	// So this is "cannot check", not "misconfigured". Erroring here would fail
	// EVERY publish on a mountless publisher, which is strictly worse than the
	// gateway panic this function exists to prevent — and it would fail the
	// common case (prefix already present) as loudly as the rare one.
	//
	// Warn rather than stay silent: on a mountless publisher, a first publish
	// into a brand-new prefix will still hit the receiver panic, and the log
	// line is what connects that panic back to here.
	if !isDir(root) {
		b.obs.Logger.Warn("ingest backend: no repository mount — cannot verify or create ancestors",
			"repo", repo, "mount", root, "parent", parent,
			"note", "mountless publisher (connect-gw -P); a first publish into a new "+
				"prefix may fail in the gateway with 'failed to graft nested catalog' "+
				"or 'catalog for directory ... cannot be found'")
		return nil
	}

	leaseTarget := repo
	if rel := strings.TrimPrefix(deepestExisting(root, parent), root); rel != "" {
		leaseTarget = repo + strings.TrimSuffix(rel, "/")
	}

	b.obs.Logger.Info("ingest backend: creating missing ancestor directories",
		"repo", repo, "parent", parent, "lease", leaseTarget)

	if out, err := b.cvmfsServerOutput(ctx, "transaction", leaseTarget); err != nil {
		return fmt.Errorf("ingest backend: cvmfs_server transaction %q (to create %q): "+
			"%w (output: %s)", leaseTarget, parent, err, truncateLog(out))
	}
	// From here the transaction is OURS, so aborting it is safe and correct.
	// That is the distinction Abort() draws: it refuses to abort because it can
	// never know whose transaction is open, whereas here we just opened it.
	if err := os.MkdirAll(parent, 0o755); err != nil {
		b.abortOwn(ctx, repo, "mkdir failed")
		return fmt.Errorf("ingest backend: mkdir %q inside transaction: %w", parent, err)
	}
	if out, err := b.cvmfsServerOutput(ctx, "publish", repo); err != nil {
		b.abortOwn(ctx, repo, "publish failed")
		return fmt.Errorf("ingest backend: cvmfs_server publish %q (creating %q): "+
			"%w (output: %s)", repo, parent, err, truncateLog(out))
	}
	b.obs.Logger.Info("ingest backend: ancestor directories created",
		"repo", repo, "parent", parent)
	return nil
}

// abortOwn rolls back a transaction this backend opened itself. Failure is
// logged, never returned: the caller is already failing for a better reason and
// an orphaned transaction is the gateway's lease to expire.
func (b *IngestBackend) abortOwn(ctx context.Context, repo, why string) {
	if out, err := b.cvmfsServerOutput(ctx, "abort", "-f", repo); err != nil {
		b.obs.Logger.Error("ingest backend: could not abort own transaction",
			"repo", repo, "reason", why, "error", err, "output", truncateLog(out))
	}
}

// deepestExisting walks up from dir towards root and returns the first
// directory that exists. root is assumed to exist and is the stopping point, so
// the result is always root or below.
func deepestExisting(root, dir string) string {
	for dir != root && strings.HasPrefix(dir, root+"/") {
		if isDir(dir) {
			return dir
		}
		dir = path.Dir(dir)
	}
	return root
}

func isDir(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

func truncateLog(s string) string {
	if len(s) > maxCvmfsLogBytes {
		return s[:maxCvmfsLogBytes] + " …[truncated]"
	}
	return s
}

// commitArgs exposes the argument vector for testing: the ordering constraint
// it encodes is enforced by a shell script in another project, so it deserves a
// test rather than a comment alone.
func (b *IngestBackend) commitArgs(repo, base, tarPath string) []string {
	args := []string{"ingest", "-t", tarPath, "-b", base}
	if b.nestedCatalog {
		args = append(args, "-c")
	}
	if b.owner != "" {
		args = append(args, "-u", b.owner)
	}
	return append(args, repo)
}

// Abort releases the repository slot. `cvmfs_server ingest` is atomic from this
// backend's point of view — it either committed or it did not — so there is
// nothing to roll back.
// It deliberately does NOT run `cvmfs_server abort`: this backend holds no
// transaction of its own, and the repository may well have one open — belonging
// to another job, or to the prepub path on a node that offers both. Aborting it
// would destroy someone else's in-flight publish. A transaction orphaned by a
// killed ingest is the gateway's to expire.
func (b *IngestBackend) Abort(_ context.Context, token string) error {
	b.release(token)
	return nil
}

// NeedsPipeline returns false: the gateway does the chunking, compression and
// catalog work, so the orchestrator hands over the raw spool tar untouched.
func (b *IngestBackend) NeedsPipeline() bool { return false }

// Probe verifies at startup that cvmfs_server is present. It deliberately does
// NOT verify gateway registration: `cvmfs_server connect-gw` state is
// per-repository and this backend may serve repositories that have not been
// published to yet, so a missing registration is reported by the first ingest
// with the tool's own error rather than by refusing to start.
func (b *IngestBackend) Probe(_ context.Context) error {
	p, err := exec.LookPath("cvmfs_server")
	if err != nil {
		return fmt.Errorf("ingest backend: cvmfs_server binary not found on PATH "+
			"(this publish path runs cvmfs_server ingest locally): %w", err)
	}
	b.obs.Logger.Info("ingest backend probe: cvmfs_server found",
		"path", p, "mount", b.cvmfsMount, "nested_catalog", b.nestedCatalog)
	return nil
}

// ── subprocess helpers ────────────────────────────────────────────────────────

func (b *IngestBackend) cvmfsServerOutput(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "cvmfs_server", args...)
	raw, err := cmd.CombinedOutput()
	out := strings.TrimSpace(string(raw))
	if out != "" {
		b.obs.Logger.Debug("cvmfs_server", "args", args, "output", truncateLog(out))
	}
	return out, err
}
