// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package job defines the job data model, FSM states, and provenance tracking
// for CVMFS publish operations.
package job

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// tagNameRE is the allowlist for valid CVMFS snapshot tag names.
// Only ASCII alphanumerics plus dot, underscore, and hyphen are permitted.
// This matches the set of characters that CVMFS accepts in tag names and
// is more restrictive than the previous denylist ("space or slash") so that
// future CVMFS restrictions cannot silently corrupt a snapshot database.
var tagNameRE = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// ValidateTagName returns a non-nil error when name is not a valid CVMFS tag
// name.  An empty name is always valid — it simply means "publish without a
// named snapshot".  When non-empty the name must be ≤ 255 characters and may
// only contain ASCII letters, digits, dots (.), underscores (_), and hyphens (-).
func ValidateTagName(name string) error {
	if name == "" {
		return nil
	}
	if len(name) > 255 {
		return fmt.Errorf("tag name too long (%d chars, max 255)", len(name))
	}
	if !tagNameRE.MatchString(name) {
		return fmt.Errorf("tag name %q contains invalid characters (allowed: A-Z a-z 0-9 . _ -)", name)
	}
	return nil
}

// ValidCatalogHash reports whether h names a CVMFS catalog object: hex digits
// carrying the catalog content-type suffix.
//
// The suffix is not decoration. It is part of the CAS key, so a bare hash names
// a different object than the catalog; and the receiver refuses a graft whose
// hash lacks it outright — "DirectGraft requires a catalog hash",
// receiver/commit_processor.cc. Rejecting it at ingress names the field, where
// rejecting it at commit costs a lease and a promotion first and reports only
// that the graft failed.
//
// Exactly 40 lower-case hex digits plus the suffix. Not a length window: this
// stack computes CAS keys with SHA-1 and nothing else (cvmfshash.HashReader),
// so 40 is the only width it can produce or resolve. The wider algorithms the
// C++ receiver recognises are rendered "<hex>-rmd160" / "<hex>-shake128", which
// a hex-only rule would reject anyway — a window of 41..51 hex characters
// therefore admits nothing real while looking permissive.
func ValidCatalogHash(h string) bool {
	if len(h) != 41 || h[40] != CatalogHashSuffix {
		return false
	}
	for i := 0; i < 40; i++ {
		c := h[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ValidStagingPrefix reports whether p is usable as an S3 key prefix for a
// staged publish.
//
// The value is producer-supplied and becomes the base of every key a promotion
// lists and copies, so it is validated here rather than trusted. The failure it
// mainly guards is not traversal — the promotion validates each key it derives —
// but SILENCE: a prefix that is merely wrong lists nothing, copies nothing, and
// returns no error, leaving a graft to run against objects that were never
// promoted. That is the "202 for a request that does nothing" this handler
// exists to refuse.
//
// Rules: 1..128 bytes, slash-separated segments of [A-Za-z0-9._-], no empty
// segment, no "." or "..", no leading or trailing slash. A final "data" segment
// is refused specifically: the promotion appends "/data/" itself, so
// "<prefix>/data" is the likeliest producer mistake and its symptom is an empty
// listing rather than an error.
func ValidStagingPrefix(p string) bool {
	if p == "" || len(p) > 128 {
		return false
	}
	segs := strings.Split(p, "/")
	for i, s := range segs {
		if s == "" || s == "." || s == ".." {
			return false
		}
		if i == len(segs)-1 && s == "data" {
			return false
		}
		for j := 0; j < len(s); j++ {
			c := s[j]
			switch {
			case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			case c == '.' || c == '_' || c == '-':
			default:
				return false
			}
		}
	}
	return true
}

// CatalogHashSuffix is the CVMFS content-type suffix for catalog objects
// (shash::kSuffixCatalog).
const CatalogHashSuffix = 'C'

// State represents a job's position in the FSM lifecycle.
type State string

const (
	// StateIncoming is the initial state when a job is submitted.
	StateIncoming State = "incoming"
	// StateStaging is the compression/dedup pipeline stage.
	StateStaging State = "staging"
	// StateUploading is the CAS upload stage.
	StateUploading State = "uploading"
	// StateDistributing is the Stratum 1 replication stage.
	StateDistributing State = "distributing"
	// StateLeased is when the gateway lease is held.
	StateLeased State = "leased"
	// StateCommitting is the final gateway publish stage.
	StateCommitting State = "committing"
	// StateAccumulated is a terminal state for a coarse-publish package job
	// (ADR-0007): its objects are uploaded and its catalog entries recorded into
	// the build accumulator, awaiting the single end-of-build finalize commit.
	// The job itself does not commit to the gateway.
	StateAccumulated State = "accumulated"
	// StatePublished is the successful terminal state.
	StatePublished State = "published"
	// StateAborted is when the job was explicitly cancelled.
	StateAborted State = "aborted"
	// StateFailed is when the job encountered an error.
	StateFailed State = "failed"
)

// Provenance holds the build-system identity attached to a job at submission time,
// and the Rekor transparency log receipt produced after publish.
// All fields are optional so legacy manifests round-trip cleanly.
type Provenance struct {
	// Caller-supplied build identity (from HTTP headers at submission time).
	// These are accepted as-is when no OIDC token is provided (Verified=false).
	GitRepo     string `json:"git_repo,omitempty"`
	GitSHA      string `json:"git_sha,omitempty"`
	GitRef      string `json:"git_ref,omitempty"`
	Actor       string `json:"actor,omitempty"`
	PipelineID  string `json:"pipeline_id,omitempty"`
	BuildSystem string `json:"build_system,omitempty"`

	// OIDC-verified identity—populated only when the caller presented a valid CI OIDC token.
	// Verified=true signals that GitRepo, GitSHA, Actor, etc. above are cryptographically
	// attested by the CI provider and cannot be forged by the caller.
	OIDCIssuer  string `json:"oidc_issuer,omitempty"`
	OIDCSubject string `json:"oidc_subject,omitempty"`
	Verified    bool   `json:"verified,omitempty"`

	// Rekor transparency log receipt—populated after each successful publish.
	// RekorSET is verifiable offline using Rekor's public key.
	RekorServer         string `json:"rekor_server,omitempty"`
	RekorUUID           string `json:"rekor_uuid,omitempty"`
	RekorLogIndex       int64  `json:"rekor_log_index,omitempty"`
	RekorIntegratedTime int64  `json:"rekor_integrated_time,omitempty"`
	RekorSET            string `json:"rekor_set,omitempty"`
}

// Job represents a single CVMFS publish job, with persistent state that survives
// service restarts and crash recovery. Jobs transition through an FSM from
// incoming to a terminal state (published, failed, or aborted).
type Job struct {
	// ID is the unique job identifier (UUID).
	ID string
	// Repo is the repository name (e.g. "software.cern.ch").
	Repo string
	// Path is the sub-path within the repo for gateway lease scoping (e.g. "atlas/24.0").
	// Optional; empty path defaults to the repo name for lease scoping.
	Path string
	// PackageName is an optional human-readable package name.
	PackageName string
	// BuildID groups the package jobs of one build (ADR-0007 coarse publish).
	// When non-empty, the orchestrator records this job's catalog entries into
	// the build-scoped accumulator (internal/buildset) instead of committing
	// per-package; a single end-of-build finalize then publishes the whole set.
	// Empty preserves the legacy per-package commit behaviour.
	BuildID string `json:"build_id,omitempty"`
	// Finalize marks this job as the coarse-publish finalize for BuildID: instead
	// of pipelining a tar, the orchestrator publishes all of the build's
	// accumulated packages in one ingestsql commit. Carries no payload.
	Finalize bool `json:"finalize,omitempty"`

	// DirectS3 asks the ingest backend to pass --direct-s3, so cvmfs_server
	// uploads data objects straight to S3 and only catalogs traverse the
	// gateway. Per job on purpose: it is the knob the two publish paths are
	// compared with, and requiring a reconfigure to switch would mean the two
	// measurements were taken against different deployments.
	//
	// Absent/false does NOT pass --no-direct-s3: it leaves the decision to the
	// repository config, whose default is off.
	DirectS3 bool `json:"direct_s3,omitempty"`

	// ObjectList asks the ingest backend to collect the list of data objects
	// the publisher confirmed into S3, so it can later pre-warm Stratum 1
	// without re-deriving the set. Only the direct-S3 uploader produces it, so
	// it is meaningless without DirectS3: ingress rejects the combination
	// with a 400, and the backend drops it with a warning if it ever arrives.
	//
	// A separate knob from DirectS3 rather than implied by it: it changes what
	// the publisher reports, not how it publishes, so keeping it independent
	// lets its cost be measured on its own.
	ObjectList bool `json:"object_list,omitempty"`

	// StagingPrefix names an S3 key prefix, in the repository's own bucket,
	// that a producer has already filled with prepared CVMFS objects — chunked,
	// compressed and hashed by the canonical publisher running on the build
	// node. prepub promotes them into the CAS with a server-side copy instead of
	// receiving and re-processing a tar.
	//
	// Set together with CatalogHash; either alone is refused at ingress. The two
	// are what make the graft possible: the objects must be in the CAS before
	// the receiver can fetch the catalog that references them.
	StagingPrefix string `json:"staging_prefix,omitempty"`

	// CatalogHash is the subtree catalog the producer built, as a suffixed
	// CVMFS hash (…C). It becomes new_root_hash on the gateway's graft
	// endpoint, and the receiver downloads it from stratum0 by that hash — so
	// it must name an object the promotion has placed in the CAS.
	//
	// Suffixed, not bare: the receiver refuses a graft whose hash does not carry
	// the catalog suffix ("DirectGraft requires a catalog hash",
	// receiver/commit_processor.cc).
	CatalogHash string `json:"catalog_hash,omitempty"`

	// TarPath is the absolute path to the tar file in spool storage.
	TarPath string
	// TarName is the original base filename of the submitted tar (e.g.
	// "payload-abc123.tar").  Populated at submission time; used by the
	// console tooltip to identify which source file produced this job.
	TarName string `json:"tar_name,omitempty"`
	// TarSize is the size of the submitted tar in bytes.
	// Populated at submission time; used by the console tooltip.
	TarSize int64 `json:"tar_size,omitempty"`
	// TarSHA256 is the hex-encoded SHA-256 digest of the tar file, recorded at
	// submission time.  Non-empty when the caller provided a checksum (required
	// for tar_path / JSON submissions; optional for multipart uploads).
	TarSHA256 string
	// State is the current FSM state.
	State State
	// CreatedAt is the job creation time.
	CreatedAt time.Time
	// UpdatedAt is the last state transition time.
	UpdatedAt time.Time
	// LeaseToken is the gateway lease identifier while held; empty otherwise.
	LeaseToken string
	// NObjects is the number of objects in the published catalog (set after pipeline).
	// This includes both newly uploaded objects and dedup hits from prior jobs.
	NObjects int
	// NNewObjects is the number of objects that were freshly uploaded to CAS in
	// this pipeline run (dedup hits excluded).  Used by the S1 distribution
	// backlog display so the object count matches what is actually being pushed.
	NNewObjects int
	// NBytesRaw is the total uncompressed content bytes.
	NBytesRaw int64
	// NBytesCompressed is the total compressed content bytes (dedup-hits not counted).
	NBytesCompressed int64
	// Error is the failure reason; set on abort or failure.
	Error string `json:"error,omitempty"`
	// FailedAtState is the FSM state the job was in when it failed (e.g.
	// "leased", "committing").  Empty for non-failed jobs.  Used by the
	// console to highlight the correct pipeline step in the miniPipeline view.
	FailedAtState string `json:"failed_at_state,omitempty"`
	// RecoveryCount is the number of times this job has been reset for recovery.
	RecoveryCount int `json:"recovery_count,omitempty"`
	// InterruptCount is the number of times this job was reset because the
	// SERVICE was restarted cleanly under it, as opposed to failing.
	//
	// These are counted apart from RecoveryCount because they are not evidence
	// of anything wrong with the job. A `systemctl restart` during a large
	// publish interrupts every in-flight job, and counting that as a failed
	// attempt meant three routine restarts during one debugging session
	// terminally failed an entire 174-package build. Tracked anyway, so a
	// restart loop cannot re-run a job forever.
	InterruptCount int `json:"interrupt_count,omitempty"`
	// WebhookURL is an optional URL to POST when the job reaches a terminal state.
	WebhookURL string `json:"webhook_url,omitempty"`
	// TagName is the optional CVMFS snapshot tag to create for this publish.
	// When non-empty the gateway records a named tag in the repository's history
	// database, making this revision reachable by name (e.g. "v3.14.0").
	// Must satisfy ValidateTagName: ≤255 chars, no spaces or forward slashes.
	TagName string `json:"tag_name,omitempty"`
	// TagDescription is a human-readable comment stored alongside TagName.
	// Ignored when TagName is empty.
	TagDescription string `json:"tag_description,omitempty"`
	// NewRootHash is the plain-hex SHA-1 hash of the root catalog after a
	// successful publish (e.g. "a3f5...").  Populated only in StatePublished.
	// Pollers can compare this against the C= field of the Stratum 1
	// .cvmfspublished manifest to determine when S1 replication is complete.
	NewRootHash string `json:"new_root_hash,omitempty"`
	// Provenance contains build identity and Rekor transparency log receipt.
	Provenance *Provenance `json:"provenance,omitempty"`
	// PreloadExe is the repo-relative path to the application binary whose
	// startup was traced (e.g. "sw/ROOT/v6-24-06-4/bin/root").  When non-empty
	// the pipeline generates a .<base>.cvmfspreload file alongside the binary.
	PreloadExe string `json:"preload_exe,omitempty"`
	// PreloadPaths is the list of repo-relative paths opened during the traced
	// startup run.  Only paths present in the submitted tar produce CAS hashes.
	PreloadPaths []string `json:"preload_paths,omitempty"`

	// PublishPath selects how this package reaches the repository:
	//
	//	"" / "prepub" — compress + dedup + CAS, then a gateway commit. Supports
	//	                pre-warming and coarse (whole-build) publish.
	//	"ingest"      — hand the tar to `cvmfs_server ingest` and let the gateway
	//	                do the chunking, dedup and catalogs (ADR-0008 D7).
	//
	// The name must resolve to a backend the deployment actually has; a job
	// naming an unserviceable path is rejected at submission.
	PublishPath string `json:"publish_path,omitempty"`
	// PreWarm requests (or declines) Stratum 1 cache pre-warming for this job.
	// Nil means "use the node's default" (--prewarm), which is off. Only the
	// prepub publish path can pre-warm — the ingest path commits through the
	// gateway, so there is nothing to announce before the catalog flip.
	PreWarm *bool `json:"prewarm,omitempty"`

	// ── Per-stage timestamps (gateway/bits path only) ─────────────────────────
	// All times are zero-value when the stage was not reached or not applicable.
	// Callers can compute per-phase duration from successive timestamps.

	// PipelineStartedAt is when the compression/dedup/CAS pipeline started.
	PipelineStartedAt time.Time `json:"pipeline_started_at,omitempty"`
	// PipelineEndedAt is when the compression/dedup/CAS pipeline completed.
	PipelineEndedAt time.Time `json:"pipeline_ended_at,omitempty"`
	// DistributingStartedAt is when background S1 pre-warming was launched.
	// Distribution runs asynchronously — the job proceeds to StateLeased
	// without waiting for it to complete.
	DistributingStartedAt time.Time `json:"distributing_started_at,omitempty"`
	// DistributingEndedAt is when background S1 pre-warming finished.
	// May be after PublishedAt since distribution is fire-and-forget.
	DistributingEndedAt time.Time `json:"distributing_ended_at,omitempty"`
	// DistributionConfirmed is the number of S1 endpoints that confirmed all objects.
	DistributionConfirmed int `json:"distribution_confirmed,omitempty"`
	// DistributionTotal is the total number of S1 endpoints attempted.
	DistributionTotal int `json:"distribution_total,omitempty"`
	// LeasedAt is when the gateway lease was successfully acquired.
	LeasedAt time.Time `json:"leased_at,omitempty"`
	// CommittingAt is when the commit phase started (after catalog merge).
	CommittingAt time.Time `json:"committing_at,omitempty"`
	// PublishedAt is when the job reached StatePublished (S0 commit complete).
	PublishedAt time.Time `json:"published_at,omitempty"`
}

// NewJob creates a new job with incoming state and the current timestamp.
func NewJob(id, repo, packageName, tarPath string) *Job {
	now := time.Now()
	return &Job{
		ID:          id,
		Repo:        repo,
		PackageName: packageName,
		TarPath:     tarPath,
		State:       StateIncoming,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}
