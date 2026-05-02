// Package job defines the job data model, FSM states, and provenance tracking
// for CVMFS publish operations.
package job

import (
	"fmt"
	"regexp"
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
	// TarPath is the absolute path to the tar file in spool storage.
	TarPath string
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
	NObjects int
	// NBytesRaw is the total uncompressed content bytes.
	NBytesRaw int64
	// NBytesCompressed is the total compressed content bytes (dedup-hits not counted).
	NBytesCompressed int64
	// Error is the failure reason; set on abort or failure.
	Error string `json:"error,omitempty"`
	// RecoveryCount is the number of times this job has been reset for recovery.
	RecoveryCount int `json:"recovery_count,omitempty"`
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
