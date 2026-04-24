package provenance

import "time"

// Record is the attestation written to the Rekor transparency log after each
// successful publish.  It binds a prepub job (and its CAS outputs) to the
// build identity that triggered it — either caller-supplied or cryptographically
// verified via an OIDC token from the CI system.
//
// The record is serialised to JSON, SHA-256-hashed, Ed25519-signed by the
// prepub node's key, and submitted to Rekor as a hashedrekord entry.  Rekor
// returns a UUID and Signed Entry Timestamp (SET) that are stored in the job
// manifest and can be used later to prove inclusion in the log.
type Record struct {
	// ── Job identity ────────────────────────────────────────────────────────
	JobID       string    `json:"job_id"`
	Repo        string    `json:"repo"`
	Path        string    `json:"path,omitempty"`
	PublishedAt time.Time `json:"published_at"`

	// ── CAS outputs ─────────────────────────────────────────────────────────
	// CatalogHash is the SHA-256 content hash of the compressed SQLite catalog
	// committed to the gateway.  It uniquely identifies the published revision.
	CatalogHash string `json:"catalog_hash"`

	// ObjectHashes are the SHA-256 content hashes of every CAS object uploaded
	// during this job.  Verifiers can use these to walk from a file path in the
	// CVMFS catalog back to this Rekor entry via the hash.
	ObjectHashes []string `json:"object_hashes,omitempty"`

	// ── Build identity ──────────────────────────────────────────────────────
	// These fields are populated either from HTTP request headers (unverified,
	// Verified=false) or from a validated CI OIDC token (Verified=true).
	GitRepo     string `json:"git_repo,omitempty"`
	GitSHA      string `json:"git_sha,omitempty"`
	GitRef      string `json:"git_ref,omitempty"`
	Actor       string `json:"actor,omitempty"`
	PipelineID  string `json:"pipeline_id,omitempty"`
	BuildSystem string `json:"build_system,omitempty"`

	// OIDCIssuer and OIDCSubject are set when a CI OIDC token was successfully
	// validated at job-submission time.  Verified=true signals that the build
	// identity fields above are cryptographically attested by the CI provider.
	OIDCIssuer  string `json:"oidc_issuer,omitempty"`
	OIDCSubject string `json:"oidc_subject,omitempty"`

	// Verified is true iff the build identity was derived from a validated OIDC
	// token rather than from caller-supplied HTTP headers.
	Verified bool `json:"verified"`

	// ── Rekor response ──────────────────────────────────────────────────────
	// These are populated after a successful Rekor submission.
	RekorServer   string `json:"rekor_server,omitempty"`
	RekorUUID     string `json:"rekor_uuid,omitempty"`
	RekorLogIndex int64  `json:"rekor_log_index,omitempty"`

	// RekorSET is the base64-encoded Signed Entry Timestamp returned by Rekor.
	// It is a self-contained receipt: verifiable offline (without contacting
	// Rekor again) using Rekor's public key, proving the record existed in the
	// log at RekorIntegratedTime.
	RekorSET            string `json:"rekor_set,omitempty"`
	RekorIntegratedTime int64  `json:"rekor_integrated_time,omitempty"`
}

// Submitted reports whether this record was successfully submitted to Rekor.
func (r *Record) Submitted() bool { return r.RekorUUID != "" }
