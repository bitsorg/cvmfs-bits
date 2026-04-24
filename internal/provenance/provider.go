// Package provenance implements irrefutable provenance recording for
// cvmfs-prepub publish jobs.
//
// After each successful publish the Provider:
//  1. Builds a ProvenanceRecord from the job identity, CAS outputs, and
//     build-system identity (caller-supplied headers or OIDC-verified claims).
//  2. Signs the record JSON with the node's Ed25519 key.
//  3. Submits it to a Rekor transparency log as a hashedrekord entry.
//  4. Stores the returned UUID and Signed Entry Timestamp (SET) in the job
//     manifest, where they serve as the permanent, offline-verifiable receipt.
//
// When Config.Enabled is false all methods are no-ops, preserving backward
// compatibility with deployments that do not require provenance tracking.
package provenance

import (
	"context"
	"crypto"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// Header names used to pass caller-supplied provenance metadata.
// CI systems set these alongside the API auth header.
const (
	HeaderGitRepo     = "X-Provenance-Git-Repo"
	HeaderGitSHA      = "X-Provenance-Git-SHA"
	HeaderGitRef      = "X-Provenance-Git-Ref"
	HeaderActor       = "X-Provenance-Actor"
	HeaderPipelineID  = "X-Provenance-Pipeline-ID"
	HeaderBuildSystem = "X-Provenance-Build-System"
	// HeaderOIDCToken carries the CI OIDC token for cryptographic verification.
	// It is distinct from the API auth token in the Authorization header.
	HeaderOIDCToken = "X-OIDC-Token"
)

// Provider manages signing key loading and provenance submission.
// The zero value is valid but behaves as a no-op (Enabled=false).
type Provider struct {
	cfg       Config
	signer    crypto.Signer
	pubKeyDER []byte
	obs       *observe.Provider
}

// New creates a Provider from cfg.  When cfg.Enabled is true, the signing key
// is loaded or generated from cfg.SigningKeyPath (falling back to
// {spoolDir}/provenance.key).
func New(cfg Config, spoolDir string, obs *observe.Provider) (*Provider, error) {
	p := &Provider{cfg: cfg, obs: obs}
	if !cfg.Enabled {
		return p, nil
	}

	keyPath := cfg.SigningKeyPath
	if keyPath == "" {
		keyPath = filepath.Join(spoolDir, "provenance.key")
	}

	signer, pubDER, err := loadOrGenerateKey(keyPath)
	if err != nil {
		return nil, fmt.Errorf("provenance: loading signing key: %w", err)
	}
	p.signer = signer
	p.pubKeyDER = pubDER

	obs.Logger.Info("provenance: signing key ready",
		"path", keyPath,
		"rekor", cfg.rekorServer(),
		"oidc_issuers", cfg.OIDCIssuers,
	)
	return p, nil
}

// ExtractFromRequest builds a partial Record from the HTTP request's provenance
// headers.  If an OIDC token is present in X-OIDC-Token and OIDC validation is
// configured, the token is validated and its claims populate the record fields,
// overriding any caller-supplied header values.  In that case Verified=true.
//
// Returns nil when the provider is disabled or when no provenance headers are
// present (no-op path).
func (p *Provider) ExtractFromRequest(r *http.Request) *Record {
	if !p.cfg.Enabled {
		return nil
	}

	rec := &Record{
		GitRepo:     r.Header.Get(HeaderGitRepo),
		GitSHA:      r.Header.Get(HeaderGitSHA),
		GitRef:      r.Header.Get(HeaderGitRef),
		Actor:       r.Header.Get(HeaderActor),
		PipelineID:  r.Header.Get(HeaderPipelineID),
		BuildSystem: r.Header.Get(HeaderBuildSystem),
	}

	// Attempt OIDC token validation if a token is present.
	rawToken := r.Header.Get(HeaderOIDCToken)
	if rawToken == "" {
		// Also accept Bearer tokens that look like JWTs (three dot-separated
		// parts) in case the CI system uses the main Authorization header for
		// its OIDC token rather than the dedicated header.
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			candidate := strings.TrimPrefix(auth, "Bearer ")
			if strings.Count(candidate, ".") == 2 {
				rawToken = candidate
			}
		}
	}

	if rawToken != "" && p.cfg.oidcEnabled() {
		claims, err := ValidateOIDCToken(
			r.Context(), rawToken, p.cfg.OIDCIssuers, p.cfg.httpTimeout(),
		)
		if err != nil {
			p.obs.Logger.Warn("provenance: OIDC token validation failed — using caller-supplied headers",
				"error", err)
		} else {
			// Verified OIDC claims take precedence over caller-supplied headers.
			rec.Verified = true
			if iss, err := claims.GetIssuer(); err == nil {
				rec.OIDCIssuer = iss
			}
			if sub, err := claims.GetSubject(); err == nil {
				rec.OIDCSubject = sub
			}
			// GitHub Actions claims.
			if claims.Repository != "" {
				rec.GitRepo = claims.Repository
			}
			if claims.SHA != "" {
				rec.GitSHA = claims.SHA
			}
			if claims.Ref != "" {
				rec.GitRef = claims.Ref
			}
			if claims.Actor != "" {
				rec.Actor = claims.Actor
			}
			if claims.RunID != "" {
				rec.PipelineID = claims.RunID
			}
			if rec.BuildSystem == "" && claims.Workflow != "" {
				rec.BuildSystem = "github-actions"
			}
			// GitLab CI claims (override GitHub ones only if set).
			if claims.ProjectPath != "" && rec.GitRepo == "" {
				rec.GitRepo = claims.ProjectPath
			}
			if claims.PipelineID != "" && rec.PipelineID == "" {
				rec.PipelineID = claims.PipelineID
			}
			if claims.UserLogin != "" && rec.Actor == "" {
				rec.Actor = claims.UserLogin
			}
			if rec.BuildSystem == "" && claims.CIConfigRef != "" {
				rec.BuildSystem = "gitlab-ci"
			}
		}
	}

	return rec
}

// Submit serialises the Record, signs it, and submits it to Rekor.  The Record
// is updated in-place with the Rekor UUID, log index, integrated time, and SET.
//
// Submit is non-fatal: a failure is logged at Warn and does not cause the job
// to fail.  The caller should still write the manifest after Submit returns so
// that partial Rekor fields (if any) are persisted.
//
// The jobID, repo, path, catalogHash, and objectHashes must be set by the
// caller before calling Submit.
func (p *Provider) Submit(ctx context.Context, rec *Record) error {
	if !p.cfg.Enabled || rec == nil {
		return nil
	}

	rec.RekorServer = p.cfg.rekorServer()
	rec.PublishedAt = time.Now().UTC()

	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("provenance: marshalling record: %w", err)
	}

	uuid, logIndex, integratedTime, set, err := submitToRekor(
		ctx,
		p.cfg.rekorServer(),
		payload,
		p.signer,
		p.pubKeyDER,
		p.cfg.httpTimeout(),
	)
	if err != nil {
		return fmt.Errorf("provenance: Rekor submission: %w", err)
	}

	rec.RekorUUID = uuid
	rec.RekorLogIndex = logIndex
	rec.RekorIntegratedTime = integratedTime
	rec.RekorSET = set

	p.obs.Logger.Info("provenance: Rekor entry created",
		"uuid", uuid,
		"log_index", logIndex,
		"rekor_server", p.cfg.rekorServer(),
		"verified", rec.Verified,
	)
	return nil
}
