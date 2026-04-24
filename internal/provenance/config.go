package provenance

import "time"

const (
	// DefaultRekorServer is the public Sigstore transparency log.
	// Override with Config.RekorServer for self-hosted deployments.
	DefaultRekorServer = "https://rekor.sigstore.dev"

	defaultHTTPTimeout = 20 * time.Second
)

// Config controls provenance recording and Rekor transparency log submission.
//
// When Enabled is false (the default), all Provider methods are no-ops and
// the pipeline behaves identically to a build without provenance tracking.
//
// When Enabled is true, the service:
//   - generates or loads an Ed25519 signing key at startup;
//   - extracts caller-supplied provenance metadata from HTTP request headers;
//   - optionally validates CI OIDC tokens and marks those claims Verified=true;
//   - after each successful publish, signs a ProvenanceRecord and submits it to
//     the Rekor transparency log, storing the returned UUID and Signed Entry
//     Timestamp (SET) in the job manifest.
type Config struct {
	// Enabled activates provenance recording and Rekor submission. Off by default.
	Enabled bool

	// RekorServer is the URL of the Rekor transparency log server.
	// Defaults to DefaultRekorServer (https://rekor.sigstore.dev).
	// Set to an internal URL for self-hosted Rekor deployments.
	RekorServer string

	// SigningKeyPath is the path to an Ed25519 private key in PEM (PKCS#8)
	// format used to sign each ProvenanceRecord before Rekor submission.
	// If empty, a key is auto-generated at startup and written to
	// {SpoolDir}/provenance.key (mode 0600).  Keep this file backed up —
	// losing it means future Rekor entries cannot be attributed to the same node.
	SigningKeyPath string

	// OIDCIssuers is the list of OIDC issuer URLs accepted for CI token
	// validation.  When empty, OIDC validation is disabled and provenance
	// fields are accepted as caller-supplied (Verified=false).
	//
	// Common values:
	//   "https://token.actions.githubusercontent.com"  — GitHub Actions
	//   "https://gitlab.com"                           — GitLab CI (SaaS)
	//   "https://gitlab.example.com"                   — self-hosted GitLab
	OIDCIssuers []string

	// HTTPTimeout caps each outbound HTTP call to Rekor or the OIDC discovery
	// and JWKS endpoints.  Defaults to 20 s.
	HTTPTimeout time.Duration
}

func (c Config) rekorServer() string {
	if c.RekorServer != "" {
		return c.RekorServer
	}
	return DefaultRekorServer
}

func (c Config) httpTimeout() time.Duration {
	if c.HTTPTimeout > 0 {
		return c.HTTPTimeout
	}
	return defaultHTTPTimeout
}

func (c Config) oidcEnabled() bool {
	return len(c.OIDCIssuers) > 0
}
