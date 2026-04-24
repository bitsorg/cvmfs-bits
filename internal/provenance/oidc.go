package provenance

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// OIDCClaims holds the subset of JWT claims used for provenance attribution.
// The field names match the claim keys issued by GitHub Actions and GitLab CI.
type OIDCClaims struct {
	jwt.RegisteredClaims

	// ── GitHub Actions ────────────────────────────────────────────────────
	Repository  string `json:"repository,omitempty"`   // "owner/repo"
	Ref         string `json:"ref,omitempty"`          // "refs/heads/main"
	SHA         string `json:"sha,omitempty"`          // git commit SHA
	Actor       string `json:"actor,omitempty"`        // triggering username
	RunID       string `json:"run_id,omitempty"`       // workflow run ID
	Workflow    string `json:"workflow,omitempty"`     // workflow name or path
	Environment string `json:"environment,omitempty"` // deployment environment

	// ── GitLab CI ─────────────────────────────────────────────────────────
	ProjectPath   string `json:"project_path,omitempty"`
	NamespacePath string `json:"namespace_path,omitempty"`
	PipelineID    string `json:"pipeline_id,omitempty"`
	UserLogin     string `json:"user_login,omitempty"`
	CIConfigRef   string `json:"ci_config_ref_uri,omitempty"`
}

// ValidateOIDCToken validates a raw JWT string against the list of allowed
// issuers, fetches the issuer's JWKS to verify the signature, and returns the
// extracted and verified claims.
//
// The validation sequence:
//  1. Decode the JWT header (unverified) to extract kid and alg.
//  2. Decode the JWT payload (unverified) to extract the issuer.
//  3. Reject tokens whose issuer is not in allowedIssuers.
//  4. Fetch the issuer's OIDC discovery document → jwks_uri.
//  5. Fetch JWKS and locate the key matching kid.
//  6. Verify the JWT signature using the fetched key.
//  7. Verify standard claims: exp, nbf.
//  8. Return the verified OIDCClaims.
func ValidateOIDCToken(
	ctx context.Context,
	rawToken string,
	allowedIssuers []string,
	timeout time.Duration,
) (*OIDCClaims, error) {
	if len(allowedIssuers) == 0 {
		return nil, fmt.Errorf("OIDC validation is disabled: no allowed issuers configured")
	}

	// Pre-parse the issuer from the (unverified) payload so we can fetch the
	// correct JWKS before signature verification.
	unverifiedIssuer, err := extractIssuer(rawToken)
	if err != nil {
		return nil, fmt.Errorf("extracting issuer from JWT: %w", err)
	}

	// Reject unknown issuers before making any outbound HTTP calls.
	if !isAllowed(unverifiedIssuer, allowedIssuers) {
		return nil, fmt.Errorf("OIDC token issuer %q is not in the allowed list", unverifiedIssuer)
	}

	// Build a key function that fetches and caches the JWKS for this issuer.
	keyFunc := func(t *jwt.Token) (interface{}, error) {
		kid, _ := t.Header["kid"].(string)
		alg, _ := t.Header["alg"].(string)
		return fetchJWKSKey(ctx, unverifiedIssuer, kid, alg, timeout)
	}

	var claims OIDCClaims
	token, err := jwt.ParseWithClaims(rawToken, &claims, keyFunc,
		jwt.WithIssuedAt(),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, fmt.Errorf("JWT parse/verification failed: %w", err)
	}
	if !token.Valid {
		return nil, fmt.Errorf("JWT is not valid after parsing")
	}
	return &claims, nil
}

// extractIssuer decodes the JWT payload without verifying the signature and
// returns the "iss" claim value.
func extractIssuer(rawToken string) (string, error) {
	parts := strings.Split(rawToken, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("malformed JWT: expected 3 dot-separated parts, got %d", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("base64-decoding JWT payload: %w", err)
	}
	var partial struct {
		Issuer string `json:"iss"`
	}
	if err := json.Unmarshal(payload, &partial); err != nil {
		return "", fmt.Errorf("parsing JWT payload JSON: %w", err)
	}
	if partial.Issuer == "" {
		return "", fmt.Errorf("JWT payload has no 'iss' claim")
	}
	return partial.Issuer, nil
}

func isAllowed(issuer string, allowed []string) bool {
	for _, a := range allowed {
		if issuer == a {
			return true
		}
	}
	return false
}

// fetchJWKSKey fetches the OIDC discovery document for issuer, then fetches
// the JWKS, and returns the public key matching kid and alg as a type
// recognised by the jwt library (* rsa.PublicKey or *ecdsa.PublicKey).
func fetchJWKSKey(ctx context.Context, issuer, kid, alg string, timeout time.Duration) (interface{}, error) {
	jwksURI, err := fetchJWKSURI(ctx, issuer, timeout)
	if err != nil {
		return nil, err
	}
	return fetchKeyFromJWKS(ctx, jwksURI, kid, timeout)
}

// fetchJWKSURI retrieves the OIDC discovery document and extracts jwks_uri.
func fetchJWKSURI(ctx context.Context, issuer string, timeout time.Duration) (string, error) {
	discoveryURL := strings.TrimRight(issuer, "/") + "/.well-known/openid-configuration"
	body, err := httpGet(ctx, discoveryURL, timeout)
	if err != nil {
		return "", fmt.Errorf("fetching OIDC discovery document from %q: %w", discoveryURL, err)
	}
	var doc struct {
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return "", fmt.Errorf("parsing OIDC discovery document: %w", err)
	}
	if doc.JWKSURI == "" {
		return "", fmt.Errorf("OIDC discovery document has no jwks_uri field")
	}
	return doc.JWKSURI, nil
}

// jwkKey is the subset of fields from a JSON Web Key used here.
type jwkKey struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	// RSA fields
	N string `json:"n"`
	E string `json:"e"`
	// EC fields
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// fetchKeyFromJWKS fetches the JWKS document from jwksURI and converts the
// key matching kid into a *rsa.PublicKey or *ecdsa.PublicKey.
func fetchKeyFromJWKS(ctx context.Context, jwksURI, kid string, timeout time.Duration) (interface{}, error) {
	body, err := httpGet(ctx, jwksURI, timeout)
	if err != nil {
		return nil, fmt.Errorf("fetching JWKS from %q: %w", jwksURI, err)
	}

	var keySet struct {
		Keys []jwkKey `json:"keys"`
	}
	if err := json.Unmarshal(body, &keySet); err != nil {
		return nil, fmt.Errorf("parsing JWKS JSON: %w", err)
	}

	for _, k := range keySet.Keys {
		// Match by kid if provided; otherwise take the first usable key.
		if kid != "" && k.Kid != kid {
			continue
		}
		switch k.Kty {
		case "RSA":
			return jwkToRSA(k)
		case "EC":
			return jwkToEC(k)
		default:
			return nil, fmt.Errorf("unsupported JWK key type %q for kid %q", k.Kty, kid)
		}
	}
	return nil, fmt.Errorf("no key with kid %q found in JWKS at %q", kid, jwksURI)
}

// jwkToRSA converts a JWK with kty=RSA to an *rsa.PublicKey.
func jwkToRSA(k jwkKey) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, fmt.Errorf("decoding RSA modulus N: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, fmt.Errorf("decoding RSA exponent E: %w", err)
	}
	e := int(new(big.Int).SetBytes(eBytes).Int64())
	if e == 0 {
		return nil, fmt.Errorf("RSA exponent E decoded to zero")
	}
	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: e,
	}, nil
}

// jwkToEC converts a JWK with kty=EC to an *ecdsa.PublicKey.
func jwkToEC(k jwkKey) (*ecdsa.PublicKey, error) {
	var curve elliptic.Curve
	switch k.Crv {
	case "P-256":
		curve = elliptic.P256()
	case "P-384":
		curve = elliptic.P384()
	case "P-521":
		curve = elliptic.P521()
	default:
		return nil, fmt.Errorf("unsupported EC curve %q", k.Crv)
	}
	xBytes, err := base64.RawURLEncoding.DecodeString(k.X)
	if err != nil {
		return nil, fmt.Errorf("decoding EC X coordinate: %w", err)
	}
	yBytes, err := base64.RawURLEncoding.DecodeString(k.Y)
	if err != nil {
		return nil, fmt.Errorf("decoding EC Y coordinate: %w", err)
	}
	return &ecdsa.PublicKey{
		Curve: curve,
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}, nil
}

// httpGet performs a GET request and returns the response body, capped at 1 MiB.
func httpGet(ctx context.Context, url string, timeout time.Duration) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building request for %s: %w", url, err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("reading response from %s: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned HTTP %d: %s", url, resp.StatusCode, body)
	}
	return body, nil
}
