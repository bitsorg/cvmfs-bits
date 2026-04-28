package lease

// auth.go — HMAC-SHA1 signing helpers for cvmfs_gateway requests.
//
// The gateway Erlang frontend (gateway/frontend/authz.go) expects all API
// requests to carry an Authorization header of the form:
//
//	<key_id> <base64(hex(HMAC-SHA1(input, secret)))>
//
// Three helper methods cover the two distinct signing strategies in use:
//
//	• computeSignature — raw bytes → token string
//	• signWithToken    — POST/DELETE /api/v1/leases/<token>: HMAC over token
//	• signRequest      — POST /api/v1/leases (new lease): HMAC over body

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
)

// computeSignature returns base64(hex(HMAC-SHA1(input, secret))), the
// Authorization token value expected by cvmfs_gateway for all endpoints.
func (c *Client) computeSignature(input []byte) string {
	h := hmac.New(sha1.New, []byte(c.Secret))
	h.Write(input)
	return base64.StdEncoding.EncodeToString([]byte(hex.EncodeToString(h.Sum(nil))))
}

// signWithToken signs the request using the session token as the HMAC input.
//
// Per gateway/frontend/authz.go, all lease and payload requests that include
// a token in the URL path use the token as the HMAC input:
//
//	DELETE /api/v1/leases/<token>      → HMAC over token  (cancel/abort)
//	POST   /api/v1/leases/<token>      → HMAC over token  (commit)
func (c *Client) signWithToken(req *http.Request, token string) {
	sig := c.computeSignature([]byte(token))
	req.Header.Set("Authorization", fmt.Sprintf("%s %s", c.KeyID, sig))
}

// signRequest signs the request using the raw request body as the HMAC input.
// Used for POST /api/v1/leases (new lease acquisition) where the body is
// the JSON payload.  Reads and rebuffers req.Body so it remains readable.
func (c *Client) signRequest(req *http.Request) error {
	var bodyBytes []byte
	if req.Body != nil && req.Body != http.NoBody {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return fmt.Errorf("reading request body for signing: %w", err)
		}
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		req.ContentLength = int64(len(bodyBytes))
	}
	sig := c.computeSignature(bodyBytes)
	req.Header.Set("Authorization", fmt.Sprintf("%s %s", c.KeyID, sig))
	return nil
}
