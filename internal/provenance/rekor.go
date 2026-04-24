package provenance

import (
	"bytes"
	"context"
	"crypto"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// rekorClient is a hardened HTTP client used exclusively for Rekor requests.
// It enforces TLS 1.2+ and does not share the process-global DefaultTransport,
// so other packages cannot accidentally downgrade its TLS configuration.
//
// The 30 s Timeout is a belt-and-suspenders guard: individual requests already
// use context.WithTimeout, but http.Client.Timeout additionally limits slow
// response-body reads (a server that drips bytes can otherwise stall the
// goroutine for much longer than the context deadline implies).
var rekorClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	},
}

// rekorEntryRequest is the wire format for POST /api/v1/log/entries.
type rekorEntryRequest struct {
	Kind       string         `json:"kind"`
	APIVersion string         `json:"apiVersion"`
	Spec       rekorEntrySpec `json:"spec"`
}

type rekorEntrySpec struct {
	Data      rekorData      `json:"data"`
	Signature rekorSignature `json:"signature"`
}

type rekorData struct {
	Hash rekorHash `json:"hash"`
}

type rekorHash struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"` // lowercase hex
}

type rekorSignature struct {
	Content   string         `json:"content"` // base64-encoded signature bytes
	Format    string         `json:"format"`  // always "x509"
	PublicKey rekorPublicKey `json:"publicKey"`
}

type rekorPublicKey struct {
	Content string `json:"content"` // base64-encoded PEM of the public key
}

// rekorResponseEntry is a single entry in the map returned by Rekor.
type rekorResponseEntry struct {
	Body           string               `json:"body"`
	IntegratedTime int64                `json:"integratedTime"`
	LogID          string               `json:"logID"`
	LogIndex       int64                `json:"logIndex"`
	Verification   rekorVerification    `json:"verification"`
}

type rekorVerification struct {
	// SignedEntryTimestamp is a base64-encoded bundle containing the tree hash,
	// log index, timestamp, and a signature over them from the Rekor server key.
	// It is verifiable offline using Rekor's public key.
	SignedEntryTimestamp string `json:"signedEntryTimestamp"`
}

// submitToRekor signs payloadJSON with signer, submits a hashedrekord entry to
// the Rekor server, and returns (uuid, logIndex, integratedTime, SET, error).
//
// The hashedrekord type records:
//   - the SHA-256 hash of payloadJSON (the artifact being attested)
//   - an Ed25519 signature over that hash
//   - the signer's DER-encoded SubjectPublicKeyInfo public key
//
// Rekor returns a UUID, a log index, an integrated timestamp, and a Signed
// Entry Timestamp (SET) — a compact bundle signed by the Rekor server key that
// proves the entry was included in the Merkle tree at a specific time.
func submitToRekor(
	ctx context.Context,
	serverURL string,
	payloadJSON []byte,
	signer crypto.Signer,
	pubKeyDER []byte,
	timeout time.Duration,
) (uuid string, logIndex int64, integratedTime int64, set string, err error) {

	// Hash the attestation payload.
	h := sha256.Sum256(payloadJSON)
	hashHex := fmt.Sprintf("%x", h[:])

	// Sign the hash with the node's Ed25519 key.
	// crypto.Hash(0) means "hash is already prehashed" (ed25519 signs raw bytes,
	// not a digest, so we pass the full hash as the message).
	sig, err := signer.Sign(nil, h[:], crypto.Hash(0))
	if err != nil {
		return "", 0, 0, "", fmt.Errorf("signing provenance payload: %w", err)
	}

	// Encode the public key: DER → PEM → base64.
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubKeyDER})

	reqBody := rekorEntryRequest{
		Kind:       "hashedrekord",
		APIVersion: "0.0.1",
		Spec: rekorEntrySpec{
			Data: rekorData{
				Hash: rekorHash{Algorithm: "sha256", Value: hashHex},
			},
			Signature: rekorSignature{
				Content: base64.StdEncoding.EncodeToString(sig),
				Format:  "x509",
				PublicKey: rekorPublicKey{
					Content: base64.StdEncoding.EncodeToString(pubPEM),
				},
			},
		},
	}

	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return "", 0, 0, "", fmt.Errorf("marshalling Rekor request: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(
		reqCtx, http.MethodPost,
		serverURL+"/api/v1/log/entries",
		bytes.NewReader(reqJSON),
	)
	if err != nil {
		return "", 0, 0, "", fmt.Errorf("building Rekor HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := rekorClient.Do(req)
	if err != nil {
		return "", 0, 0, "", fmt.Errorf("calling Rekor %s: %w", serverURL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1 MiB cap
	if err != nil {
		return "", 0, 0, "", fmt.Errorf("reading Rekor response body: %w", err)
	}

	if resp.StatusCode != http.StatusCreated {
		// Rekor may return 409 Conflict if the identical entry was already
		// submitted (idempotent — not an error for our purposes).
		if resp.StatusCode == http.StatusConflict {
			// Rekor sets a Location header pointing to the existing entry's URL,
			// e.g. "/api/v1/log/entries/<uuid>".  Extract the UUID from that path
			// so callers get a real, addressable identifier rather than "duplicate".
			location := resp.Header.Get("Location")
			return parseRekorConflict(respBody, location)
		}
		return "", 0, 0, "", fmt.Errorf("Rekor API %s returned HTTP %d: %s",
			serverURL, resp.StatusCode, respBody)
	}

	// Successful creation: response is a map keyed by entry UUID.
	var entries map[string]rekorResponseEntry
	if err := json.Unmarshal(respBody, &entries); err != nil {
		return "", 0, 0, "", fmt.Errorf("parsing Rekor response JSON: %w", err)
	}

	for entryUUID, e := range entries {
		return entryUUID, e.LogIndex, e.IntegratedTime, e.Verification.SignedEntryTimestamp, nil
	}
	return "", 0, 0, "", fmt.Errorf("Rekor response contained no entries")
}

// parseRekorConflict handles a 409 response, which Rekor returns when an
// identical entry already exists.  The Location header contains the URL of the
// existing entry (e.g. "/api/v1/log/entries/<uuid>"); the response body may
// contain the entry's details.
func parseRekorConflict(body []byte, location string) (uuid string, logIndex int64, integratedTime int64, set string, err error) {
	// Extract UUID from the Location header path component.
	if location != "" {
		if idx := strings.LastIndex(location, "/"); idx >= 0 && idx < len(location)-1 {
			uuid = location[idx+1:]
		}
	}

	// Rekor 409 body contains the existing entry as a plain object (not a map).
	var e rekorResponseEntry
	if jsonErr := json.Unmarshal(body, &e); jsonErr != nil {
		// We have the UUID from Location but could not parse the body.
		// Return what we have — the UUID alone lets callers build a receipt.
		if uuid == "" {
			return "", 0, 0, "", fmt.Errorf("duplicate Rekor entry (409) but could not parse response: %w", jsonErr)
		}
		return uuid, 0, 0, "", nil
	}

	if uuid == "" {
		uuid = "duplicate" // last-resort sentinel when Location header is absent
	}
	return uuid, e.LogIndex, e.IntegratedTime, e.Verification.SignedEntryTimestamp, nil
}

// SearchRekor queries the Rekor search API for entries matching a SHA-256
// artifact hash.  Returns a list of entry UUIDs.
//
// This is the lookup direction used to answer: "given a CAS object hash,
// which prepub job published it?"
func SearchRekor(ctx context.Context, serverURL, sha256Hex string, timeout time.Duration) ([]string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	q := url.Values{}
	q.Set("hash", "sha256:"+sha256Hex)
	searchURL := serverURL + "/api/v1/index/retrieve?" + q.Encode()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, searchURL, nil)
	if err != nil {
		return nil, fmt.Errorf("building Rekor search request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := rekorClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling Rekor search %s: %w", serverURL, err)
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return nil, fmt.Errorf("reading Rekor search response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Rekor search returned HTTP %d: %s", resp.StatusCode, body)
	}

	var uuids []string
	if err := json.Unmarshal(body, &uuids); err != nil {
		return nil, fmt.Errorf("parsing Rekor search response: %w", err)
	}
	return uuids, nil
}
