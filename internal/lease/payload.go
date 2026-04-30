package lease

// payload.go — CVMFS object pack framing and gateway payload upload.
//
// The gateway's ObjectPackConsumer expects each CAS object to be preceded by
// a text TOC header (buildCvmfsPackHeader) and wrapped in an outer JSON
// message (submitOneObject).  SubmitPayload drives the sequence: chunks first,
// catalog last, so the gateway can verify referential integrity.
//
// IMPORTANT — CVMFS hash algorithm constraint:
//
// The C++ cvmfs_receiver uses shash::MkFromHexPtr / shash::MkFromSuffixedHexPtr
// to parse the hashes we send.  Those functions recognise only the algorithms
// in CVMFS's Algorithms enum:
//
//   kMd5    (16 bytes → 32 hex chars, no suffix)
//   kSha1   (20 bytes → 40 hex chars, no suffix)
//   kRmd160 (20 bytes → 47 hex chars with "-rmd160" suffix)
//   kShake128 (20 bytes → 49 hex chars with "-shake128" suffix)
//
// SHA-2 256-bit (64 hex chars) is NOT in the enum.  Using it causes
// MkFromHexPtr to return algorithm=kAny (default-constructed), which makes
// GetContextSize(kAny) hit its default: PANIC(...) branch → abort() → SIGABRT.
//
// Therefore ALL hashes embedded in the ObjectPack wire format MUST use SHA-1:
//   • payload_digest   — SHA-1 of the pack text header
//   • C <hashStr>      — SHA-1 of the compressed object bytes

import (
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec // CVMFS wire protocol requires SHA-1; see above
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// buildCvmfsPackHeader constructs the CVMFS object pack text header for a
// single CAS blob.  The format is defined by ObjectPackProducer in
// cvmfs/pack.cc:
//
//	V2\n
//	S<objSize>\n
//	N1\n
//	--\n
//	C <hashStr> <objSize>\n
//
// hashStr is the hex-encoded SHA-1 of the compressed object bytes being
// uploaded.  SHA-1 produces a 40-char hex string that the C++ receiver can
// parse via shash::MkFromSuffixedHexPtr (kSha1, no suffix required).
// objSize is the byte length of the compressed object payload that follows
// the header.
func buildCvmfsPackHeader(hashStr string, objSize int) []byte {
	return []byte(fmt.Sprintf("V2\nS%d\nN1\n--\nC %s %d\n",
		objSize, hashStr, objSize))
}

// submitOneObject sends a single compressed CAS object to the gateway using
// the correct CVMFS binary payload framing protocol.
//
// HTTP body layout (confirmed from gateway/frontend/{authz,payloads}.go and
// cvmfs/receiver/{reactor,payload_processor}.cc):
//
//	POST /api/v1/payloads
//	Content-Type: application/octet-stream
//	Message-Size: N          (N = byte length of the JSON message wrapper)
//	Authorization: <KEY_ID> <HMAC>
//
//	[N bytes: JSON message wrapper]
//	[header_size bytes: CVMFS pack text header]
//	[remaining bytes: compressed object data]
//
// JSON wrapper fields:
//   - session_token:  the lease token
//   - payload_digest: SHA-1 hex of the pack text header (NOT the object hash)
//   - header_size:    byte length of the pack text header (NOT the JSON size)
//   - api_version:    "2"
//
// HMAC for the deprecated POST /payloads endpoint is computed over the first
// Message-Size bytes of the body (the JSON wrapper bytes).
func (c *Client) submitOneObject(ctx context.Context, basePayloadURL, token, hash string, objBytes []byte) error {
	// Compute SHA-1 of the compressed object bytes.
	//
	// The C++ ObjectPackConsumer parses the hash from the "C <hash> <size>"
	// line using shash::MkFromSuffixedHexPtr.  That function only recognises
	// SHA-1 (40 hex, no suffix), RIPEMD-160 (47 hex), and SHAKE-128 (49 hex).
	// A SHA-256 hash (64 hex chars) hits the default: PANIC path in
	// GetContextSize, aborting the receiver.  Use SHA-1 here so that the
	// receiver can hash the bytes it receives (also SHA-1) and compare them to
	// event.id without crashing.
	objHashArr := sha1.Sum(objBytes) //nolint:gosec // required by CVMFS wire protocol
	objHashStr := hex.EncodeToString(objHashArr[:])

	// Propagate the CVMFS content-type suffix from the CAS key to the C line.
	//
	// CAS keys for catalog objects are stored as "sha1C" (41 chars) where the
	// trailing 'C' is the CVMFS catalog content-type suffix.  The receiver's
	// LocalUploader uses event.id.MakePath() to derive the storage path, so
	// the C line hash MUST include the 'C' suffix for catalogs — otherwise the
	// object lands at data/XY/sha1 instead of data/XY/sha1C and CommitProcessor
	// cannot find it when fetching the new root catalog from stratum0.
	//
	// The sha1.Any equality comparison ignores the suffix, so the receiver's
	// hash-verification step (file_hash != event.id) still passes regardless of
	// whether the suffix is present.
	//
	// For regular file objects the CAS key is plain sha1 (40 chars) and no
	// suffix is appended.
	const sha1HexLen = 40
	if len(hash) > sha1HexLen {
		objHashStr += hash[sha1HexLen:] // append content-type suffix, e.g. "C"
	}

	// Build the CVMFS object pack text header (the TOC that precedes the data).
	packHeader := buildCvmfsPackHeader(objHashStr, len(objBytes))

	// Compute SHA-1 of the pack text header itself.
	//
	// payload_digest is passed to ObjectPackConsumer(expected_digest, header_size).
	// The consumer calls shash::HashString(raw_header, &digest) where
	// digest.algorithm = expected_digest.algorithm.  A SHA-256 payload_digest
	// sets algorithm=kAny, causing GetContextSize(kAny) → PANIC → SIGABRT on
	// the very first payload.  SHA-1 (40 hex chars) is correctly parsed as
	// kSha1 and works with the receiver's hash infrastructure.
	packHeaderDigestArr := sha1.Sum(packHeader) //nolint:gosec // required by CVMFS wire protocol
	packHeaderDigest := hex.EncodeToString(packHeaderDigestArr[:])

	// Build the JSON message wrapper.
	msgJSON, err := json.Marshal(map[string]interface{}{
		"api_version":    "2",
		"session_token":  token,
		"payload_digest": packHeaderDigest,
		"header_size":    strconv.Itoa(len(packHeader)),
	})
	if err != nil {
		return fmt.Errorf("marshalling payload JSON: %w", err)
	}

	// HTTP body = [JSON (message-size bytes)][pack header text][compressed object bytes]
	body := make([]byte, 0, len(msgJSON)+len(packHeader)+len(objBytes))
	body = append(body, msgJSON...)
	body = append(body, packHeader...)
	body = append(body, objBytes...)

	req, err := http.NewRequestWithContext(ctx, "POST", basePayloadURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating payload request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Message-Size", strconv.Itoa(len(msgJSON)))
	req.ContentLength = int64(len(body))

	// POST /api/v1/payloads (deprecated): HMAC over the first Message-Size
	// bytes of the body (the JSON wrapper).
	sig := c.computeSignature(msgJSON)
	req.Header.Set("Authorization", fmt.Sprintf("%s %s", c.KeyID, sig))

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("POST payload: %w", err)
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }() //nolint:errcheck

	// Read and parse the JSON response body.
	//
	// The cvmfs_gateway payload endpoint ALWAYS returns HTTP 200 regardless of
	// whether the upload succeeded or failed.  The actual outcome is in the JSON
	// body as {"status":"ok"} or {"status":"error","reason":"uploader_error",...}.
	// Checking only resp.StatusCode (as before) caused uploader_error responses
	// to be silently treated as successes, leaving the CAS object unstored and
	// causing a downstream HTTP 404 when CommitProcessor tried to fetch the
	// catalog from Stratum 0.
	respData, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != 200 {
		return fmt.Errorf("payload upload failed (hash=%s): status %d: %s", hash, resp.StatusCode, respData)
	}
	if readErr != nil {
		return fmt.Errorf("reading payload response (hash=%s): %w", hash, readErr)
	}

	var result struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if jsonErr := json.Unmarshal(respData, &result); jsonErr != nil {
		// If body is not JSON, fall through — a non-200 status was already caught above.
		return fmt.Errorf("parsing payload response (hash=%s): %w — body: %s", hash, jsonErr, respData)
	}
	if result.Status != "ok" {
		return fmt.Errorf("payload upload rejected by gateway (hash=%s): status=%s reason=%s",
			hash, result.Status, result.Reason)
	}
	return nil
}

// SubmitPayload uploads all staged CAS objects (file chunks + catalog) to the
// gateway using the binary framing protocol required by cvmfs_gateway.
//
// Objects are uploaded in the order given (objectHashes first, then
// catalogHash last so the gateway sees the catalog only after all its
// referenced chunks are present).
func (c *Client) SubmitPayload(ctx context.Context, token, catalogHash string, objectHashes []string, store ObjectReader) error {
	ctx, span := c.obs.Tracer.Start(ctx, "lease.submit_payload")
	defer span.End()

	payloadURL := fmt.Sprintf("%s/api/v1/payloads", c.BaseURL)

	// Upload file-chunk objects first, catalog last.
	allHashes := append(append([]string(nil), objectHashes...), catalogHash)

	for _, hash := range allHashes {
		if hash == "" {
			continue
		}

		// Read compressed object bytes from the local CAS.
		rc, err := store.Get(ctx, hash)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("reading object %s from CAS: %w", hash, err)
		}
		objBytes, readErr := io.ReadAll(rc)
		rc.Close()
		if readErr != nil {
			span.RecordError(readErr)
			return fmt.Errorf("reading object %s bytes: %w", hash, readErr)
		}

		if err := c.submitOneObject(ctx, payloadURL, token, hash, objBytes); err != nil {
			span.RecordError(err)
			return fmt.Errorf("object %s: %w", hash, err)
		}
	}

	return nil
}
