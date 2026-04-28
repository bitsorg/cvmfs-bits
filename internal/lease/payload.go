package lease

// payload.go — CVMFS object pack framing and gateway payload upload.
//
// The gateway's ObjectPackConsumer expects each CAS object to be preceded by
// a text TOC header (buildCvmfsPackHeader) and wrapped in an outer JSON
// message (submitOneObject).  SubmitPayload drives the sequence: chunks first,
// catalog last, so the gateway can verify referential integrity.

import (
	"bytes"
	"context"
	"crypto/sha256"
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
// hashStr is the hex-encoded SHA-256 of the object bytes being uploaded.
// objSize is the byte length of the object payload that follows the header.
// The receiver (ObjectPackConsumer) reads exactly len(header) bytes as the
// pack TOC text, verifies its digest, then reads objSize bytes as the object.
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
//   - payload_digest: SHA-256 hex of the pack text header (NOT the object hash)
//   - header_size:    byte length of the pack text header (NOT the JSON size)
//   - api_version:    "2"
//
// HMAC for the deprecated POST /payloads endpoint is computed over the first
// Message-Size bytes of the body (the JSON wrapper bytes).
func (c *Client) submitOneObject(ctx context.Context, basePayloadURL, token, hash string, objBytes []byte) error {
	// Compute SHA-256 of the compressed object bytes.
	objHashArr := sha256.Sum256(objBytes)
	objHashStr := hex.EncodeToString(objHashArr[:])

	// Build the CVMFS object pack text header (the TOC that precedes the data).
	packHeader := buildCvmfsPackHeader(objHashStr, len(objBytes))

	// Compute SHA-256 of the pack text header itself.
	packHeaderDigestArr := sha256.Sum256(packHeader)
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

	if resp.StatusCode != 200 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("payload upload failed (hash=%s): status %d: %s", hash, resp.StatusCode, data)
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
