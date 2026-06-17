// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package manifest defines the transaction manifest exchanged between a
// cvmfs-prepub publisher (Stratum 0) and receivers (Stratum 1) under the
// pull-based distribution model (ADR-0001).
//
// A manifest is the authoritative, deduplicated set of CAS objects a
// transaction adds, plus the metadata a receiver needs to fetch and verify
// them. Small (incremental) manifests serialise as a single JSON document;
// large (cold-start / catch-up) manifests serialise as NDJSON — a header line
// followed by one object record per line — so neither side must buffer the
// whole set in memory.
package manifest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// Generator records how a manifest's object set was derived.
type Generator string

const (
	// GeneratorPipeline: the set came from the publish pipeline's dedup step
	// (the incremental, authoritative new-object set — ADR D3).
	GeneratorPipeline Generator = "pipeline"
	// GeneratorDiff: the set was computed from a catalog diff
	// (cvmfs_server diff / catalog walk) for cold-start or catch-up (ADR D4).
	GeneratorDiff Generator = "diff"
)

// Auth is the object-channel authorization policy for a transaction (ADR D8).
type Auth string

const (
	// AuthPublic serves objects cacheably, like stock CVMFS (default).
	AuthPublic Auth = "public"
	// AuthToken gates objects until commit; not shared-cacheable.
	AuthToken Auth = "token"
)

// ObjRef identifies one CAS object to transfer.
type ObjRef struct {
	Hash   string `json:"hash"`             // content hash (lower-hex)
	Size   int64  `json:"size"`             // compressed size in bytes
	Suffix string `json:"suffix,omitempty"` // CVMFS object suffix ("" data, "C" chunked, ...)
}

// Manifest is the unit of work for one distribution transaction.
//
// For NDJSON serialisation the Objects field is carried out-of-band (one record
// per line) and is nil in the header; see EncodeNDJSON / DecodeNDJSON.
type Manifest struct {
	TransactionID  string    `json:"transaction_id"`
	Repo           string    `json:"repo"`
	BaseRootHash   string    `json:"base_root_hash"`   // root this txn advances FROM
	TargetRootHash string    `json:"target_root_hash"` // root it advances the catalog TO
	BaseURLs       []string  `json:"base_urls"`        // S0 object base URL(s)
	Generator      Generator `json:"generator"`
	Auth           Auth      `json:"auth"`
	CreatedAt      time.Time `json:"created_at"`
	TotalSize      int64     `json:"total_size"`
	Objects        []ObjRef  `json:"objects,omitempty"`
	// Provisional marks a pre-warm manifest whose TargetRootHash is a placeholder
	// (the real catalog root is unknown until commit). Receivers pull its objects
	// but MUST NOT record TargetRootHash as the last-synced root.
	Provisional    bool      `json:"provisional,omitempty"`
}

// Validate checks the required fields are present and internally consistent.
func (m *Manifest) Validate() error {
	switch {
	case m.TransactionID == "":
		return fmt.Errorf("manifest: missing transaction_id")
	case m.Repo == "":
		return fmt.Errorf("manifest: missing repo")
	case m.TargetRootHash == "":
		return fmt.Errorf("manifest: missing target_root_hash")
	case len(m.BaseURLs) == 0:
		return fmt.Errorf("manifest: at least one base_url is required")
	}
	switch m.Generator {
	case GeneratorPipeline, GeneratorDiff:
	default:
		return fmt.Errorf("manifest: invalid generator %q", m.Generator)
	}
	switch m.Auth {
	case AuthPublic, AuthToken, "":
	default:
		return fmt.Errorf("manifest: invalid auth %q", m.Auth)
	}
	for i, o := range m.Objects {
		if !isObjectName(o.Hash) {
			return fmt.Errorf("manifest: object %d has invalid hash %q", i, o.Hash)
		}
	}
	return nil
}

// isObjectName reports whether s is a safe CVMFS object name: a hex digest
// optionally followed by a content-type suffix letter — alphanumeric only, so it
// cannot encode path traversal ("..", "/") when turned into an object URL/path.
func isObjectName(s string) bool {
	if len(s) < 3 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		default:
			return false
		}
	}
	return true
}

// Missing returns the subset of Objects for which has(hash) reports false — the
// receiver-local delta (ADR D3). The receiver supplies a predicate backed by its
// own CAS, so no per-receiver hash list is round-tripped to S0.
func (m *Manifest) Missing(has func(hash string) bool) []ObjRef {
	out := make([]ObjRef, 0, len(m.Objects))
	for _, o := range m.Objects {
		if !has(o.Hash) {
			out = append(out, o)
		}
	}
	return out
}

// Encode writes the manifest as a single JSON document (incremental form).
func (m *Manifest) Encode(w io.Writer) error {
	return json.NewEncoder(w).Encode(m)
}

// Decode reads a single-JSON-document manifest.
func Decode(r io.Reader) (*Manifest, error) {
	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return nil, fmt.Errorf("manifest: decode: %w", err)
	}
	return &m, nil
}

// EncodeNDJSON writes the manifest in streaming form: a header line (the
// manifest metadata with Objects omitted) followed by one ObjRef JSON object per
// line. Suitable for very large (cold-start / catch-up) deltas.
func (m *Manifest) EncodeNDJSON(w io.Writer) error {
	if err := EncodeNDJSONHeader(w, m); err != nil {
		return err
	}
	for i := range m.Objects {
		if err := EncodeNDJSONObject(w, &m.Objects[i]); err != nil {
			return fmt.Errorf("manifest: encode object %d: %w", i, err)
		}
	}
	return nil
}

// EncodeNDJSONHeader writes just the NDJSON header line (manifest metadata with
// Objects omitted). Pair it with EncodeNDJSONObject to stream an object set that
// is too large to materialise — e.g. a catch-up diff generated on the fly (ADR
// D4 / P4), where the producer never holds the whole set in memory.
func EncodeNDJSONHeader(w io.Writer, m *Manifest) error {
	header := *m
	header.Objects = nil
	if err := json.NewEncoder(w).Encode(&header); err != nil {
		return fmt.Errorf("manifest: encode header: %w", err)
	}
	return nil
}

// EncodeNDJSONObject writes one ObjRef as a single NDJSON line.
func EncodeNDJSONObject(w io.Writer, o *ObjRef) error {
	if err := json.NewEncoder(w).Encode(o); err != nil {
		return fmt.Errorf("manifest: encode object: %w", err)
	}
	return nil
}

// DecodeNDJSON reads a streaming manifest: it parses the header line and then
// invokes onObj for each object record without buffering the whole set. The
// returned Manifest has a nil Objects slice. A nil onObj skips object bodies.
func DecodeNDJSON(r io.Reader, onObj func(ObjRef) error) (*Manifest, error) {
	return DecodeNDJSONStream(r, nil, onObj)
}

// DecodeNDJSONStream is the streaming decoder used by catch-up pulls (ADR D4 /
// P4). It parses the header line, invokes onHeader once (if non-nil) BEFORE any
// object — so the consumer has the header's BaseURLs/roots before it starts
// fetching — then invokes onObj for each object record. Neither the header nor
// the object set is buffered. A non-nil error from onHeader or onObj aborts the
// scan and is returned to the caller.
func DecodeNDJSONStream(r io.Reader, onHeader func(*Manifest) error, onObj func(ObjRef) error) (*Manifest, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // allow long header lines
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return nil, fmt.Errorf("manifest: read header: %w", err)
		}
		return nil, fmt.Errorf("manifest: empty NDJSON stream")
	}
	var m Manifest
	if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
		return nil, fmt.Errorf("manifest: decode header: %w", err)
	}
	if onHeader != nil {
		if err := onHeader(&m); err != nil {
			return nil, err
		}
	}
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var o ObjRef
		if err := json.Unmarshal(line, &o); err != nil {
			return nil, fmt.Errorf("manifest: decode object: %w", err)
		}
		if onObj != nil {
			if err := onObj(o); err != nil {
				return nil, err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("manifest: scan: %w", err)
	}
	return &m, nil
}
