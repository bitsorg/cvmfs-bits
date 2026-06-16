// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"context"
	"strings"
	"time"

	"cvmfs.io/prepub/internal/distribute/manifest"
	"cvmfs.io/prepub/pkg/cvmfshash"
)

// ObjectSizer reports object sizes (the read subset of cas.Backend used here).
type ObjectSizer interface {
	Size(ctx context.Context, hash string) (int64, error)
}

// BuildParams carries the fixed metadata for a transaction manifest.
type BuildParams struct {
	TxnID          string
	Repo           string
	BaseRootHash   string
	TargetRootHash string
	BaseURLs       []string
	Generator      manifest.Generator
	Auth           manifest.Auth
}

// BuildManifest assembles a manifest from the transaction metadata and the set
// of new-object hashes — the authoritative set the publish pipeline already
// computed during dedup (ADR D3). Object sizes are filled from sizer when
// provided; a per-object Size error is tolerated (left zero) so a transient
// store hiccup does not fail manifest generation.
func BuildManifest(ctx context.Context, p BuildParams, hashes []string, sizer ObjectSizer) (*manifest.Manifest, error) {
	m := &manifest.Manifest{
		TransactionID:  p.TxnID,
		Repo:           p.Repo,
		BaseRootHash:   p.BaseRootHash,
		TargetRootHash: p.TargetRootHash,
		BaseURLs:       p.BaseURLs,
		Generator:      p.Generator,
		Auth:           p.Auth,
		CreatedAt:      time.Now().UTC(),
		Objects:        make([]manifest.ObjRef, 0, len(hashes)),
	}
	for _, h := range hashes {
		o := manifest.ObjRef{Hash: h}
		if sizer != nil {
			if sz, err := sizer.Size(ctx, h); err == nil {
				o.Size = sz
				m.TotalSize += sz
			}
		}
		m.Objects = append(m.Objects, o)
	}
	return m, m.Validate()
}

// ObjectURL builds the fetch URL for a hash given an object base URL that ends
// in ".../data" (the value carried in Manifest.BaseURLs).
func ObjectURL(base, hash string) string {
	op := cvmfshash.ObjectPath(hash) // "data/xx/rest"
	return strings.TrimRight(base, "/") + "/" + op[len("data/"):]
}
