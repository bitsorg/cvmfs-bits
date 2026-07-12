# ADR-0006: Gateway-authoritative root + server-side catalog construction for the bits publish path

**Status:** Draft — for discussion (NOT decided, NOT committed). **Largely superseded by [ADR-0007](0007-descriptor-ingest-server-side-catalog.md)** if descriptor-based ingest is adopted; see 0007 §Relationship.
**Date:** 2026-07-12
**Deciders:** Predrag Buncic; cvmfs-bits maintainers; cvmfs gateway/receiver maintainers
**Component:** cvmfs gateway (`internal/gateway/{frontend,backend,receiver}`), cvmfs receiver (`cvmfs/receiver/{reactor,commit_processor}.cc`), cvmfs-bits/cvmfs-prepub (`internal/api`, `internal/lease`, `pkg/cvmfscatalog`)
**Supersedes (mitigation):** the prepub-side `waitForManifestPropagation` "serialize-until-published" barrier and lock-across-mutation change (unpushed, `cvmfs-devel`).
**Hard constraint:** the same gateway serves standard `cvmfs_server` clients on the DiffRec publishing path. Every change here MUST be additive and non-interfering — no change to the semantics of the existing `POST /api/v1/leases/:token` (commit) and `POST /api/v1/leases/:token/graft` endpoints or the DiffRec code path.

## Context

### Two publish paths share one gateway

1. **Standard path (DiffRec).** A `cvmfs_server` client submits a payload; the receiver (`CatalogMergeTool`, C++) diffs it against the current catalog and **builds/merges the catalog server-side**. This is what other clients of this gateway use and it must keep working unchanged.

2. **Bits path (DirectGraft).** The prepub compresses files, uploads content-addressed objects to the CAS/S3 store, **builds the nested catalog itself**, uploads the catalog object, and commits via the experimental graft endpoint. The receiver's DirectGraft branch grafts that pre-built catalog *by reference* (`WritableCatalogManager::TryGraftNestedCatalog`) — it does not build it.

### Problem 1 — throughput: the graft base lags

On the DirectGraft path the receiver picks its graft base from stratum0, ignoring the client's `old_root_hash`:

```cpp
// cvmfs/receiver/commit_processor.cc:244
const shash::Any manifest_base_hash;                    // null
manifest_tgt = FetchRemoteManifest(params.stratum0, repo_name, manifest_base_hash);
...
new catalog::WritableCatalogManager(manifest_tgt->catalog_hash(), ...) // base = whatever stratum0 serves now
```

Stratum0's `.cvmfspublished` **lags** the just-committed state (HTTP caching and/or S3 read-after-write visibility). Under the many-small-packages bits workload, a burst of commits to a fresh subtree all graft onto a stale base whose parent dirs are not yet visible → `CommitProcessor::kMergeFailure` (`merge_error`). The publish loop does not retry, so the pipeline fails.

The current mitigation (prepub `internal/api/orchestrator.go`): serialize commits per-repo and, after each successful commit, **poll stratum0 until the pointer advances** before releasing the lock (`waitForManifestPropagation`). This is correct but pays a propagation wait *per package* — the "takes ages" cost. Note the catalog *objects* are written synchronously and are content-addressed; only the `.cvmfspublished` *pointer* lags. The barrier is compensating for the receiver re-reading a stale pointer.

Also note (reactor.cc:472): the receiver already computes the new manifest hash (`manifest_tgt->catalog_hash()`) but the reply returns only `final_revision` — the new root is discarded, so the client cannot chain it and is forced to re-derive it from the lagging pointer.

### Problem 2 — maintenance: the catalog format is forked

`cvmfs-bits/pkg/cvmfscatalog/catalog.go` is a Go reimplementation of the CVMFS catalog SQLite format: it hardcodes `schema 2.5 / schema_revision 7`, creates the `catalog / chunks / nested_catalogs / bind_mountpoints / statistics / properties` tables, replicates the counter rows "cvmfs_receiver's `SqlGetCounter` expects" (its comments cite `cvmfs/catalog_counters.h`), and reproduces the MD5 path hashing. Any change to the cvmfs catalog schema or semantics must now be mirrored in Go, out of band from the canonical C++ implementation the standard path uses. This is a standing correctness and maintenance liability, and it is the deeper reason the bits path drifts from the standard path.

### What the gateway already gives us

The gateway **already serializes commits per repository** under a lock we can extend:

```go
// gateway/internal/gateway/backend/lease_service.go:400
s.DB.WithLock(ctx, lease.Repository, func() error {
    finalRev, err = s.Pool.GraftLease(ctx, leasePath, oldRootHash, newRootHash, tag, commitDeadline)
    ...
})
```

That lock is the natural home for a per-repo authoritative "publish head", and the reply plumbing already exists to carry a new root back up.

## Decision (proposed, phased — all additive)

**Make the gateway the per-repo authority for the current root, teach the receiver to graft onto a caller-supplied base and return the new root, and — strategically — move catalog *construction* into the gateway/receiver so the prepub stops shipping a hand-built catalog.** Every step is a *new* endpoint or an *optional* request field; the DiffRec path and existing endpoints are untouched.

### Phase 1 — authoritative base + return new root (removes the barrier)

**Receiver (backward-compatible extension of `DoCommit`):**
- Parse *optional* `base_root_hash` and `base_revision` from the request. Absent → today's behavior (`FetchRemoteManifest` from stratum0), so existing callers are unchanged. Present → build the `WritableCatalogManager` and manifest from that base (base catalog fetched from the object store; revision taken from `base_revision`).
- Always add `new_root_hash` to the `status:ok` reply, next to `final_revision` (additive JSON field; old gateways ignore it).

**Gateway (additive endpoint + in-process head tracker):**
- Per-repo `publishHead{rootHash, revision}` updated *inside* the existing `DB.WithLock(repo, …)`. Seeded lazily: the first head-commit for a repo passes an empty base (receiver falls back to stratum0), then records the returned root.
- New route `POST /api/v1/leases/:token/graft-head`: injects `publishHead[repo]` as `base_*`, updates the head from the reply, returns `{new_root_hash, revision}`. Legacy `/graft` and `/leases/:token` are unchanged and do **not** touch the tracker.

**Prepub (mine):**
- New `GraftHead` client call; orchestrator uses it for gateway subtree publishes and for `ensureParentDirs`, sets `j.NewRootHash` from the reply, and **deletes `waitForManifestPropagation`**. Behind `--gateway-graft-head` for A/B and rollback.

Result: commits chain at graft speed (no per-package propagation wait), still serialized, correct.

### Phase 2 — `reserve` (mkdir-p + reserve-or-409)

- New route `POST /api/v1/repos/:repo/reserve {path}`: under `DB.WithLock`, the receiver asserts the leaf is absent in the head catalog (new reply reason `path_exists` → HTTP 409) and grafts missing parent dirs against the head, returning the new root.
- Prepub replaces `ensureParentDirs` + the post-hoc `PathExists` probe with this one authoritative call. 409 is the clean, terminal "already published — no retry."

### Phase 3 — relax prepub serialization (throughput)

- With the gateway as base authority and serializer, disjoint-path packages can call `reserve`/`SubmitPayload`/`graft-head` concurrently; the gateway serializes the commits and each graft advances the head. Removes the prepub per-repo commit lock. Do last, validate carefully.

### Phase 4 — server-side catalog construction (strategic end-state, UNDER DISCUSSION)

The end-state that resolves Problem 2 and converges the bits path toward the standard model:

- **Prepub uploads objects (as today) and posts a description**, not a catalog: a JSON manifest of entries — `path`, content `hash` (+ compressed size), `mode`, `mtime`, `symlink target`, `hardlink group`, `xattrs`, and for chunked files the ordered chunk list `(hash, offset, size)` — plus the target `lease_path`.
- **The gateway/receiver builds the nested catalog** from that description using the canonical C++ `WritableCatalogManager` (`AddFile`/`AddDirectory`/`AddChunkedFile`/…), then grafts it against the authoritative head and returns the new root.
- **`pkg/cvmfscatalog`'s builder is retired.** The prepub keeps only what it needs to *describe* entries (path/metadata/hashes), not to author SQLite. Catalog-format knowledge lives once, in C++, shared with the standard path.

New route (additive), e.g. `POST /api/v1/leases/:token/publish-manifest` carrying the description; the receiver gains a "build catalog from entry list" op that reuses existing catalog-manager machinery. This is a larger change and is the main thing still to debate (see Open questions).

## Additive API summary (nothing existing changes)

| New surface | Where | Purpose |
|---|---|---|
| optional `base_root_hash`, `base_revision` in commit/graft request | receiver | graft onto authoritative base instead of stratum0 |
| `new_root_hash` in ok reply | receiver | let gateway/client chain the root |
| reason `path_exists` | receiver | clean duplicate detection |
| per-repo `publishHead` under `DB.WithLock` | gateway backend | authoritative current root |
| `POST /leases/:token/graft-head` | gateway frontend | Phase 1 |
| `POST /repos/:repo/reserve` | gateway frontend | Phase 2 |
| `POST /leases/:token/publish-manifest` | gateway frontend | Phase 4 |

## Consequences

**Positive.** Removes the propagation barrier (throughput); makes the gateway the single source of truth for the current root (safer under multiple publishers than everyone re-reading stratum0); gives clean, authoritative duplicate rejection; and (Phase 4) eliminates the forked Go catalog implementation, collapsing catalog-format maintenance to one C++ codebase shared with the standard path. `.cvmfspublished` propagation to FUSE clients becomes a pure read-side concern decoupled from publish throughput.

**Negative / cost.** Phase 4 shifts catalog-build CPU from distributed prepub workers to the gateway/receiver pool (acceptable for many-small-package catalogs; watch at scale). The receiver changes touch signing/revision continuity, which must be got right. Two more moving parts (head tracker, new endpoints) to operate.

**Neutral.** Serialization remains for correctness of the head chain until Phase 3; the win in Phase 1 is removing the *wait*, not the *ordering*.

## Alternatives considered

- **Keep the barrier, just tune it.** Rejected: it is compensating for the receiver reading a stale pointer; the wait is inherent, not tunable away.
- **Cache-bust the stratum0 read in the receiver.** Insufficient if the lag is S3 read-after-write, not only HTTP caching; and it still re-reads an external pointer instead of using authoritative in-process state.
- **Return the new root to the client only (client chains it) without changing the receiver's base.** Rejected: the receiver would keep grafting onto the lagging pointer regardless of what the client sends — the base selection is the actual bug.
- **Modify the existing `/graft` semantics in place.** Rejected by the hard constraint: other clients depend on the current behavior.

## Risks / things to get right

1. **Revision continuity.** When grafting onto a base ahead of stratum0, the manifest revision must come from the tracked head (`base_revision`), not stratum0, or tags/history regress. Primary receiver-side review item.
2. **Head/stratum0 divergence + mixed publishers.** `graft-head` and legacy `graft` must not run concurrently on the *same* repo (legacy advances stratum0 without touching the tracker). True for test.cvmfs.io (sole publisher); must be documented/guarded for shared repos. Consider a per-repo "authoritative mode" flag so a repo opts into head-tracking exclusively.
3. **Head recovery.** On gateway restart the in-process head is lost; re-seed from stratum0 on first use (safe because at rest stratum0 is current). Consider persisting head in the lease DB.
4. **Trust boundary (Phase 4).** Building server-side means the gateway validates the description; objects must already be in CAS (content-addressed, verifiable). Cleaner trust model than accepting a client-authored SQLite, but the description parser is new attack surface — validate paths/hashes strictly.

## Migration / rollback

Deploy order is forced: (1) receiver (accepts `base_*`, returns `new_root_hash`) — backward-compatible, shippable alone; (2) gateway endpoint + head tracker; (3) prepub `--gateway-graft-head`. Rollback = turn the flag off; old endpoints and code paths remain intact throughout. Phases 2–4 layer on independently.

## Open questions (for discussion — not yet decided)

- **Phase 4 scope/timing.** Do we commit to server-side catalog construction as the target now (and treat Phases 1–3 as steps toward it), or land 1–3 first and revisit 4 once throughput is acceptable?
- **Description schema.** What exactly must the entry description carry to cover everything bits publishes (chunked files, xattrs, hardlinks, special files, autocatalog hints, `.cvmfsdirtab`)? Can it be a thin projection of what `pkg/cvmfscatalog` already assembles as `CatalogEntries`?
- **Where does the head live** — in-process only, or persisted in the gateway lease DB for restart safety and multi-instance gateways?
- **Shared-repo safety.** Is a per-repo "head-authoritative" opt-in mode worth building now, given the standard path must coexist?
- **Do we still need the prepub per-repo lock after Phase 1**, or can Phase 3 land together with Phase 1 since the gateway already serializes?
