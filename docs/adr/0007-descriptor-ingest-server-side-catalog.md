# ADR-0007: Descriptor-based ingest — prepub emits file metadata, canonical CVMFS code builds the catalog

**Status:** Decided — pending implementation (NOT committed). **Locked decisions:** Variant A (reuse `ingestsql`, delete `pkg/cvmfscatalog`); coarse + serialized, one commit per publish at end-of-build; **zero gateway changes**; blast radius handled prepub-side (validate-then-commit + bits-hash dedup). Publish-at-end-of-build (no incremental visibility) accepted, which is what permits the zero-gateway-change path.
**Date:** 2026-07-12
**Deciders:** Predrag Buncic; cvmfs-bits maintainers; cvmfs gateway/receiver maintainers
**Component:** cvmfs (`cvmfs/swissknife_ingest*.cc`, `cvmfs/catalog_mgr_rw.*`, `cvmfs/receiver/*`), cvmfs gateway (`internal/gateway/*`), cvmfs-bits/cvmfs-prepub (`pkg/cvmfscatalog`, `internal/api`, `internal/lease`)
**Relationship to ADR-0006:** If adopted, this **largely supersedes ADR-0006**. 0006 keeps the prepub-built catalog and works around the graft-base lag with new gateway endpoints; this ADR removes the prepub-built catalog altogether, which dissolves most of the problem 0006 was solving. The one 0006 idea that may survive as a smaller, optional robustness fix is the gateway-authoritative base (see §Relationship).
**Hard constraint (unchanged):** the same gateway serves standard `cvmfs_server` clients. All changes must be additive; existing endpoints and the DiffRec path keep their current semantics.

## Context

### Where catalogs are built today

- **Standard publish / ingest (canonical, C++).** `cvmfs_server ingest` → `swissknife::Ingest` (`swissknife_ingest.cc`) reads a **tar** (`sync_union_tarball`), builds the catalog with the canonical writable catalog manager (`catalog_mgr_rw` — `AddFile`, `AddChunkedFile`, `AddDirectory`, `AddHardlinkGroup`, `TryGraftNestedCatalog`), spools the resulting objects, acquires a **gateway lease**, and commits. The tar is consumed **client-side** (release manager); the gateway ingests the resulting **ObjectPack** and runs the commit/merge.
- **`swissknife_ingestsql` (canonical, C++, descriptor-driven).** Same machinery, but the input is a **descriptor** (SQLite `.db` files) rather than a tar. It drives `catalog_mgr_rw`, acquires/refreshes a gateway lease, and calls `make_commit_on_gateway(old_root_hash, …)`. This is essentially "build catalog from a precomputed description of entries, commit through the gateway."
- **Bits path (prepub, Go).** The prepub compresses/hashes/dedups/uploads objects to CAS (the expensive, parallel work), then **re-implements the CVMFS catalog format in Go** (`pkg/cvmfscatalog`: SQLite `schema 2.5 / schema_revision 7`, counter rows mirroring `catalog_counters.h`, MD5 path hashing) to build a nested catalog, and commits via the experimental DirectGraft, which grafts the prebuilt catalog by reference.

So the bits path is the *only* place a non-canonical, forked catalog builder exists — and it is also the source of the graft-base-lag/`merge_error` problem (ADR-0006), because it commits per-package via DirectGraft against a stratum0 base that lags.

### The receiver already ingests objects; it doesn't need a tar

The receiver's `PayloadProcessor` consumes an **ObjectPack** (`ObjectPackConsumer`), writing content objects (`kCas`) and named objects like catalogs (`kNamed`) into the store. It has no tar or JSON parser. Catalog *authoring* happens in the client tool (`swissknife_ingest`/`ingestsql`) via `catalog_mgr_rw`; the receiver's `CommitProcessor` does the merge/graft at commit.

## Proposal

**Keep the expensive, parallelisable work in the prepub (compress, hash, CAS upload, dedup). Stop building the catalog in the prepub. Instead, hand a JSON descriptor of the already-uploaded objects to the canonical CVMFS catalog builder — modelled on `ingestsql` — and let it build the catalog and commit through the gateway.**

The descriptor is a list of entries for objects that are *already in CAS*: for each, its `path`, content `hash` (+ compressed size), `mode`, `mtime`, and as needed `symlink target`, `hardlink group`, `xattrs`, chunk list `(hash, offset, size)`, plus nested-catalog / dirtab hints. The builder calls `catalog_mgr_rw::AddFile`/`AddChunkedFile`/`AddDirectory`/… — no object upload (objects exist), just catalog registration — then commits.

Two ways to realise it (to be chosen in discussion):

**(A) Client-side, reuse the canonical tool — least work, no gateway change.**
The prepub, after CAS upload, emits the descriptor and invokes the canonical C++ ingest tool (a JSON-input sibling of `ingestsql`, or `ingestsql` itself if we emit its SQLite descriptor). That tool builds the catalog with `catalog_mgr_rw` and commits via the gateway lease exactly like `ingestsql` does today. **No gateway/receiver change required** — it uses the existing lease + commit endpoints. `pkg/cvmfscatalog` is deleted.

**(B) Server-side, new additive gateway endpoint — matches the "gateway builds the catalog" goal.**
A new endpoint (e.g. `POST /api/v1/leases/:token/ingest-manifest`) consumes the JSON descriptor; the gateway/receiver builds the catalog with `catalog_mgr_rw` **inside the gateway host** and commits under the per-repo lock it already holds (`DB.WithLock`). Because it builds and commits server-side against the gateway's own current state, it can graft onto the authoritative base directly — folding in ADR-0006's base fix for free. More work (a new receiver op that drives `catalog_mgr_rw` from a parsed descriptor), but the cleanest long-term shape and the one that centralises all catalog handling in the gateway.

Either way, the descriptor format and the entry coverage are the same; (A) vs (B) is only *where the builder runs*.

## What this removes

- **`cvmfs-bits/pkg/cvmfscatalog`** (the Go catalog/SQLite reimplementation: `catalog.go`, `subtree.go`, `subtree_helpers.go`, `entry.go`, `xattr.go`, `manifest.go`, `exists.go`) — deleted. Catalog-format knowledge lives once, in C++, shared with the standard path.
- **Most of the orchestrator's catalog/graft machinery** (`internal/api/orchestrator.go`): `BuildSubtree`, `ensureParentDirs`, the DirectGraft submit/commit dance, `waitForManifestPropagation`, the lock-across-mutation change from this cycle — all gone or greatly reduced. The prepub shrinks to: pipeline (compress/hash/CAS/dedup) → emit descriptor → invoke builder / call endpoint → done.
- **Most of ADR-0006.** No `graft-head`, no per-package base chaining, no `reserve` endpoint — duplicate detection and parent creation come from the canonical builder (`AddDirectory` creates parents; a pre-existing path is a normal catalog-level condition), and the burst problem shrinks because a publish becomes one descriptor/commit rather than N tiny grafts.

## Feasibility

Strong, because the pattern already ships: `ingestsql` proves the canonical builder can be driven from a precomputed descriptor and committed through a gateway lease. The main new pieces are (1) a JSON descriptor schema (or reuse of the SQLite descriptor `ingestsql` already accepts) and (2) an "objects already in CAS — skip upload" mode so the builder registers entries without re-spooling content. `catalog_mgr_rw` already exposes exactly the add-operations the descriptor needs, including chunked files, hardlink groups, and nested-catalog grafts.

### The `ingestsql` descriptor (dug in) — near drop-in for Variant A

Input is a SQLite DB (`-D`) with tables:
- `dirs(name, mode, mtime, owner, grp, acl, nested)` — `nested` flags a nested-catalog boundary; `acl` is marshalled into an `XattrList`.
- `files(name, mode, mtime, owner, grp, size, hashes, internal, compressed)` — content referenced **by hash** (`hashes` is chunk-aware; `internal` = inlined small file; `compressed` = compression flag). The file **content objects are assumed already in the store**; `ingestsql`'s spooler uploads only the catalog objects it builds.
- `links(…)` — symlinks.
- a `properties` table with `schema_revision` and a `completed_graft` idempotency marker.

It loads these, builds the full path tree (including parent dirs), and calls `catalog::WritableCatalogManager::AddDirectory / AddFile / AddChunkedFile / TouchDirectory` + symlink adds — the canonical builder. Flags already present: `-z` create missing nested catalogs (parent creation), `-B` block on pending visibility, `-Z` completed-graft idempotency, `-P` priority.

Because content is referenced by hash and assumed already in the store, this is precisely the bits model (prepub already uploads objects to CAS). So Variant A is nearly drop-in: prepub emits this SQLite descriptor as a projection of its `CatalogEntries` and invokes `ingestsql` at the gateway. `-D` (the descriptor) is essentially all the producer-side work.

## Does this still need serialization? (the lag analysis)

Yes — unless the gateway is changed, but the change is far smaller than ADR-0006's new endpoints.

`ingestsql` already defeats the stratum0 lag **client-side**: after acquiring the lease it reads `current_revision` + `current_root_hash` from the acquire reply, and when the gateway is ahead of `.cvmfspublished` (the lag case) it rebases its build onto the gateway's root (`swissknife_ingestsql.cc:812-819`). This is exactly ADR-0006's authoritative-base idea, and it is the standard upstream contract.

**But no gateway in this repo supplies those fields.** `handleNewLease` returns only `status`, `session_token`, `max_api_version` — confirmed on both `devel` and `origin/bits`, and no branch sets `root_hash`/`revision` anywhere in the gateway frontend (the client parser `ParseAcquireReplyWithRevision` reads JSON `revision` + `root_hash`). So `ingestsql`'s revision-reconciliation is dormant against every gateway here; it falls back to "Using .cvmfspublished" and builds against the lagging base.

The minimal, additive gateway change (if needed) is:

> **Track the authoritative head per repo (in-process, under the `DB.WithLock` the commit path already holds) and return `revision` + `root_hash` in the existing acquire reply.**

No new endpoints; it enriches one existing reply with fields the client already consumes and upstream already defines.

### Granularity ↔ gateway-change (the key mapping)

Whether that change is needed at all depends entirely on commit granularity:

- **Coarse (one commit per publish, serialized) → ZERO gateway changes.** `ingestsql` reads its base from stratum0; with a single commit per publish, serialized and settled, stratum0 *is* current at build time, so the base is correct and there is no lag. The merge_error problem only ever came from concurrent / rapid-succession commits (the per-package burst); coarse has neither. Most-minimal-gateway (nothing to change) and most reliable (one atomic-ish commit, no races). Cost: whole-build latency + one large transaction (performance).
- **Batched / per-package → needs the acquire-reply enrichment.** Multiple commits per publish reintroduce rapid succession, so between commits you need either a settle-wait (barrier, slow) or the enrichment. Concurrent commits to the *same* repo additionally race even with the enrichment (rebase is captured at *acquire*, not *commit*) — that residual is the only thing ADR-0006's commit-time `graft-head` would address, and only if same-repo concurrency is required.

So granularity is the lever, and it maps cleanly onto the (already-minimal) gateway work.

### Gateway compatibility (bits and devel)

Verified against both `origin/bits` and `origin/devel`: `swissknife_ingestsql` is present on both; the standard DiffRec commit path in the receiver is present and identical on both; and the lease API is byte-identical (acquire → `session_token`, commit/graft → `final_revision`, neither returns `root_hash`/`revision`). `ingestsql` commits via the *standard* commit endpoint (`make_commit_on_gateway`), not the bits-only DirectGraft, so the coarse Variant A flow runs unchanged on **either** gateway. The acquire-reply enrichment is absent from both, so the batched/concurrent escalation would need the same small patch on whichever gateway is deployed — not required for coarse.

## Recommendation

Given the stated priority order — **(1) minimal gateway change, (2) reliability, (3) performance** — and Variant A chosen:

1. **Variant A** — reuse `ingestsql` (or a JSON-input sibling); delete `pkg/cvmfscatalog`. No new gateway endpoints.
2. **Start coarse + serialized** — one deduplicated descriptor per publish, one commit, one at a time per repo. This needs **zero gateway changes**, is race-free, and is the most reliable (single atomic-ish commit). Bound blast radius entirely on the prepub side: **validate-then-commit + bits-hash dedup**, no gateway involvement.
3. **Escalate to batched-by-dependency-layer only if** a publish grows large enough to risk lease-expiry or an unwieldy single transaction, **or** if the workflow requires incremental visibility of packages as they finish. Only then add the acquire-reply enrichment (`revision` + `root_hash`) — the single upstream-standard, forward-compatible gateway change.

The forcing function to decide up front: **coarse defers all publishing until the whole build completes** (no incremental visibility). If the workflow needs packages to appear as built, go straight to batched + the acquire enrichment.

## Implementation plan

Decided context: Variant A, coarse + serialized, publish-at-end-of-build, zero gateway change required. The prepub keeps the pipeline (compress/hash/CAS upload/dedup) and stops authoring catalogs; the canonical `ingestsql` builds and commits from a descriptor. Each phase has an explicit verification gate (goal-driven; see CLAUDE.md §4).

### Phase 0 — Round-trip `ingestsql` (de-risk gate, no prepub changes)

Prove the mechanism and pin the descriptor schema empirically before writing any emitter.

- Hand-build a tiny SQLite descriptor (`dirs` + `files` + `properties.schema_revision`) referencing 2–3 objects **pre-placed in the repo's CAS/S3 store**; run `cvmfs_swissknife ingestsql -D … -N <repo> -g <gateway> -w <stratum0>` against a scratch repo on the bits (or devel) gateway.
- **Verify:** commit succeeds; objects are *not* re-uploaded (already in store); files resolve at the expected paths under `/cvmfs/<repo>`; a second run of the same descriptor is idempotent (or fails cleanly, per `completed_graft`).
- **Output:** the exact `schema_revision`, the `hashes` column encoding for plain and chunked files, and the `acl`/xattr representation — the ground truth the emitter must match.

### Phase 1 — Descriptor emitter (prepub)

New `pkg/cvmfsdescriptor` (sibling of the doomed `pkg/cvmfscatalog`): given `[]cvmfscatalog.Entry`, write the `ingestsql` SQLite descriptor.

- Map `Entry` → `dirs(name,mode,mtime,owner,grp,acl,nested)` / `files(name,mode,mtime,owner,grp,size,hashes,internal,compressed)` / `links`. `nested` ← `IsNestedRoot`; `compressed` ← `CompAlgo`; chunked files ← encode `Chunks[]` in `hashes` per Phase 0; `acl`/xattr ← map `Entry.Xattr` (**friction point:** `ingestsql` expects ACL text via `acl_from_text_to_xattr_value`; the prepub has an arbitrary xattr map — confirm the column format or extend the schema).
- **Verify:** unit tests for the SQLite writer; emit from a known `[]Entry`, run through the Phase 0 harness, and diff the resulting tree against expectation (dirs, files by hash, symlinks, chunked file, xattr, hardlink group, nested boundary).

### Phase 2 — Coarse publish orchestration (prepub + bits-console)

- Per-package job (`internal/api/orchestrator.go`): run the pipeline and **record the package's `[]Entry` + provenance (bits hash) into a build-scoped accumulator** in the spool — but do **not** commit.
- New end-of-build finalize step in the prepub: assemble all packages' entries for the build, write one descriptor (Phase 1), invoke `ingestsql` once against the publish root.
- bits-console pipeline: replace per-package publish/commit with a single terminal `finalize` job that triggers the prepub finalize for the pipeline/build ID.
- **Design to settle:** the publish-set key (pipeline/build ID) and how the prepub groups jobs; serialize one finalize per repo at a time.
- **Verify:** a multi-package build publishes in a single commit; all packages appear and resolve; a second build to the same repo serializes cleanly.
- **Status (2026-07-12): implemented, pending testbed validation.** Core: `internal/buildset` (accumulator `Record`/`Load`, `Assemble` with bits-hash dedup/conflict + package-relative→repo-relative expansion + nested-per-package, `Finalize` = one descriptor via `pkg/cvmfsdescriptor` + one `cvmfs_swissknife ingestsql` invocation); `job.Job.BuildID` plumbed through `submitJob`. Live-wiring: `StateAccumulated` terminal FSM state (+ spool dirs/lookups); `Orchestrator.Run` records entries and finishes in `StateAccumulated` instead of committing when `BuildID` is set (empty preserves legacy per-package commit); `POST /api/v1/builds/{id}/finalize` assembles the build and publishes it in one commit via `buildset.Finalize`, reporting published/conflicts. Commits `62e4e44` (core) + `abaadf3` (wiring) on `cvmfs-devel`; whole module builds/vets/tests green.

**Testbed findings + fixes (2026-07-12):**
- A 2-package coarse descriptor panicked in `ingestsql` (`catalog_mgr_rw.cc`: "catalog for directory … cannot be found") — `ingestsql` does not reliably auto-create the intermediate ancestor directories of a *branching* multi-package tree; they must be in the descriptor. `buildset.Assemble` now synthesises a dir entry for every ancestor between the build's common lease root and each entry (commit `a07c38b`).
- The pipeline emits a package's root entry as `FullPath="."` (and may carry a leading `./`); `expand` normalises both to the package base (commit `e5be127`).

**Full service E2E PASSED (2026-07-12).** Two packages submitted through the running prepub API with a shared `build_id` each pipelined and reached `StateAccumulated` (orchestrator branch, entries recorded in `builds/<id>/`). The real `buildset.Finalize` (run via the new `cmd/prepub-finalize` host CLI) loaded both members, assembled one descriptor (7 dirs, 4 files incl. synthesized intermediates), and published them in a single `ingestsql` commit — `{packages:2, published:2, conflicts:null}` — with both package roots as nested catalogs in the new published root.

### Multi-host / S3 deployment (verified)

The testbed is co-located (prepub CAS bind-mounted to the gateway's local store); production splits prepub, gateway and stratum0 across hosts over a shared **S3** store. This works because:
- **Object flow is unchanged and correct.** In the bits path, `SubmitPayload` only ever uploads *catalog* objects to the gateway (`orchestrator.go:1209`); **file objects are never sent to the gateway** — the pipeline writes them to the CAS, which *is* the shared store (S3 in production). So after the pipeline (before finalize) every file object is already in S3; `ingestsql` references them and uploads only the catalog objects it builds. Coarse changes nothing here.
- **Finalize needs no local store.** `ingestsql` uploads catalog objects via its spooler (S3 — the `ingestsql` patch already uses the repo's `CVMFS_UPSTREAM_STORAGE`, so an S3 upstream def just works), fetches the base catalog from stratum0 over HTTP, and commits via the gateway over HTTP. It only needs `cvmfs_swissknife` (patched), the S3 config + gateway key, and network to gateway + stratum0 + S3 — runnable on the prepub host, a dedicated finalize host, or the prepub container (add swissknife). No store mount.
- **Deployment requirements:** (1) prepub CAS = the shared S3 store (already a bits requirement — file objects live only in the CAS); (2) the finalize environment carries swissknife + an S3 `CVMFS_UPSTREAM_STORAGE`/S3 config + gateway key + stratum0/gateway URLs.

**Finalize as a job (implemented).** Rather than a bespoke endpoint call, the finalize is a normal prepub job so the console reuses its existing submit+poll loop: a payload-less `/jobs` submission with `finalize=true` + `build_id`. `Orchestrator.Run` short-circuits it to `Orchestrator.FinalizeBuild` (shared with the `/builds/{id}/finalize` endpoint), which uses the prepub's configured ingest settings (`IngestSwissknife`/`IngestConfigPrefix`/`IngestEnv`). **bits-console** (`cvmfs-prepub-publish.yml`, opt-in `PREPUB_COARSE=true`): every package is submitted with a shared `build_id` (the pipeline id) → accumulates; after all reach `accumulated`, one finalize job is submitted and polled to `published`. Empty `PREPUB_COARSE` preserves legacy per-package commits. Commits: prepub `53fe0f5`, console `ac9eed8`.

**Remaining (deployment / #2):** package `cvmfs_swissknife` (with the spooler patch) into the environment that runs finalize (the prepub image, or a host running `cmd/prepub-finalize`), and provide the ingest config (`IngestConfigPrefix` → a gateway-client dir with the S3 `CVMFS_UPSTREAM_STORAGE` + S3 config + gateway key + stratum0/gateway URLs). This is a `cvmfs-testbed` image/config change plus `cvmfs-bits` docs/`install.sh`.

### Phase 3 — Dedup + validate-then-commit (prepub)

- At assembly, dedup entries by **bits hash** (first-wins; drop the loser's objects to GC).
- Pre-flight the descriptor against the published common-manifest bits hashes: same path + same bits hash → skip; same path + **different** bits hash → drop + report; commit the consistent remainder.
- **Verify:** unit tests for dedup and the three conflict cases; an integration test with an injected divergent-build collision confirms it is reported and excluded, not fatal, and the rest publishes.

### Phase 4 — Remove the Go catalog builder

- Delete `pkg/cvmfscatalog` catalog authoring (`catalog.go`, `subtree.go`, `subtree_helpers.go`, `manifest.go`, `exists.go`) and the orchestrator's DirectGraft / `ensureParentDirs` / `waitForManifestPropagation` / `graft-head` / lock-across-mutation machinery (including this cycle's additions). Keep only `Entry`/`ChunkRecord` (the descriptor input model), moved into `pkg/cvmfsdescriptor`.
- **Verify:** `go build ./... && go test ./...` pass; a clean end-to-end build publishes via `ingestsql` only; `grep` confirms no remaining references to the deleted builder or the DirectGraft path.

### Phase 5 — Packaging & deploy

- Ensure `cvmfs_swissknife` (with `ingestsql`) is present in the prepub runtime (container image / package dependency), versioned alongside the prepub.
- Confirm `ingestsql`'s spooler upstream == the prepub's CAS/S3 store, so by-hash file references resolve (config alignment — likely already true).
- **Verify:** staged deploy; a real build publishes end-to-end; stratum0 serves the tree; S1 replication (ADR-0001) unaffected.

### Phase 0 finding — `ingestsql` object spooler is hardcoded to S3 (revises "zero cvmfs change")

Running Phase 0 on the local-storage testbed surfaced that `ingestsql` builds its **catalog objects** and uploads them via a spooler whose definition is **hardcoded to S3** (`swissknife_ingestsql.cc`: `"S3," + tmp + "," + repo + "@" + s3_file`), with a gateway spooler (`"gw,,"…`) sitting commented out right above it. cvmfs itself supports `local`, `S3`, and `gw` spooler drivers, but `ingestsql` only ever uses S3. Consequences:

- On a **local-storage** repo (the testbed: `CVMFS_UPSTREAM_STORAGE=local,…`) `ingestsql` cannot write its catalog objects at all — it aborts before commit.
- Even on S3, `ingestsql`'s object upload **bypasses the gateway** (direct S3), which is inconsistent with the gateway-mediated model.

So the "zero cvmfs change" claim is revised to **one small `ingestsql` (swissknife, client-side) change — not a gateway API change**: make the spooler follow the repo's configured upstream instead of assuming S3. The repo's `CVMFS_UPSTREAM_STORAGE` value is already a valid spooler-definition string (`type,txn,base`), so the fix is essentially "use the repo upstream as the spooler definition" (covers `local`/`S3`), with the `gw` spooler as the cleaner long-term option (objects submitted through the gateway lease, backend-agnostic). This is consistent with Variant A — `ingestsql` is a client tool we already accept building/shipping — and leaves the gateway untouched.

**Phase 0 result: PASSED (2026-07-12, testbed `test.cvmfs.io`, local storage).** With the spooler fix (committed on `feature/ingestsql-spooler-follow-upstream`) a descriptor built by our schema (schema_revision 4), referencing an object already in the store, was ingested and committed via the gateway: lease acquired, catalog built, **no object re-upload**, signed publish advanced stratum0 from root `d4a03c4a…` (rev 34) to `f6308220…` (rev 35), and the new root catalog contains the published entry. This validates the whole descriptor → `ingestsql` → gateway path for Variant A. Incidental confirmation: `ingestsql` logged *"Gateway has not supplied a revision. Using .cvmfspublished"* — i.e. the coarse single-commit path works without the acquire-reply enrichment, which is exactly the field the enrichment (`feature/gateway-acquire-reply-root`) would supply for the batched/concurrent case.

### Sequencing & effort

Phase 0 is a hard gate — it validates the entire approach for a day's work before committing to Phases 1–5. Phases 1→2→3 are ordered; Phase 4 (deletion) only after 1–3 are proven end-to-end; Phase 5 runs alongside 0–2 (packaging can proceed early). The gateway acquire-reply enrichment (already implemented on `feature/gateway-acquire-reply-root`) is **not** on this path — coarse needs none of it — but if merged, `ingestsql` picks it up automatically and enables a later batched escalation without further client work.

### Cross-cutting details to resolve

- **Chunking — DECIDED: align to `ingestsql`'s fixed size, do NOT extend `ingestsql`.** `ingestsql` assumes fixed-size chunks (`24 MiB` external / `6 MiB` internal), computing offsets as `i·chunkSize` and asserting `len(hashes) == ceil(size/chunkSize)`. To keep gateway/cvmfs changes at zero, the prepub's ingestsql path uses **fixed 24 MiB chunking** (`ChunkSize = 24 MiB`, content-defined `ChunkAvg = 0`, `internal = 0`) instead of content-defined chunking. Consequence: files > 24 MiB are re-chunked at fixed boundaries, changing those objects' content hashes (a one-time rebuild) and slightly reducing dedup for large files only; files ≤ 24 MiB (one hash) are unaffected — the vast majority. The emitter then emits one hash per ≤24 MiB file and `ceil(size/24MiB)` hashes otherwise.
- **File xattrs / hardlinks — RESOLVED, no cvmfs change needed.** bits-built software carries **no xattrs**, so the descriptor's lack of a file xattr column is a non-issue and `dirs.acl` is always empty. **Hardlinks are converted to symlinks** — which the descriptor represents natively via the `links` table — so the missing hardlink column is also a non-issue. The emitter therefore only ever writes plain files, symlinks and dirs. Task: the prepub converts any hardlink group to one real file plus symlinks to it (or the build guarantees none); confirm no residual hardlinks reach the descriptor.
- **End-of-build trigger contract** — the console↔prepub handshake and build-set identity (Phase 2).
- **Lease ownership** — `ingestsql` acquires/refreshes its own lease and commits; the prepub likely stops acquiring publish leases itself (simplifies `internal/lease` usage).
- **Serialization** — one `ingestsql` per repo at a time (one finalize per build; builds to the same repo queued).

## Conflict handling & blast radius

A coarse, atomic "one commit per publish-set" naively risks one bad package failing the whole publish, and raises the concurrent-shared-dependency case: two jobs both need dep D; if each publishes its own closure, the second registration of D conflicts. Resolution:

**The dedup/conflict key is the bits hash (identity), NOT the CVMFS content hash.** A package built from the same recipe + configuration + dependencies on two nodes has the same **bits hash** (identity → CVMFS path `pkg/version-revision/…`) but may have *different* **content hashes** (embedded timestamps, build paths). So content-hash equality cannot be the idempotency test; bits identity is.

- **Coarse = defer + dedup by bits hash.** Assembling the publish set into a *single* descriptor lets the prepub emit each identity (bits hash) exactly once, first-wins; the losing build's objects are left unreferenced in CAS → GC. The shared-dependency conflict then vanishes by construction (it is an artifact of *incremental per-job* publishing).
- **Same path + same bits hash → idempotent skip (first-wins).** The builds are interchangeable by bits' contract (same recipe → ABI-equivalent; the byte diff is non-reproducibility noise), so content-hash divergence is expected and ignored.
- **Same path + different bits hash → fatal.** A genuinely different identity under an already-claimed `pkg/version-revision` label without a revision bump — this is the "republish of the same version cleanly fails" rule.
- **Layering.** Identity + dedup live in the **prepub** (bits domain); the canonical CVMFS builder (`ingestsql`/`catalog_mgr_rw`) stays identity-agnostic (paths + content only). The **bits hash rides along as catalog metadata** (already recorded — the manifest carries the bits fingerprint from the provenance work), so a pre-flight can read the existing entry's bits hash at a path and fail only on a true mismatch. The reserve/idempotency token in ADR-0006 is therefore the **bits hash**, not the content hash.
- **Validate-then-commit (partial success)** to bound blast radius: pre-flight the assembled descriptor; drop same-identity duplicates silently, drop+report true (different-bits-hash) collisions, commit the consistent remainder. Keeps the single fast commit but isolates faults ("published all but these N, with reasons") instead of all-or-nothing. Trade-off: not strictly atomic, so omissions must be reported loudly.
- **Granularity is free of the performance trade.** The acquire-reply authoritative base removes the propagation wait, so batched/per-package commits are no longer expensive; granularity becomes a blast-radius/latency choice.

**Accepted property:** because the winning build is race-dependent and content hashes differ, the published tree is **not bit-reproducible** — which build's bytes land is nondeterministic. This is acceptable under bits' interchangeability contract (ABI-stable, timestamp-only noise) but should be stated, not incidental. CVMFS file-level content dedup still operates underneath on content hash, independently.

## Consequences

**Positive.** Eliminates the forked Go catalog implementation (the single worst coupling); the bits path becomes a *thinner* client than the standard one (pre-hashed objects + metadata instead of a tar); catalog-format maintenance collapses to one C++ codebase; and the graft-base-lag problem largely dissolves (fewer, coarser commits; and in variant B, authoritative-base commits). Prepub codebase shrinks substantially.

**Negative / cost.** In variant B, catalog-build CPU moves to the gateway host (fine for many-small-package catalogs; watch at scale). A dependency on a canonical C++ ingest tool/endpoint is introduced into the prepub flow (build/package/version coupling). The descriptor must faithfully carry everything bits publishes (chunked files, xattrs, hardlinks, special files, dirtab/autocatalog hints) — schema completeness is the main design risk.

**Neutral.** Serialization/ordering semantics are inherited from the existing ingest/commit path, which the gateway already handles for standard clients.

## Relationship to ADR-0006

- 0006 Phase 1 (authoritative base): **survives in a much smaller form** — not the new `graft-head` endpoint, but simply enriching the existing acquire reply with `current_revision` + `current_root_hash` (see §"Does this still need serialization?"). That single additive change is the only gateway work needed for serialized/coarse publishing. The `graft-head` commit-time endpoint is needed only for fine-grained same-repo concurrency.
- 0006 Phase 2 (`reserve`/mkdir-p/409): **replaced** by the canonical builder's native directory creation + existing-path handling.
- 0006 Phase 3 (relax prepub serialization): **absorbed** — concurrency is whatever the ingest/commit path already supports.
- Net: adopt 0007 → retire or heavily trim 0006.

## Alternatives considered

- **Keep 0006 (prepub builds catalog, patch the gateway around the lag).** Rejected as the primary direction: it entrenches the Go catalog fork, which is the deeper liability. May remain as a fallback if 0007 proves too invasive short-term.
- **Server-side tar ingest (send the tar, gateway extracts).** Rejected: it throws away the prepub's compression/hash/CAS/dedup work and re-does it, and moves large data through the gateway. The point is to keep those in the prepub and send only metadata.

## Open questions (for discussion)

1. **Variant A vs B** — client-side reuse of a canonical tool (least work, no gateway change, uses existing lease+commit) vs a new server-side gateway endpoint (centralises catalog handling, folds in the base fix). Which is the target, and do we stage A→B?
2. **Descriptor format** — new JSON, or emit the SQLite descriptor `ingestsql` already consumes? JSON is friendlier to produce from Go; SQLite means zero new C++ parser.
3. **"Objects already in CAS" mode** — `ingest`/`ingestsql` normally spool objects via the uploader; we need the builder to register entries whose content is already present and skip upload. How much of the spooler path does that touch?
4. **Descriptor completeness** — can it be a faithful projection of the `CatalogEntries` the pipeline already assembles (chunked files, xattrs, hardlinks, symlinks, special files), plus dirtab/autocatalog boundaries?
5. **Commit granularity** — one descriptor per publish-set (coarse, fewest commits) vs per-package (matches current job model but reintroduces bursts)? This is the lever: coarse/serialized + the acquire-reply enrichment needs no other gateway change; per-package concurrency needs the commit-time `graft-head`. Which model do we want for bits?
6. **Acquire-reply enrichment** — confirm the newer upstream gateway's acquire reply field names/shape (`current_revision`, `current_root_hash`) and that the head is tracked authoritatively (in-process under `DB.WithLock`, seeded from stratum0), so the returned root never itself lags.
6. **Tooling/packaging** — if variant A, the prepub gains a runtime dependency on a cvmfs C++ tool; how is that built, versioned, and shipped alongside the prepub?
