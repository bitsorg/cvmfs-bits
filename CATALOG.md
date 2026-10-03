# CVMFS Catalog: Schema, Generation, and Storage

A CVMFS catalog is a SQLite database that describes the namespace of a
repository — its files, directories, symbolic links, and their metadata — for
one scope of the directory tree. Every repository has at least one catalog
(the *root catalog*). Large or deeply nested trees are split into *nested
catalogs*, each covering a subtree, so that clients only fetch the catalogs
relevant to the paths they access.

This document describes the catalog schema, the binary encoding conventions,
how cvmfs-bits generates catalogs from a tar archive, and how the resulting
objects are stored in the Content-Addressable Store (CAS).

Only the default `prepub` publish path in gateway mode builds catalogs this
way. The local backend (`cvmfs_server publish`), the `ingest` path
(`cvmfs_server ingest`) and whole-build finalize (`cvmfs_swissknife ingestsql`)
leave catalog building to the CVMFS tools, and the `staged` path grafts a
catalog that the producer has already built.

---

## 1. Schema Version

cvmfs-bits creates catalogs at **schema 2.5, schema_revision 7**. The schema
version is stored in the `properties` table and is read by `cvmfs_receiver` to
select the correct SQL statement set. Earlier revisions (< 4) do not include
`bind_mountpoints`; `cvmfs_receiver` crashes if that table is absent when the
schema_revision indicates it should exist.

---

## 2. Tables

### 2.1 `catalog` — file-system entries

The core table. Each row is one file-system object (regular file, directory,
symbolic link, or special file).

| Column       | Type    | Description |
|--------------|---------|-------------|
| `md5path_1`  | INTEGER | Low 8 bytes of MD5(absolute path), interpreted as a signed 64-bit integer (little-endian). Primary lookup key. |
| `md5path_2`  | INTEGER | High 8 bytes of MD5(absolute path), same interpretation. Together with `md5path_1` forms the UNIQUE lookup key. |
| `parent_1`   | INTEGER | `md5path_1` of the parent directory. The repository root entry has `parent_1 = parent_2 = 0`; the root entry of a nested catalog points to its real parent directory. |
| `parent_2`   | INTEGER | `md5path_2` of the parent directory. |
| `hardlinks`  | INTEGER | Packed encoding: `(hardlink_group << 32) | link_count`. For normal (non-hardlinked) files: `(0 << 32) | 1 = 1`. |
| `hash`       | BLOB    | Raw bytes of the content hash. NULL for directories and symbolic links. For regular files the hash algorithm is encoded in `flags` bits 8–10. |
| `size`       | INTEGER | Uncompressed file size in bytes. For directories, the size from the tar header (placeholder root entries use 4096). |
| `mode`       | INTEGER | Unix file mode: type bits (`0o040000` dir, `0o120000` symlink, `0o100000` regular) OR'd with permission bits (including setuid/setgid/sticky). |
| `mtime`      | INTEGER | Modification time, Unix epoch seconds. |
| `mtimens`    | INTEGER | Nanosecond part of `mtime`. Currently written as 0. |
| `flags`      | INTEGER | Packed bit field (see §3). |
| `name`       | TEXT    | Base name of the entry (filename only, no path separators). Empty string for the root directory entry. |
| `symlink`    | TEXT    | Symlink target, or empty string for non-symlinks. |
| `uid`        | INTEGER | Owner user ID. |
| `gid`        | INTEGER | Owner group ID. |
| `xattr`      | BLOB    | Extended attributes serialized as a CVMFS binary blob (see §6). NULL when no extended attributes are present. Regular files always carry the synthetic attributes of §6, so their column is never NULL. |

**Indexes:**

```sql
CREATE UNIQUE INDEX idx_catalog_path ON catalog (md5path_1, md5path_2);
```

Lookups by path use `WHERE md5path_1 = ? AND md5path_2 = ?`. The raw path
string is never stored in this table; name resolution requires knowing the
full absolute path to compute its MD5.

**Path encoding:**

```
MD5Path(absPath) → (md5path_1, md5path_2)

absPath = ""          → root directory (repo root catalog only)
absPath = "/foo"      → top-level entry "foo"
absPath = "/foo/bar"  → entry "bar" inside directory "foo"
```

The 16-byte MD5 digest is split into two signed 64-bit integers using
little-endian byte order:

```
md5path_1 = int64(LittleEndian(digest[0:8]))
md5path_2 = int64(LittleEndian(digest[8:16]))
```

### 2.2 `chunks` — file chunks for large files

Files larger than the chunk size are split into pieces (*chunked files*):
fixed 6 MiB pieces by default, content-defined boundaries when the chunk
minimum, average and maximum are configured to differ. Each chunk is a separate CAS object. This table stores the
chunk map.

| Column      | Type    | Description |
|-------------|---------|-------------|
| `md5path_1` | INTEGER | Same as in `catalog` — identifies the owning file. |
| `md5path_2` | INTEGER | Same as in `catalog`. |
| `offset`    | INTEGER | Byte offset of this chunk in the uncompressed file. |
| `size`      | INTEGER | Uncompressed size of this chunk in bytes. |
| `hash`      | BLOB    | Raw bytes of the chunk's CAS key (SHA-1 of the zlib-compressed chunk). |

**Index:**

```sql
CREATE UNIQUE INDEX idx_chunks_path_offset ON chunks (md5path_1, md5path_2, offset);
```

When a file is chunked, `catalog.hash` holds the *bulk hash* (SHA-1 of the
full uncompressed content) and `FlagFileChunk` is set in `catalog.flags`.
The CVMFS client reads such a file from the chunks listed in this table; no
object is stored under the bulk hash.

### 2.3 `nested_catalogs` — nested catalog mount points

Each row records one child catalog that is grafted into the tree at the given
path.

| Column | Type    | Description |
|--------|---------|-------------|
| `path` | TEXT PK | Absolute mount path (e.g. `/atlas/24.0/run3`). |
| `sha1` | TEXT    | Plain 40-character lowercase hex SHA-1 of the *compressed* child catalog object. No suffix character. |
| `size` | INTEGER | Size in bytes of the child catalog's *uncompressed* SQLite file (the value `cvmfs_swissknife check` compares against). |

`cvmfs_receiver` joins this table with `bind_mountpoints` using a UNION ALL
when loading a catalog with `schema_revision >= 4`.

### 2.4 `bind_mountpoints` — bind-mount points

Required for schema_revision >= 4. Structure identical to `nested_catalogs`.
cvmfs-bits always creates this table empty; it is populated by the CVMFS server
infrastructure for bind-mount operations outside the scope of pre-publishing.

| Column | Type    | Description |
|--------|---------|-------------|
| `path` | TEXT PK | Absolute bind-mount path. |
| `sha1` | TEXT    | Compressed catalog hash (plain hex, no suffix). |
| `size` | INTEGER | Catalog size in bytes (uncompressed, as in `nested_catalogs`). |

### 2.5 `statistics` — entry counters

One row per counter. Used by `cvmfs_receiver` to update the repository-level
object count displayed by `cvmfs_server info`. Every catalog must have a row
for every counter name or the receiver crashes inside `Sql::LazyInit`.

| Column    | Type         | Description |
|-----------|--------------|-------------|
| `counter` | TEXT PK      | Counter name (see §5). |
| `value`   | INTEGER ≥ 0  | Current count. |

### 2.6 `properties` — catalog metadata

Key-value pairs.

| Key                | Value type | Description |
|--------------------|------------|-------------|
| `schema`           | TEXT       | Always `"2.5"`. |
| `schema_revision`  | TEXT       | Always `"7"` for catalogs created by cvmfs-bits. |
| `root_prefix`      | TEXT       | Absolute path of this catalog's root (e.g. `/atlas/24.0`); empty for a repository root catalog. cvmfs-bits always writes the row; native CVMFS root catalogs omit it, which `Open()` reads as empty. For the lease catalog the value depends on the commit mode (see §7.3). |
| `revision`         | TEXT       | Created as `0` and incremented by `Finalize()`, so a new catalog has revision `1`. |
| `last_modified`    | TEXT       | Unix timestamp (seconds) of the last `Finalize()` call. |
| `previous_revision`| TEXT       | Empty string; reserved for future use. |

---

## 3. The `flags` Column

The integer `flags` column encodes the entry type, content hash algorithm,
compression algorithm, and several boolean attributes. Bit layout (from LSB):

| Bits  | Width | Field | Values |
|-------|-------|-------|--------|
| 0     | 1     | `FlagDir`            | 1 = directory |
| 1     | 1     | `FlagDirNestedMount` | 1 = directory is a nested catalog mount point |
| 2     | 1     | `FlagFile`           | 1 = regular file (or special file) |
| 3     | 1     | `FlagLink`           | 1 = symbolic link |
| 4     | 1     | `FlagFileSpecial`    | 1 = special file (device, pipe, socket) — set together with `FlagFile` |
| 5     | 1     | `FlagDirNestedRoot`  | 1 = this entry is the root of a nested catalog |
| 6     | 1     | `FlagFileChunk`      | 1 = file content is split into chunks (see `chunks` table) |
| 7     | 1     | `FlagFileExternal`   | 1 = external file (content served by a separate CAS) |
| 8–10  | 3     | Hash algorithm       | CVMFS algorithm id − 1 — see table below |
| 11–13 | 3     | Compression algorithm| Raw `CompAlgo` enum — see table below |
| 14    | 1     | Bind mountpoint      | Reserved for CVMFS bind-mount infrastructure |
| 15    | 1     | `FlagHidden`         | 1 = hidden entry (defined, not set by cvmfs-bits) |
| 16    | 1     | Direct I/O           | Reserved |

**Hash algorithm encoding (bits 8–10):**

The stored value is the CVMFS algorithm id minus one (`shash::Algorithms`:
MD5 = 0, SHA-1 = 1, RIPEMD-160 = 2, SHAKE-128 = 3), so SHA-1 gives `0b000 = 0`,
leaving bits 8–10 at zero. To decode: `algorithm = ((flags >> 8) & 7) + 1`.

| CVMFS algorithm | Stored (bits 8–10) | Hash suffix |
|-----------------|--------------------|-------------|
| SHA-1           | 0                  | none        |
| RIPEMD-160      | 1                  | `-rmd160`   |
| SHAKE-128       | 2                  | `-shake128` |

cvmfs-bits writes SHA-1 for all content. The Go constants in
`pkg/cvmfscatalog` call ids 2 and 3 `HashSha256` and `HashRipeMD160`, and
`HashSuffix()` maps them to `-` and `~`; those names and suffixes do not match
CVMFS, but no content hash uses them (see §9 for the one place id 2 appears).

**Compression algorithm encoding (bits 11–13):**

| `CompAlgo` | Stored (bits 11–13) | Algorithm |
|------------|--------------------|-|
| 0 (CompZlib)  | 0               | zlib deflate |
| 1 (CompNone)  | 1               | No compression (verbatim) |

**Important:** `FlagXattr` (bit 17) is an *internal cvmfs-bits flag* used only
for in-memory statistics tracking. It is **never written to the SQLite `flags`
column**: it is masked out before every insert. Extended attribute presence is
determined exclusively by whether the `xattr` BLOB column is NULL. (CVMFS itself
uses bit 17 for `kFlagBundleTrigger`; because `FlagXattr` never reaches the
database, the two do not collide.)

---

## 4. Content Hash Conventions

### 4.1 Regular (non-chunked) files

The content pipeline compresses each file with zlib (default level 6) and
computes the SHA-1 of the *compressed* bytes:

```
CAS key = SHA-1(zlib(raw content))
```

This hash is stored in `catalog.hash` as raw bytes, and the object is placed
in CAS at `data/XY/hash[2:]` (where `XY` = first two hex characters).

### 4.2 Chunked files

Files above the chunk size are split into pieces (see §2.2). Each chunk is
independently compressed and hashed:

```
chunk CAS key   = SHA-1(zlib(chunk_bytes))    → stored in chunks.hash
file bulk hash  = SHA-1(raw_full_content)      → stored in catalog.hash
```

The bulk hash covers the *uncompressed* full-file content. No object is stored
under it; the client reads the file from its chunks.

### 4.3 Catalogs

Catalog objects use the same zlib + SHA-1 scheme as data objects, with a `C`
suffix character appended to the hash when constructing CAS paths:

```
compressed_catalog = zlib(raw_sqlite_bytes)
catalog CAS key    = SHA-1(compressed_catalog)
CAS path           = data/XY/hash[2:]C         (note the 'C' suffix)
```

The `sha1` column in `nested_catalogs` stores the 40-character lowercase hex
hash **without** the `C` suffix. The suffix is a content-type marker at the CAS
layer, not part of the hash identity.

**Journal mode:** cvmfs-bits uses SQLite `DELETE` journal mode (the default)
for new catalogs, not WAL mode. With WAL mode, the passive checkpoint on
`db.Close()` may not flush all pages back to the main `.db` file before
`Finalize()` reads it with `os.ReadFile`, producing a truncated catalog that
`cvmfs_receiver` rejects. DELETE mode ensures every committed transaction is
immediately written to the main file.

---

## 5. Statistics Counters

Each catalog maintains 24 counters split into `self_*` (entries in this
catalog only) and `subtree_*` (entries in all descendant nested catalogs, not
including this one). `cvmfs_receiver` reads these via
`SELECT value FROM statistics WHERE counter = :counter` and uses them to update
the repository-wide object count.

| Counter                   | Tracks |
|---------------------------|--------|
| `self_regular`            | Regular files in this catalog |
| `self_symlink`            | Symbolic links in this catalog |
| `self_special`            | Special files (devices, FIFOs, sockets) |
| `self_dir`                | Directories in this catalog |
| `self_nested`             | Nested catalog mount points |
| `self_chunked`            | Chunked files |
| `self_chunks`             | Total number of chunks |
| `self_file_size`          | Cumulative uncompressed size of all regular files, including chunked and external ones (bytes) |
| `self_chunked_size`       | Cumulative size of chunked files (bytes) |
| `self_xattr`              | Entries with extended attributes |
| `self_external`           | External files |
| `self_external_file_size` | Cumulative size of external files (bytes) |
| `subtree_*`               | Same as `self_*` but accumulated across all nested catalogs |

cvmfs-bits accumulates `self_*` changes in an in-memory `Statistics` delta
during `Upsert` / `BatchUpsert` / `BatchInsert` / `Remove` calls, then flushes the delta to
the database in `Finalize()` using:

```sql
UPDATE statistics SET value = value + ? WHERE counter = ?
```

The delta is applied only after the wrapping transaction commits, so a rollback
never permanently corrupts the in-memory delta. `subtree_*` values are
propagated from child to parent during `BuildSubtree` finalization.

---

## 6. Extended Attributes (xattr BLOB)

Extended attributes are serialized as a binary blob stored in the `xattr`
column. The format is defined by `pkg/cvmfsxattr` (all integers
little-endian):

```
[4 bytes: number of entries, uint32]
for each entry (keys sorted):
  [2 bytes: key length, uint16]
  [4 bytes: value length, uint32]
  [key bytes: UTF-8 key string, no terminator]
  [value bytes: raw value]
```

A NULL `xattr` column means no extended attributes. For non-NULL blobs,
`FlagXattr` is tracked in the in-memory delta (for `self_xattr` statistics)
but never written to the `flags` column.

cvmfs-bits merges two sources of extended attributes into each file entry:

- **Tar xattrs** from PAX extended headers in the source tar archive
  (`SCHILY.xattr.<name>` records, any namespace).
- **Synthetic xattrs** injected by `cvmfscatalog.SyntheticAttrs()` for every
  regular file (they replace a tar xattr of the same name):
  - `user.cvmfs.hash` — hex content hash (SHA-1, so no suffix).
  - `user.cvmfs.compression` — `"zlib"` or `"none"`.
  - `user.cvmfs.chunk_list` — chunked files only; one line per chunk,
    `offset:uncompressed_size:hex_hash`, lines separated by `\n`.

---

## 7. Catalog Generation: the BuildSubtree Path

cvmfs-bits does not download or modify existing repository catalogs. Instead,
it builds a *fresh subtree catalog* covering exactly the lease path and any
catalog split points within it. `cvmfs_receiver` grafts this subtree into the
existing repository tree at commit time.

### 7.1 Input

`BuildSubtree(ctx, SubtreeConfig, []Entry)` receives a flat slice of
`cvmfscatalog.Entry` values from the pipeline, with `Hash` populated from the
compress stage and `Chunks` populated for chunked files. `FullPath` is
tar-relative (e.g. `usr/lib/foo.so`, or `.` for the tar root); `BuildSubtree`
prefixes the lease path to make it absolute (e.g. `/atlas/24.0/usr/lib/foo.so`).

### 7.2 Catalog split planning

Split points are determined by:

1. `.cvmfscatalog` marker files in the entry list — any directory containing
   such a file becomes a catalog boundary.
2. Glob rules from a `.cvmfsdirtab` file found in the tar payload — the CVMFS
   configuration file that specifies which subdirectories get their own
   catalog. Rules apply to directories in the payload.

Only paths strictly below the lease path are considered. The function
`planSplits` returns the sorted list of absolute split paths.

The lease directory and every split point become nested catalog roots, so
`BuildSubtree` adds a `.cvmfscatalog` marker entry to any of them that lacks
one. The marker is an empty file; when one is added, the caller also stores the
empty-file object in the CAS (`SubtreeResult.NeedsMarkerObject`).

### 7.3 Catalog creation

One `Catalog` object is created per split point using `Create(dbPath, rootPrefix)`.
The root catalog for the lease path is created separately. All catalogs start
with the schema described in §2, a placeholder root directory entry, and 24
zero-initialized statistics counters. The placeholder is replaced by the real
directory entry from the payload when there is one; for the lease path a
directory entry is synthesized if the tar has none.

Split catalogs keep their own path as `root_prefix`. The lease catalog keeps
its path as `root_prefix` only for the direct-graft commit
(`--gateway-direct-graft`, the default); for the standard commit it is cleared
to `""`, because the receiver then loads the catalog with an empty mount
point.

### 7.4 Entry routing

Each entry is assigned to the deepest catalog whose root path is a strict prefix
of the entry's `FullPath`. Entries under a split path go to that split's catalog;
all other entries go to the lease root catalog.

A split point's own directory entry is written to its parent catalog (where it
is the mount point) and also, as the root entry, to the split catalog itself.

Entries are accumulated in per-catalog batches and written using `BatchInsert`,
which wraps all inserts for a given catalog in a single SQLite transaction
(every catalog is new, so no existence check is needed). Deletion entries
(where `IsDelete == true`) flush the pending batch first, then call `Remove`.

### 7.5 Finalization (deepest-first)

Split catalogs are finalized in descending path-length order (children before
parents). For each split catalog:

1. `Finalize(tempDir)` increments the revision, flushes statistics, compresses
   the SQLite file, hashes the compressed bytes, and writes the result to
   `tempDir/data/XY/<rest of hash>C`.
2. The parent catalog records the child via `AddNestedMount(path, hash, size)`
   with the child's uncompressed size; this inserts into `nested_catalogs` and
   sets `FlagDirNestedMount` on the mount-point directory entry.
3. The child's statistics delta is propagated into the parent's `SubtreeRegular`,
   `SubtreeSymlink`, etc. fields.

The lease root catalog is finalized last. Its hash is returned as
`SubtreeResult.CatalogHashSuffixed` (= `hash + "C"`).

---

## 8. CAS Storage Layout

Catalog objects follow the same CAS layout as data objects:

```
<cas_root>/data/<first_two_hex_chars>/<remaining_hex><suffix>
```

For example, a catalog with hash `a3b4c5...` is stored at:

```
data/a3/b4c5...C
```

The `C` suffix identifies the object as a compressed catalog. Data objects
written by cvmfs-bits are SHA-1 and have no suffix. (In CVMFS, RIPEMD-160 and
SHAKE-128 hashes carry the algorithm suffixes `-rmd160` and `-shake128`; see
§3.)

The object is the raw zlib-compressed SQLite database file. `cvmfs_receiver`
fetches this object, decompresses it, and opens it as SQLite to perform the
graft operation.

---

## 9. Known Limitations and Gaps

**`previous_revision` not populated.** The `previous_revision` property is
always stored as an empty string. cvmfs-bits builds fresh subtree catalogs
without downloading the existing catalog, so the previous revision is not
known at catalog creation time.

**No VACUUM.** `Catalog.Finalize()` does not call `VACUUM` before reading the
raw SQLite bytes. The SQLite file may contain unused pages if entries were
deleted or replaced. For typical publish workloads this has negligible effect
on catalog size, but long-lived incremental catalogs could benefit from
periodic compaction.

**Placeholder root entry hash bits.** `Create()` writes its placeholder root
directory entry with algorithm id 2 (named `HashSha256` in the code), which sets
bits 8–10 to `1` — RIPEMD-160 in CVMFS terms — while all content uses SHA-1.
The entry has no content hash, so this does not affect correctness, and
`BuildSubtree` replaces the placeholder with the real directory entry whenever
the payload has one. It can still be confusing when inspecting the raw `flags`
column.
