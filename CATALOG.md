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
| `parent_1`   | INTEGER | `md5path_1` of the parent directory. Root entries have `parent_1 = parent_2 = 0`. |
| `parent_2`   | INTEGER | `md5path_2` of the parent directory. |
| `hardlinks`  | INTEGER | Packed encoding: `(hardlink_group << 32) | link_count`. For normal (non-hardlinked) files: `(0 << 32) | 1 = 1`. |
| `hash`       | BLOB    | Raw bytes of the content hash. NULL for directories and symbolic links. For regular files the hash algorithm is encoded in `flags` bits 8–10. |
| `size`       | INTEGER | Uncompressed file size in bytes. For directories, conventionally 4096. |
| `mode`       | INTEGER | Unix file mode: type bits (`0o040000` dir, `0o120000` symlink, `0o100000` regular) OR'd with permission bits (including setuid/setgid/sticky). |
| `mtime`      | INTEGER | Modification time, Unix epoch seconds. |
| `mtimens`    | INTEGER | Nanosecond part of `mtime`. Currently written as 0. |
| `flags`      | INTEGER | Packed bit field (see §3). |
| `name`       | TEXT    | Base name of the entry (filename only, no path separators). Empty string for the root directory entry. |
| `symlink`    | TEXT    | Symlink target, or empty string for non-symlinks. |
| `uid`        | INTEGER | Owner user ID. |
| `gid`        | INTEGER | Owner group ID. |
| `xattr`      | BLOB    | Extended attributes serialized as a CVMFS TLV binary blob (see §6). NULL when no extended attributes are present. |

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

Files larger than the chunk threshold are split into fixed-size pieces
(*chunked files*). Each chunk is a separate CAS object. This table stores the
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
The CVMFS client fetches all chunks for such a file, decompresses each, and
concatenates them; it verifies the concatenated content against the bulk hash.

### 2.3 `nested_catalogs` — nested catalog mount points

Each row records one child catalog that is grafted into the tree at the given
path.

| Column | Type    | Description |
|--------|---------|-------------|
| `path` | TEXT PK | Absolute mount path (e.g. `/atlas/24.0/run3`). |
| `sha1` | TEXT    | Plain 40-character lowercase hex SHA-1 of the *compressed* child catalog object. No suffix character. |
| `size` | INTEGER | Size in bytes of the compressed child catalog object in CAS. |

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
| `size` | INTEGER | Compressed catalog size in bytes. |

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
| `root_prefix`      | TEXT       | Absolute path of this catalog's root (e.g. `/atlas/24.0`). Empty string for the repository root catalog. Written only when `root_prefix != ""`; absent in native root catalogs. |
| `revision`         | TEXT       | Monotonically increasing integer, incremented in `Finalize()`. |
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
| 8–10  | 3     | Hash algorithm       | `(HashAlgo - 1)` — see table below |
| 11–13 | 3     | Compression algorithm| Raw `CompAlgo` enum — see table below |
| 14    | 1     | Bind mountpoint      | Reserved for CVMFS bind-mount infrastructure |
| 15    | 1     | `FlagHidden`         | 1 = hidden entry (used for `.shares/` secret directories) |
| 16    | 1     | Direct I/O           | Reserved |

**Hash algorithm encoding (bits 8–10):**

The stored value is `HashAlgo - 1`, so SHA-1 (the default) gives `0b000 = 0`,
leaving bits 8–10 at zero. To decode: `HashAlgo = ((flags >> 8) & 7) + 1`.

| `HashAlgo` | Stored (bits 8–10) | Algorithm | CAS path suffix |
|------------|-------------------|-----------|-----------------|
| 1 (SHA-1)  | 0                 | SHA-1     | `""` (none)     |
| 2 (SHA-256)| 1                 | SHA-256   | `"-"`           |
| 3 (RipeMD-160)| 2              | RipeMD-160| `"~"`           |

**Compression algorithm encoding (bits 11–13):**

| `CompAlgo` | Stored (bits 11–13) | Algorithm |
|------------|--------------------|-|
| 0 (CompZlib)  | 0               | zlib deflate |
| 1 (CompNone)  | 1               | No compression (verbatim) |

**Important:** `FlagXattr` (bit 17) is an *internal cvmfs-bits flag* used only
for in-memory statistics tracking. It is **never written to the SQLite `flags`
column**. Extended attribute presence is determined exclusively by whether the
`xattr` BLOB column is NULL. Bit 17 is safely above all known CVMFS flag bits.

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

Files above the chunk threshold are split into fixed-size pieces. Each chunk
is independently compressed and hashed:

```
chunk CAS key   = SHA-1(zlib(chunk_bytes))    → stored in chunks.hash
file bulk hash  = SHA-1(raw_full_content)      → stored in catalog.hash
```

The bulk hash covers the *uncompressed* full-file content. The CVMFS client
downloads all chunks, decompresses each, concatenates them, and verifies the
result against the bulk hash.

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
catalog only) and `subtree_*` (entries in this catalog and all descendant
nested catalogs). `cvmfs_receiver` reads these via
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
| `self_file_size`          | Cumulative uncompressed file size (bytes) |
| `self_chunked_size`       | Cumulative size of chunked files (bytes) |
| `self_xattr`              | Entries with extended attributes |
| `self_external`           | External files |
| `self_external_file_size` | Cumulative size of external files (bytes) |
| `subtree_*`               | Same as `self_*` but accumulated across all nested catalogs |

cvmfs-bits accumulates `self_*` changes in an in-memory `Statistics` delta
during `Upsert` / `BatchUpsert` / `Remove` calls, then flushes the delta to
the database in `Finalize()` using:

```sql
UPDATE statistics SET value = value + ? WHERE counter = ?
```

The delta is applied only after the wrapping transaction commits, so a rollback
never permanently corrupts the in-memory delta. `subtree_*` values are
propagated from child to parent during `BuildSubtree` finalization.

---

## 6. Extended Attributes (xattr BLOB)

Extended attributes are serialized as a CVMFS binary TLV (type-length-value)
blob stored in the `xattr` column. The format is defined by `pkg/cvmfsxattr`:

```
[2 bytes: number of entries, big-endian uint16]
for each entry:
  [2 bytes: key length, big-endian uint16]
  [key bytes: UTF-8 key string]
  [4 bytes: value length, big-endian uint32]
  [value bytes: raw value]
```

A NULL `xattr` column means no extended attributes. For non-NULL blobs,
`FlagXattr` is tracked in the in-memory delta (for `self_xattr` statistics)
but never written to the `flags` column.

cvmfs-bits merges two sources of extended attributes into each file entry:

- **User xattrs** from PAX extended headers in the source tar archive
  (key prefix `user.`).
- **Synthetic xattrs** injected by `cvmfscatalog.SyntheticAttrs()`:
  - `user.cvmfs.hash` — hex hash with algorithm suffix (e.g. `abc123...` for
    SHA-1, `abc123...-` for SHA-256, `abc123...~` for RipeMD-160).
  - `user.cvmfs.compression` — `"zlib"` or `"none"`.
  - `user.cvmfs.chunk_list` — one line per chunk:
    `offset:uncompressed_size:hex_hash\n`.

---

## 7. Catalog Generation: the BuildSubtree Path

cvmfs-bits does not download or modify existing repository catalogs. Instead,
it builds a *fresh subtree catalog* covering exactly the lease path and any
catalog split points within it. `cvmfs_receiver` grafts this subtree into the
existing repository tree at commit time.

### 7.1 Input

`BuildSubtree(ctx, SubtreeConfig, []Entry)` receives a flat slice of
`cvmfscatalog.Entry` values from the pipeline. Each entry has its `FullPath`
set to the absolute CVMFS path, `Hash` populated from the compress stage, and
`Chunks` populated for chunked files.

### 7.2 Catalog split planning

Split points are determined by:

1. `.cvmfscatalog` marker files in the entry list — any directory containing
   such a file becomes a catalog boundary.
2. Dirtab glob rules from `.cvmfsdirtab` — the CVMFS configuration file that
   specifies which subdirectories should have their own catalog.

Only paths under the lease boundary are considered. The function `planSplits`
returns the sorted list of absolute split paths.

### 7.3 Catalog creation

One `Catalog` object is created per split point using `Create(dbPath, rootPrefix)`.
The root catalog for the lease path is created separately. All catalogs start
with the schema described in §2, a root directory entry, and 24 zero-initialized
statistics counters.

### 7.4 Entry routing

Each entry is assigned to the deepest catalog whose root path is a strict prefix
of the entry's `FullPath`. Entries under a split path go to that split's catalog;
all other entries go to the lease root catalog.

Entries are accumulated in per-catalog batches and written using `BatchUpsert`,
which wraps all inserts for a given catalog in a single SQLite transaction.
Deletion entries (where `IsDelete == true`) flush the pending batch first, then
call `Remove`.

### 7.5 Finalization (deepest-first)

Split catalogs are finalized in descending path-length order (children before
parents). For each split catalog:

1. `Finalize(tempDir)` increments the revision, flushes statistics, compresses
   the SQLite file, hashes the compressed bytes, and writes the result to
   `tempDir/data/XY/hashC`.
2. The parent catalog records the child via `AddNestedMount(path, hash, size)`,
   which inserts into `nested_catalogs` and sets `FlagDirNestedMount` on the
   mount-point directory entry.
3. The child's statistics delta is propagated into the parent's `SubtreeRegular`,
   `SubtreeSymlink`, etc. fields.

The lease root catalog is finalized last. Its hash is returned as
`SubtreeResult.CatalogHashSuffixed` (= `hash + "C"`).

### 7.6 Catalog chain model

At present, `BuildSubtree` uses a single-element chain (just the lease root
catalog). The chain loop exists for forward compatibility with a future
multi-level chain path, but that branch never fires in the current
implementation.

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
have no suffix (SHA-1), `-` suffix (SHA-256), or `~` suffix (RipeMD-160).

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

**Root catalog hash algorithm mismatch.** The root directory entry is created
with `HashAlgo: HashSha256` (for consistency with the receiver's
`GraftNestedCatalog` validation), while all data objects use SHA-1. This is
intentional: the root entry has no data content, only directory metadata, so
the hash algorithm field is irrelevant for the root entry's correctness.
However, it may cause confusion when inspecting the raw flags column.
