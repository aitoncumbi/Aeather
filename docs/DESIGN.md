# Aether design

Aether is an S3-compatible object store written in Rust, for teams that run their
own servers or containers and need object storage for internal applications.

This document records the architecture and the decisions behind it. The phased
plan lives in [ROADMAP.md](ROADMAP.md).

## Goals

- **S3-compatible.** Unmodified AWS SDKs, the AWS CLI and rclone work against it.
- **Production-grade on one node.** Crash-safe writes, checksums, metrics, sane defaults.
- **Any object size.** From a 1 KB document to multi-terabyte files.
- **Cluster without a rewrite.** A single node is a cluster of one. Clustering adds
  nodes; it does not replace the storage or metadata code.
- **Simple to operate.** One binary, one config file, no external database.

## Non-goals

These are out of scope unless the roadmap says otherwise:

- AWS IAM policy language, STS, temporary credentials
- S3 Select, static website hosting, BitTorrent, S3 Inventory
- Storage classes and tiering to other clouds
- Public multi-tenant SaaS (accounts, billing)

## Decisions

| Decision | Choice | Why |
|---|---|---|
| Data API | S3 protocol | Every SDK and tool already speaks it. |
| Management API | JSON over HTTP, separate port | The console and automation need things S3 doesn't model (keys, permissions, stats). |
| S3 protocol layer | [`s3s`](https://crates.io/crates/s3s) on hyper/axum | Handles routing, XML, SigV4 (header, presigned, aws-chunked with trailers) and errors, so we write storage logic, not protocol parsing. |
| Metadata | [`redb`](https://crates.io/crates/redb), embedded | Sorted key-value store with ACID transactions. S3 listing is a range scan over sorted keys. |
| Metadata in a cluster | Raft ([`openraft`](https://crates.io/crates/openraft)) over the same tables | Strong consistency without an external database. |
| Object data | Immutable blocks on local disk | Crash-safe, easy to replicate and garbage-collect. |

## Architecture

```
  S3 clients (SDKs, aws cli, rclone)           Web console (v1.1)
                 │                                     │
        ┌────────▼────────┐                   ┌────────▼────────┐
        │   S3 gateway    │  s3s on axum      │    Admin API    │  JSON, axum
        └────────┬────────┘                   └────────┬────────┘
                 └─────────────────┬───────────────────┘
                          ┌────────▼────────┐
                          │     Engine      │  object semantics, listing, GC
                          └───┬─────────┬───┘
                 ┌────────────▼──┐   ┌──▼────────────┐
                 │   Metadata    │   │  Block store  │
                 │    (redb)     │   │  (local disk) │
                 └───────────────┘   └───────────────┘
```

### S3 gateway

`s3s` turns HTTP requests into typed operation inputs and turns our outputs and
errors back into S3 XML responses. Our implementation of the `s3s::S3` trait maps
those inputs onto engine calls. It owns everything that is S3-specific and not
storage-specific:

- Verifying `Content-MD5`, `x-amz-checksum-*` headers and trailing checksums
  (current AWS SDKs send CRC32 checksums in aws-chunked trailers by default).
- Mapping engine errors to S3 error codes.
- Path-style addressing (`/bucket/key`) by default. Virtual-hosted style
  (`bucket.s3.example.com`) when a base domain is configured.

Authentication uses `s3s`'s SigV4 verification, backed by access keys stored in
the metadata store. Authorization (which key can do what on which bucket) is an
`s3s::access::S3Access` implementation. Anonymous requests are rejected.

Health endpoints live under `/_aether/` on the S3 port. A leading underscore is
not valid in a bucket name, so they cannot collide with path-style requests.

### Admin API

A JSON API on its own port: access keys, bucket permissions, usage stats, node
status. The web console is a client of this API and has no private backdoor.

### Engine

The engine implements object semantics independently of HTTP and XML:
put/get/head/delete/copy, listing, multipart uploads, conditional writes and
garbage collection. It is the only component that coordinates metadata and blocks,
so the write ordering below is enforced in exactly one place.

### Metadata store

All metadata lives in one redb database (`<data_dir>/meta.redb`).

| Table | Key | Value |
|---|---|---|
| `buckets` | bucket name | `BucketRecord` (id, created_at, settings) |
| `objects` | bucket_id (16 B) ‖ object key bytes | `ObjectRecord` |
| `blocks` | block_id (16 B) | reference count |
| `gc_queue` | due_at (u64 BE) ‖ block_id | – |
| `uploads` | bucket_id ‖ object key ‖ 0x00 ‖ upload_id | `UploadRecord` |
| `parts` | upload_id ‖ part number (u32 BE) | `PartRecord` |
| `system` | name | schema version and other node-level values |

Added in later milestones: `access_keys`, `versions` (v2).

Object keys may not contain NUL: S3's XML responses cannot carry it, and the
`uploads` key uses it as a separator so uploads sort by key, then by start time
(upload ids are UUIDv7, which are time-ordered).

Notes:

- **Object keys are prefixed with a bucket id, not the bucket name.** The fixed-size
  prefix keeps each bucket's objects in one contiguous, byte-ordered range. S3 lists
  keys in UTF-8 binary order, which is exactly redb's order. Deleting and re-creating
  a bucket with the same name can never expose stale entries.
- **Values are MessagePack with named fields, behind a one-byte format version.**
  New fields are added with `#[serde(default)]`, with no table migration.
- **All writes are commands.** Every mutation is a `meta::Command` value
  (`CreateBucket`, `PutObject`, `DeleteObject`, ...) applied by a single function in
  one write transaction. Commands carry everything non-deterministic (timestamps,
  generated ids), so applying the same command twice on two machines gives the same
  result. This is the seam where Raft plugs in for clustering.
- **Conditional writes are evaluated inside the transaction.** `If-None-Match: *`
  and `If-Match` are part of the `PutObject` command, so two racing writers cannot
  both succeed.
- redb allows one writer at a time with concurrent MVCC readers. Each commit is one
  fsync. redb is synchronous, so the engine calls it from `spawn_blocking`.

`ObjectRecord` holds: version id, size, ETag, last-modified, content headers
(type, encoding, disposition, language, cache-control, expires), user metadata,
client-supplied checksums, and the data location, which is either inline bytes or
a list of block references (`id`, `len`, `crc32c`).

### Block store

Object data lives in immutable files under `<data_dir>/blocks/<aa>/<bb>/<block_id>`.

- **Blocks.** A single upload is split into blocks of at most `block_size`
  (default 64 MiB). Blocks are never modified after they are written.
- **Inline small objects.** Objects up to `inline_threshold` (default 4 KiB) are
  stored inside the metadata record: no file, and one fsync instead of three.
- **Atomic block writes.** Each block is written to `<data_dir>/tmp/`, fsynced,
  renamed into place, and its directory fsynced. A file under `blocks/` is always
  complete.
- **Checksums.** Each block's CRC32C is stored in the object record so a background
  scrubber can detect bit rot. The object's MD5 is its ETag (single-part uploads).

### Write path

1. Check the bucket exists and the preconditions hold (fail fast).
2. Stream the body into blocks, computing MD5, the per-block CRC32C and any
   checksums the client asked for. Nothing is held in memory beyond a write buffer.
3. Verify the client's checksums. On mismatch, delete the new blocks and return
   `BadDigest`.
4. Apply one `PutObject` command: re-check preconditions, write the object record,
   add the new blocks to the `blocks` index, and release the replaced object's blocks.
5. Respond.

If the process dies before step 4, the new blocks are not in the `blocks` index and
the orphan sweeper deletes them. After step 4 the object is durable.

### Read path

Read the object record from a snapshot, then stream only the blocks that overlap the
requested byte range. Byte ranges map to `(block, offset)` without reading earlier
blocks.

### Delete and garbage collection

Deleting or overwriting an object decrements its blocks' reference counts. Blocks
that reach zero go into `gc_queue` with a due time of now + `gc_grace` (default
15 minutes), so downloads already in progress can finish. A background task deletes
due blocks.

The orphan sweeper deletes block files that have no `blocks` entry, are older than
the grace period, and are not being written right now. It runs at startup and
periodically.

### Multipart uploads

Each part is staged and committed like a small object: its blocks are written and
fsynced, then a `PutPart` command stores a `PartRecord` and takes a reference on
them. Parts are never inlined. Re-uploading a part number releases the old part's
blocks.

Completing an upload writes only metadata. The engine validates the requested part
list (ascending, ETags match, every part but the last at least 5 MiB), then a
`CompleteUpload` command creates the object from the parts' blocks. The object
takes over the parts' block references, so no data is copied. Parts that were
uploaded but not listed are released. The command carries the block list it was
built from and fails with a conflict if a part was replaced in the meantime, so a
completed object can never point at released blocks.

The object's ETag is the MD5 of the parts' MD5s followed by `-<part count>`, as in
S3, and the object keeps each part's size so `GetObject?partNumber=N` can serve a
single part.

**Checksums.** When an upload is created with a checksum algorithm, every part is
hashed with it, whether or not the client sends a value. On completion the
object's checksum is derived from the parts' checksums:

- `COMPOSITE` (default except for CRC64NVME): the checksum of the concatenated part
  checksums, with a `-<part count>` suffix. SDKs do not validate these on download.
- `FULL_OBJECT` (CRC32, CRC32C, CRC64NVME): the part CRCs combined mathematically
  into the CRC of the whole object, which SDKs do validate on download.

**Abandoned uploads** are aborted after `gc.abandoned_upload_secs` (default 7 days),
and deleting a bucket aborts its uploads.

### Copies

`CopyObject` writes a new object record that points at the source's blocks and
increments their reference counts, so a copy of any size is instant and uses no
space. The command checks that the source is still the version that was read; if it
changed, the gateway retries. References are added before the destination's old
blocks are released, which makes copying an object onto itself (to change its
metadata) safe.

`UploadPartCopy` copies bytes into new blocks, because the requested range rarely
lines up with block boundaries.

### Listing

`ListObjects` (v1 and v2) is a range scan over `objects`, starting at the largest of
the prefix, `start-after`/marker and the continuation token. With a delimiter, when
a common prefix is found the scan jumps past every key under it instead of reading
them. Continuation tokens are opaque (the next start key, base64url-encoded).
`max-keys` is capped at 1000.

### Consistency

On a single node, reads see every write that has returned. A PUT replies only after
its metadata commit, and every read uses the latest committed snapshot. Concurrent
PUTs to the same key: the last commit wins. Conditional writes let applications opt
into compare-and-swap.

## Security

- Every S3 request must be signed with SigV4 (anonymous access arrives with
  public-read buckets, later). Signature Version 2 is rejected: AWS deprecated it
  and it relies on HMAC-SHA1.
- Each application gets its own access key, limited to specific buckets
  (read, write or admin).
- SigV4 needs the plaintext secret to verify signatures, so secrets cannot be hashed.
  In v1 they are protected by file permissions on the data directory. With
  encryption at rest (v2), they are encrypted under the master key.
- TLS is supported natively (rustls) or can be terminated by a reverse proxy.
- `s3s` is not a full security boundary: we set body and object size limits,
  timeouts and listing limits ourselves.

## Planned designs

### Versioning (v2)

Each `ObjectRecord` already carries a version id. Versioning adds a `versions`
table keyed by bucket_id ‖ key ‖ inverted version time, plus delete markers.
`objects` stays the index of latest versions, so plain listings stay fast.
Unversioned buckets report the version id `null`, as S3 does.

### Encryption at rest (v2)

SSE-S3 style: each object gets a random data key, encrypted with a master key from
the config (later from an external KMS). Data is encrypted in fixed-size frames
(e.g. 64 KiB, AES-256-GCM) so a range read decrypts only the frames it needs.
SSE-C (customer-provided keys) uses the same framing.

### Clustering (v3)

- **Metadata.** The redb tables become the state machine of a Raft group
  (openraft). Gateways submit `meta::Command`s to the leader. If one group becomes
  the bottleneck, metadata is later sharded by bucket.
- **Blocks.** Placed on nodes by rendezvous hashing of the block id over the cluster
  layout, replicated 3 ways by default. Layout changes trigger rebalancing. Erasure
  coding comes later to reduce storage overhead.
- **Gateways** stay stateless, so any node can serve any request.

## Configuration

A TOML file (`aether.toml` by default, `--config` to override) with environment
variable overrides (`AETHER_` prefix, `__` between levels, e.g.
`AETHER_S3__LISTEN=0.0.0.0:9000`). See `aether.example.toml`.
