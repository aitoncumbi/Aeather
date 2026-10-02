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
  S3 clients (SDKs, aws cli, rclone)          Web console, aether CLI
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
- Mapping engine errors to S3 error codes. Error bodies carry the request id
  (`<RequestId>`), as SDKs expect.
- Path-style addressing (`/bucket/key`). Virtual-hosted style
  (`bucket.s3.example.com`, with a configured base domain) is planned for v1.1.
- Answering the calls that clients and tools make on every bucket, for features
  that do not exist yet, the way S3 answers for a bucket that does not use
  them: `ListObjectVersions` lists every object as version `null`,
  `GetBucketVersioning` reports versioning never enabled, `GetObjectTagging`
  returns an empty tag set (the AWS CLI reads it when copying) and
  `GetBucketTagging` returns `NoSuchTagSet`. Requests that would *set* such
  features fail with `NotImplemented`.

Authentication uses `s3s`'s SigV4 verification, backed by access keys stored in
the metadata store. Authorization (which key can do what on which bucket) is an
`s3s::access::S3Access` implementation. Anonymous requests are rejected. A
caller with a valid key that addresses a bucket that does not exist gets
`NoSuchBucket` rather than `AccessDenied`, as in S3.

Health endpoints live under `/_aether/` on the S3 port. A leading underscore is
not valid in a bucket name, so they cannot collide with path-style requests.

### Admin API

A JSON API on its own port (`127.0.0.1:9001` by default): access keys, bucket
permissions, buckets with usage, objects, node status and activity. Requests use
HTTP Basic auth with the access key id and secret of the root key or an admin
key, or a console session (below). The web console and the `aether` CLI
(`aether key …`, `aether bucket …`, `aether status`) are clients of this API;
neither has a private backdoor.

| Method and path | Purpose |
|---|---|
| `GET /api/v1/status` | Version, uptime, counts, disk, storage settings, GC queue, scrub report, the S3 endpoint |
| `GET /api/v1/stats?range=1h\|24h\|7d` | Request rates by kind, latency percentiles, bandwidth, errors and object counts over time |
| `POST /api/v1/scrub` | Start a scrub now (`409` while one runs) |
| `GET, POST /api/v1/keys` | List keys; create one (the response is the only time its secret is shown) |
| `GET, PATCH, DELETE /api/v1/keys/{id}` | Show, update (name, admin, can_create_buckets, disabled), delete |
| `PUT, DELETE /api/v1/keys/{id}/buckets/{bucket}` | Set or remove a key's permissions on a bucket |
| `GET, POST /api/v1/buckets` | List buckets with usage; create one |
| `GET, DELETE /api/v1/buckets/{name}` | Usage and the keys with access; delete an empty bucket |
| `GET /api/v1/buckets/{name}/objects` | List objects: `prefix`, `delimiter`, `continuation`, `max_keys` |
| `GET, PUT, DELETE /api/v1/buckets/{name}/object?key=` | Object details (storage, parts, checksums, metadata); upload; delete |
| `GET /api/v1/buckets/{name}/object/download?key=` | The object's data, as an attachment |
| `POST /api/v1/buckets/{name}/object/share` | A presigned S3 GET URL, signed with the caller's key |
| `POST /api/v1/buckets/{name}/uploads`, `PUT …/uploads/{id}/parts/{n}`, `POST …/uploads/{id}/complete`, `DELETE …/uploads/{id}` | Multipart uploads |
| `GET /api/v1/session`, `POST /api/v1/login`, `POST /api/v1/logout` | Console sessions |
| `GET /api/v1/health` | Whether the node is ready, without credentials (for the sign-in page) |

Errors are `{"error": {"code": "...", "message": "..."}}` with a matching HTTP status.

### Web console

A Next.js app in `console/`, built as a static export and embedded in the
binary at compile time (`build.rs` includes `console/out`, or
`$AETHER_CONSOLE_DIR`). The admin listener serves it next to the API, so there
is one process to deploy and one origin: no CORS, and nothing to configure for
the console to find the API. A binary built without the console serves a page
explaining how to build it; `admin.console = false` turns it off.

- **Sign-in.** Any admin key signs in. `POST /api/v1/login` checks the secret
  and sets a session cookie (`HttpOnly`, `SameSite=Strict`, `Secure` with admin
  TLS). Sessions live in memory: they end on sign-out, after 15 minutes idle
  (timed console refreshes do not count), after 7 days, on restart, or as soon
  as the key is disabled, deleted or loses admin. Browsers keep no secret.
- **Objects go through the admin API**, not the S3 port: the console works
  wherever the admin port is reachable (including an SSH tunnel), needs no CORS
  rules on buckets, and signs nothing in the browser. Files up to 32 MiB are
  one `PUT`; larger ones are multipart uploads, three parts at a time, aborted
  if they fail or are cancelled.
- **Share links** are ordinary presigned S3 GET URLs (SigV4, up to 7 days),
  signed with the caller's key. They point at `s3.public_url` when set, or else
  at the host the console was opened on, with the S3 port.
- **Activity history** is sampled every 10 seconds into in-memory rings
  (10-second, 1-minute and 10-minute resolution, covering 1 hour, 1 day and 7
  days), so it starts empty after a restart. Prometheus remains the long-term
  record.

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
| `access_keys` | access key id | `AccessKeyRecord` (secret, roles, per-bucket permissions) |
| `bucket_usage` | bucket_id | object count and bytes, updated by every write |
| `system` | name | schema version and other node-level values |

Added in later milestones: `versions` (v2).

The schema version is checked at startup and older databases are migrated in place
(version 1 → 2 computed `bucket_usage` from existing objects).

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
- **Conditional writes are evaluated inside the transaction.** `If-Match` and
  `If-None-Match` (`*` or an ETag) are part of the `PutObject`, `CompleteUpload`
  and `CopyObject` commands, so two racing writers cannot both succeed. Deletes
  take an ETag, last-modified time and size to match (`DeleteObject`, and per key
  in `DeleteObjects`); deleting a key that does not exist succeeds whatever the
  condition, as in S3.
- redb allows one writer at a time with concurrent MVCC readers. Each commit is one
  fsync. redb is synchronous, so the engine calls it from `spawn_blocking`. One
  fsync'd commit per write caps small PUTs at roughly 1,500 per second on the
  benchmark machine ([BENCHMARKS.md](BENCHMARKS.md)); group commit, several
  waiting commands in one transaction, is the planned remedy.

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
single part. It also keeps the id of the upload that created it: a client whose
`CompleteMultipartUpload` response was lost retries after the upload is gone,
and gets the same success response as long as the key still holds that object.

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

### HTTP layer

Both listeners run on a small accept loop over hyper-util rather than a
framework server, to control connection behaviour:

- **TLS** with rustls (`ring` backend, so no C toolchain is needed to build).
  Certificates are reloaded when their files change; a broken renewal is logged
  and the previous certificate stays in use. HTTP/2 is negotiated with ALPN.
- **Timeouts.** Request headers must arrive within `http.header_timeout_secs`
  (slow-loris protection) and TLS handshakes within
  `http.handshake_timeout_secs`. There is no total request timeout, since
  multi-gigabyte transfers legitimately take long; instead an upload that sends
  nothing for `http.body_timeout_secs` fails with `RequestTimeout`.
- **Shutdown.** On SIGTERM the listeners stop accepting, in-flight requests get
  `http.shutdown_timeout_secs` to finish, then the remaining connections close.
- **Request ids.** Each request gets a 16-hex-digit id, returned as
  `x-amz-request-id` (S3) or `x-request-id` (admin) and attached to every log
  line of the request through a tracing span. The S3 permission check records
  the operation and access key for the access log and metrics.

### Observability

The `s3s` library logs every failed request at ERROR, including the body of
malformed XML. Client mistakes are not server errors and request bodies do not
belong in logs, so its logs are off unless `log.level` names it (e.g.
`info,s3s=debug`); the access log records every failure's status, and Aether
logs its own internal errors.

Prometheus metrics are served at `/metrics` on the admin listener (loopback by
default), without credentials. Request counters and latency histograms are
recorded as responses are sent; usage, disk space and background-work counters
are read from the engine at scrape time.

| Metric | Labels |
|---|---|
| `aether_http_requests_total` | `api`, `operation`, `status` |
| `aether_http_request_duration_seconds` | `api`, `operation` |
| `aether_received_bytes_total`, `aether_sent_bytes_total` | |
| `aether_bucket_objects`, `aether_bucket_bytes` | `bucket` |
| `aether_buckets`, `aether_blocks`, `aether_access_keys` | |
| `aether_disk_available_bytes`, `aether_disk_total_bytes` | |
| `aether_gc_deleted_blocks_total`, `aether_orphan_removed_blocks_total`, `aether_aborted_uploads_total` | |
| `aether_scrub_checked_blocks_total`, `aether_scrub_checked_bytes_total`, `aether_scrub_corrupt_blocks_total`, `aether_scrub_last_finished_timestamp_seconds` | |
| `aether_build_info`, `aether_uptime_seconds` | `version` |

### Scrubbing

A background pass re-reads every block of every object and compares its length
and CRC32C with the values recorded at write time, every `scrub.interval_secs`
(default 7 days) at up to `scrub.bytes_per_sec` (default 64 MiB/s). The time of
the last finished pass is stored node-locally, so restarts do not trigger
rescans. Before reporting a block, the scrubber re-reads the object record, so
objects deleted or overwritten during the pass are not reported. On a single
node damage can only be reported (error log, metric, `aether status`); with
replication (v3) the same pass will repair from a healthy copy.

## Security

- Every S3 request must be signed with SigV4 (anonymous access arrives with
  public-read buckets, later). Signature Version 2 is rejected by default: AWS
  deprecated it and it relies on HMAC-SHA1. `s3.allow_sig_v2` accepts it for
  clients that cannot be changed (boto3 presigns with SigV2 unless configured
  for `s3v4`).
- **Identities.** The root key comes from the config file and can do everything.
  Every other identity is an access key stored in metadata, created through the
  admin API. A key is either `admin` (everything, including the admin API) or has
  per-bucket permissions:
  - `read`: get and list objects
  - `write`: put, copy and delete objects; multipart uploads
  - `owner`: delete the bucket and, later, change its settings

  `HeadBucket` needs any permission; `ListParts` and `ListMultipartUploads` need
  read or write, so upload-only keys can resume uploads. `CopyObject` and
  `UploadPartCopy` also need read on the source bucket. `ListBuckets` shows only the
  buckets a key has some permission on.
- **Bucket creation.** Keys with `can_create_buckets` become owners of the buckets
  they create. A key without it that "creates" a bucket it can already use gets
  `BucketAlreadyOwnedByYou`, which applications that create their bucket at startup
  already handle.
- **Revocation is immediate.** Keys are cached in memory and the cache is reloaded
  before an admin change returns. Disabled and deleted keys fail with
  `InvalidAccessKeyId`. Permissions are by bucket name and are removed when the
  bucket is deleted, so a new bucket with the same name starts with no grants.
- Admin API secrets are compared in constant time.
- SigV4 needs the plaintext secret to verify signatures, so secrets cannot be hashed.
  In v1 they are protected by file permissions on the data directory. With
  encryption at rest (v2), they are encrypted under the master key.
- TLS is supported natively (rustls) or can be terminated by a reverse proxy. The
  admin API listens on loopback by default; exposing it without TLS logs a warning,
  and the console's sign-in page warns when it is opened over plain HTTP from
  another host.
- **Console sessions.** Writes authenticated by the session cookie must carry the
  `x-aether-console` header, which a cross-site form cannot send (CSRF).
  Requests from the console never get a Basic `WWW-Authenticate` challenge, so
  browsers do not show their own sign-in dialog. Failed sign-ins wait 250 ms.
- **Console pages** are served with a strict Content Security Policy
  (`default-src 'self'`, `frame-ancestors 'none'`), `X-Frame-Options: DENY` and
  `nosniff`. Downloads are always `application/octet-stream` attachments with
  `Content-Security-Policy: sandbox`, so a stored HTML object cannot run script
  in the console's origin.
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
