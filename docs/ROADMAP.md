# Aether roadmap

Architecture and rationale are in [DESIGN.md](DESIGN.md).

## v1: production-ready single node

The bar: an internal team points an unmodified AWS SDK at Aether, and it works for
anything from a 1 KB document to a 100 GB file.

### M1. Storage core
- [x] redb metadata store: buckets, objects, block index, GC queue, schema version
- [x] Block store: streaming writes split into blocks, tmp + fsync + rename, CRC32C per block
- [x] Inline storage for small objects
- [x] Engine: create/delete/list buckets; put/get/head/delete objects; byte ranges
- [x] Listing with prefix, delimiter and pagination
- [x] Conditional writes and deletes (`If-Match`, `If-None-Match`) checked inside the commit
- [x] Background GC with a grace period, orphan sweeper

### M2. S3 gateway
- [x] `s3s` on axum, replacing the actix-web prototype
- [x] Buckets: CreateBucket, DeleteBucket (empty only), HeadBucket, ListBuckets, GetBucketLocation
- [x] Objects: PutObject, GetObject, HeadObject, DeleteObject, DeleteObjects
- [x] ListObjects and ListObjectsV2
- [x] Range GET, conditional GET (`If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`)
- [x] Content-MD5 and `x-amz-checksum-*` verification, including aws-chunked trailers
- [x] SigV4 auth with a root key from config
- [x] Liveness and readiness endpoints
- [x] End-to-end tests with the Rust AWS SDK

### M3. Large objects and copies
- [x] Multipart upload: Create, UploadPart, Complete, Abort, ListParts, ListMultipartUploads
- [x] UploadPartCopy
- [x] Automatic cleanup of abandoned multipart uploads
- [x] CopyObject (shares blocks by reference count; no data copy)
- [x] GetObject/HeadObject `partNumber`
- [x] Multipart checksums: `COMPOSITE` and `FULL_OBJECT` (CRCs combined across parts)

### M4. Access control and admin API
- [x] Access keys stored in metadata, per-bucket permissions (read, write, owner)
- [x] Presigned URLs (verified by `s3s`; covered by tests)
- [x] Admin API: keys, buckets, permissions, usage stats
- [x] `aether key`, `aether bucket` and `aether status` CLI commands (admin API clients)
- [x] Import existing access keys (migrating from another S3 store)

### M5. Operations
- [x] TOML config with environment overrides
- [x] Native TLS (rustls), with certificate reload on renewal
- [x] Prometheus metrics: requests, latency, bytes, usage, GC, scrubbing, disk space
- [x] JSON logs option, access log
- [x] Request ids in logs and responses
- [x] Graceful shutdown with a deadline for in-flight requests
- [x] Header, TLS handshake and upload stall timeouts
- [x] Docker image (distroless, non-root, health check) and compose file
- [x] Background scrubber that verifies block CRC32Cs

### M6. Compatibility
- [x] CI runs the AWS CLI, boto3 and rclone against a live server (`compat/clients`)
- [x] A subset of Ceph's [s3-tests](https://github.com/ceph/s3-tests) in CI, tracked as a pass list
  (`compat/s3-tests`, 239 tests)
- [x] Fixes from both: ListObjectVersions and GetBucketVersioning for unversioned buckets,
  conditional deletes, `If-Match: *` and `If-None-Match: <etag>` on writes, retried
  CompleteMultipartUpload, ListBuckets paging, NoSuchBucket for missing buckets, empty
  object tag sets for copy tools, and more
- [x] Optional SigV2 (`s3.allow_sig_v2`) for legacy clients
- [x] Benchmarks (small-object ops/s, large-object throughput): [BENCHMARKS.md](BENCHMARKS.md)

## v1.1: console
- [x] Web console on the admin API, embedded in the binary: overview with usage and
  activity charts, buckets, object browser (upload, download, presigned share links),
  keys and permissions; sign-in sessions; end-to-end tests in CI
- [x] Group commit for metadata writes (small PUTs were capped by one fsync'd transaction
  at a time; now about four times as many per second, see [BENCHMARKS.md](BENCHMARKS.md))
- [x] Per-bucket CORS (needed for browser uploads with presigned URLs)
- [x] Bucket quotas
- [x] Virtual-hosted-style addressing

## v2: data protection
- [x] Bucket versioning: enable/suspend, version listing, delete markers, GET/DELETE by version
- [x] Encryption at rest: SSE-S3 (master key from config)
- [x] SSE-C (customer-provided keys)
- [x] Encrypt stored access-key secrets under the master key
- [x] Lifecycle rules: expire objects, expire noncurrent versions, abort incomplete uploads
- [x] Object tagging

## v2.1: polish
- [x] Console: browse an object's versions, restore or delete them, and list deleted objects
- [x] `aether backup`: a consistent snapshot of a running node (metadata and blocks)
- [x] `GetObjectAttributes` (size, ETag, checksums, parts with their checksums), and part-number GETs answered as Ceph does

## v3: clustering
Design: [CLUSTERING.md](CLUSTERING.md).

- [x] Single-node groundwork: node-local state in `node.redb`; version ids and
  Last-Modified stamped at commit, in commit order; exact metadata snapshots and
  a state digest
- [x] Internal API and node identity (port 9002, signed requests with replay
  protection, node ids, `aether cluster ping`)
- [ ] Raft-replicated metadata (openraft) using the existing command model: the
  leader stamps and proposes batches, followers serve linearizable reads, exact
  snapshots are built by log size
- [ ] Cluster layout, node join/leave, failure detection
- [ ] Block replication (3 copies, written to at least 2) with rendezvous-hash
  placement, a persistent repair queue, and staging leases so the orphan sweeper
  never deletes copies of an upload in progress
- [ ] Rebalancing that keeps blocks readable across every unsettled layout
  version; repair from the repair queues, an hourly block-list comparison and the
  scrubber; an orphan sweeper that waits until its node has caught up
- [ ] `aether cluster recover`: restart from a survivor after losing a majority
- [ ] Any node serves any request

## Later
- Erasure coding
- Webhook/queue notifications on object changes
- Object lock (WORM retention, legal hold)
- Multiple disks per node, with redundancy across them (like RAID 1): each block
  on two disks, so a node keeps serving all its data when one disk fails, and a
  replaced disk is refilled from the other
- Automated backups: `aether backup` on a schedule (`backup.schedule`,
  `backup.dir`), dated directories, retention (keep the last N daily and
  weekly), and the last result in `aether status` and the metrics
- Compression, deduplication
- Bucket replication between clusters
- External KMS for encryption keys

## Not planned
AWS IAM policy language, STS, S3 Select, static website hosting, storage classes
and tiering.
