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
- [x] Conditional writes (`If-None-Match: *`, `If-Match`) checked inside the commit
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
- [ ] Multipart upload: Create, UploadPart, Complete, Abort, ListParts, ListMultipartUploads
- [ ] UploadPartCopy
- [ ] Automatic cleanup of abandoned multipart uploads
- [ ] CopyObject (shares blocks by reference count; no data copy)
- [ ] GetObject/HeadObject `partNumber`

### M4. Access control and admin API
- [ ] Access keys stored in metadata, per-bucket permissions (read, write, admin)
- [x] Presigned URLs (verified by `s3s`; covered by tests)
- [ ] Admin API: keys, buckets, permissions, usage stats
- [ ] `aether` CLI subcommands for bootstrapping (create the first key)

### M5. Operations
- [x] TOML config with environment overrides
- [ ] Native TLS (rustls)
- [ ] Prometheus metrics: requests, latency, bytes, errors, GC, disk usage
- [x] JSON logs option
- [ ] Request ids in logs and responses
- [x] Graceful shutdown (finish in-flight requests)
- [ ] Request timeouts and body limits
- [ ] Docker image and example compose file
- [ ] Background scrubber that verifies block CRC32Cs

### M6. Compatibility
- [ ] CI runs the AWS CLI, boto3 and rclone against a live server
- [ ] A subset of Ceph's [s3-tests](https://github.com/ceph/s3-tests) in CI, tracked as a pass list
- [ ] Benchmarks (small-object ops/s, large-object throughput)

## v1.1: console
- [ ] Web console on the admin API: buckets, object browser, keys, permissions, usage
- [ ] Per-bucket CORS (needed for browser uploads with presigned URLs)
- [ ] Bucket quotas
- [ ] Virtual-hosted-style addressing

## v2: data protection
- [ ] Bucket versioning: enable/suspend, version listing, delete markers, GET/DELETE by version
- [ ] Encryption at rest: SSE-S3 (master key from config), then SSE-C
- [ ] Encrypt stored access-key secrets under the master key
- [ ] Lifecycle rules: expire objects, expire noncurrent versions, abort incomplete uploads
- [ ] Object tagging

## v3: clustering
- [ ] Raft-replicated metadata (openraft) using the existing command model
- [ ] Cluster layout, node join/leave, failure detection
- [ ] Block replication (3 copies by default) with rendezvous-hash placement
- [ ] Rebalancing and repair after node loss
- [ ] Any node serves any request

## Later
- Erasure coding
- Webhook/queue notifications on object changes
- Object lock (WORM retention, legal hold)
- Multiple disks per node
- Compression, deduplication
- Bucket replication between clusters
- External KMS for encryption keys

## Not planned
AWS IAM policy language, STS, S3 Select, static website hosting, storage classes
and tiering.
