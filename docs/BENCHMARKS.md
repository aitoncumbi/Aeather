# Benchmarks

Baseline numbers for a single Aether node, how to reproduce them, and what
limits them today. Treat them as a reference point for changes to Aether, not as
a hardware rating: the client and server shared one small cloud VM.

## Running the benchmark

`examples/bench.rs` is a load generator built on the AWS SDK for Rust. It works
against any S3 endpoint. It creates a scratch bucket, runs a fixed sequence of
phases and deletes everything it wrote:

| phase | what it does |
|---|---|
| `PUT <size>` | `--concurrency` workers write new small objects for `--duration` seconds |
| `GET <size>` | the same workers read random objects from the PUT phase |
| `LIST` | up to 8 workers page through the first small-object prefix, 1000 keys a page |
| `PUT <n> MiB` | `--large-count` single PUTs of `--large-mib` MiB, `--large-concurrency` at a time |
| `GET <n> MiB` | reads each of those objects back |

```sh
cd aether_backend
cargo build --release --bin aether --example bench

# A throwaway server with default settings.
AETHER_STORAGE__DATA_DIR=/tmp/aether-bench \
AETHER_S3__ACCESS_KEY=bench AETHER_S3__SECRET_KEY=bench-secret-key \
    target/release/aether serve > /dev/null &

target/release/examples/bench --endpoint http://127.0.0.1:9000 \
    --access-key bench --secret-key bench-secret-key --duration 10
```

Defaults: 32 concurrent requests, 1 KiB and 64 KiB small objects
(`--small-sizes`), 16 large objects of 64 MiB with 4 in flight. Each phase
prints one Markdown table row; latencies are per request, measured by the
client.

The deleted objects' blocks are kept for the GC grace period (15 minutes), so
repeated runs need free disk space for all of them, or a fresh data directory
each time.

## Results

Aether at commit `2e4d60c` with the default configuration (64 MiB blocks,
objects up to 4 KiB stored inline, access log on). Server and client ran on the
same VM: 4 vCPUs (Intel Xeon, 2.1 GHz), 15 GB of RAM, a virtio disk, Linux 6.18.

### `examples/bench`

| phase | ops | ops/s | MiB/s | p50 ms | p90 ms | p99 ms | max ms | errors |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| PUT 1 KiB | 14377 | 1435 | 1.4 | 21.8 | 26.3 | 48.7 | 147.0 | 0 |
| GET 1 KiB | 84530 | 8449 | 8.3 | 3.7 | 5.6 | 7.6 | 13.1 | 0 |
| PUT 64 KiB | 9951 | 992 | 62.0 | 30.9 | 41.7 | 68.1 | 108.4 | 0 |
| GET 64 KiB | 67296 | 6726 | 420.4 | 4.5 | 7.1 | 10.1 | 18.1 | 0 |
| LIST 1000 keys/page | 5355 | 527 | 0.0 | 14.6 | 20.8 | 27.2 | 44.3 | 0 |
| PUT 64 MiB | 16 | 4 | 245.5 | 978.7 | 1179.3 | 1245.7 | 1245.7 | 0 |
| GET 64 MiB | 16 | 21 | 1343.7 | 167.8 | 257.5 | 265.8 | 265.8 | 0 |

### Cross-check with MinIO's warp

[warp](https://github.com/minio/warp) 1.3.0 against the same server, 32
concurrent requests, 10 seconds per run:

```sh
warp put --host 127.0.0.1:9000 --access-key bench --secret-key bench-secret-key \
    --concurrent 32 --duration 10s --noprefix --obj.size 64KiB
warp get ... --obj.size 64KiB --objects 2000
```

| operation | obj/s | MiB/s | p50 ms | p90 ms | p99 ms |
|---|--:|--:|--:|--:|--:|
| PUT 1 KiB | 1197 | 1.2 | 25.3 | 33.3 | 55.8 |
| GET 1 KiB | 15954 | 15.6 | 5.9 | 13.0 | 28.1 |
| PUT 64 KiB | 999 | 62.4 | 31.9 | 41.1 | 71.4 |
| GET 64 KiB | 9569 | 598.1 | 10.4 | 13.0 | 24.1 |

The PUT figures agree. warp reads about twice as fast because it is the lighter
client: on four shared cores, the AWS SDK's signing and response checksums are
what limit `GET` in `examples/bench`, not the server.

## What limits Aether today

**Small writes: one metadata commit at a time.** Every write is a redb
transaction that is fsynced before the client gets its answer, and redb runs
one write transaction at a time. Small PUTs therefore top out at about 1,400 to
1,700 per second. Adding concurrency only adds queueing:

| concurrency | PUT 1 KiB ops/s | p50 ms | p99 ms |
|--:|--:|--:|--:|
| 1 | 720 | 1.3 | 2.1 |
| 4 | 1393 | 2.6 | 6.4 |
| 16 | 1578 | 10.0 | 21.0 |
| 64 | 1692 | 36.6 | 78.2 |

The fix is group commit: let one transaction (and one fsync) carry every write
that is waiting, with per-write rollback for writes whose preconditions fail.
It is on the roadmap.

**Large writes: hashing and syncing.** A 64 MiB PUT is hashed three times
(MD5 for the ETag, CRC32C for the block, and the CRC32 that the SDK sends) and
then synced to disk. That gives about 245 MiB/s here, spread over 4 uploads.

**Reads: mostly the page cache.** Freshly written data is still in memory, so
these reads measure the request path rather than the disk. Reads from a cold
cache are bounded by the disk.

**Listing** returns about 500 pages of 1000 keys per second (roughly half a
million keys per second) with 8 clients.
