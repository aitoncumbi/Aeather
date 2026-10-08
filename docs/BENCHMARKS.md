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

To compare two versions, run them alternately, each with a fresh data
directory, and compare medians: separate runs of one version differ by up to
about 10%. The 64 MiB phases are short (16 requests, a second or two) and come
after the small-object phases, which leave more data behind the faster the
server writes; compare large objects on their own with
`--duration 1 --small-sizes 1024`.

## Results

Aether at commit `43b3ae0` (with group commit, versioning, encryption at rest
and the web console), compared with `2e4d60c`, the v1 baseline. Both were built
with Rust 1.98 and run with the default configuration (64 MiB blocks, objects
up to 4 KiB stored inline, access log on, no encryption at rest) on one VM:
4 vCPUs (Intel Xeon, 2.1 GHz), 15 GB of RAM, a virtio disk, Linux 6.18. Server
and client shared the VM. The versions ran alternately, each run with a fresh
data directory, and the figures are medians of three runs.

### `examples/bench`

`--duration 10`, other options at their defaults:

| phase | ops/s | MiB/s | p50 ms | p90 ms | p99 ms | errors |
|---|--:|--:|--:|--:|--:|--:|
| PUT 1 KiB | 6042 | 5.9 | 5.1 | 7.1 | 10.1 | 0 |
| GET 1 KiB | 7985 | 7.8 | 3.9 | 6.0 | 8.0 | 0 |
| PUT 64 KiB | 1600 | 100.0 | 19.1 | 26.5 | 41.2 | 0 |
| GET 64 KiB | 6758 | 422.4 | 4.5 | 7.0 | 9.7 | 0 |
| LIST 1000 keys/page | 575 | 0.0 | 13.4 | 19.0 | 25.0 | 0 |
| PUT 64 MiB | 5 | 288.4 | 893.5 | 946.9 | 952.8 | 0 |
| GET 64 MiB | 15 | 964.5 | 210.1 | 494.1 | 503.2 | 0 |

Compared with `2e4d60c` in the same runs:

| measure | `2e4d60c` | `43b3ae0` | change |
|---|--:|--:|--:|
| PUT 1 KiB, ops/s | 1518 | 6042 | ×4.0 |
| PUT 1 KiB, p99 ms | 51.7 | 10.1 | ÷5.1 |
| PUT 64 KiB, ops/s | 1039 | 1600 | +54% |
| GET 1 KiB, ops/s | 8237 | 7985 | −3% |
| GET 64 KiB, ops/s | 7033 | 6758 | −4% |
| LIST, pages/s | 625 | 575 | −8% |
| PUT 64 MiB, MiB/s | 274 | 288 | +5% |
| GET 64 MiB, MiB/s | 1252 | 965 | −23% |

- **Small writes** are four times as fast, with a fifth of the tail latency:
  group commit (below).
- **Small reads** are 3 to 4% slower (for GET 1 KiB, within the spread
  between runs of either version).
- **Listing:** the faster PUT phase leaves four times as many keys under the
  listed prefix (60,000 instead of 15,000). With 40-second phases `2e4d60c`
  writes as many and lists 601 pages per second (one run), so about half of
  the difference is the larger table and about 4% is the code.
- **GET 64 MiB** in this sequence is not a like-for-like comparison. The phase
  is bound by the client's CPU (in a traced run, 2.7 of the 4 cores in the
  client and 0.3 in the server) and slows down the more the earlier phases
  wrote: `2e4d60c` with 40-second phases drops to 663 MiB/s as well.

Large objects on their own (`--duration 1 --small-sizes 1024`, two runs each,
MiB/s) are as fast as before:

| | PUT 64 MiB | GET 64 MiB |
|---|--:|--:|
| `2e4d60c` | 430, 503 | 1308, 1443 |
| `43b3ae0` | 469, 465 | 1441, 1446 |

The server alone, measured with curl (64 downloads of 64 MiB, 4 at a time, no
response checksum validation), serves 3,267 and 3,440 MiB/s (`2e4d60c`) and
3,395, 3,246 and 3,448 MiB/s (`43b3ae0`).

### Encryption at rest

With `encryption.master_key` set, every new object is encrypted (SSE-S3).
Medians of three runs, alternating with the runs above:

| measure | without | with SSE-S3 | change |
|---|--:|--:|--:|
| PUT 1 KiB, ops/s | 6042 | 5742 | −5% |
| GET 1 KiB, ops/s | 7985 | 8083 | +1% |
| PUT 64 KiB, ops/s | 1600 | 1506 | −6% |
| GET 64 KiB, ops/s | 6758 | 6632 | −2% |
| PUT 64 MiB on its own, MiB/s | 469, 465 | 452, 455 | −3% |
| GET 64 MiB on its own, MiB/s | 1441, 1446 | 1159, 1112 | −21% |
| GET 64 MiB, server alone (curl, one run), MiB/s | 3448 | 2152 | −38% |

Decryption is the visible cost: it takes about a third of the server's read
throughput for large objects, which still leaves more than 2 GB/s on four
cores. Small objects cost at most about 6%.

### Cross-check with MinIO's warp

[warp](https://github.com/minio/warp) 1.3.0 against the same builds, 32
concurrent requests, 10 seconds per run, one run each:

```sh
warp put --host 127.0.0.1:9000 --access-key bench --secret-key bench-secret-key \
    --concurrent 32 --duration 10s --noprefix --obj.size 64KiB
warp get ... --obj.size 64KiB --objects 2000
```

| operation | `2e4d60c` obj/s | `43b3ae0` obj/s |
|---|--:|--:|
| PUT 1 KiB | 1461 | 6712 |
| GET 1 KiB | 17444 | 16989 |
| PUT 64 KiB | 1185 | 1887 |
| GET 64 KiB | 9985 | 9477 |

The writes agree with `examples/bench`. warp reads about twice as fast
because it is the lighter client: on four shared cores, the AWS SDK's signing
and response checksums are what limit `GET` in `examples/bench`, not the
server. warp's latency figures are left out: between identical runs of GET
1 KiB its average ranged from 2 to 11 ms on both versions, while throughput
varied by less than 10% (repeats: 18,523 obj/s for `2e4d60c`; 17,420 and
17,349 for `43b3ae0`).

## What limits Aether today

**Small writes: the CPU, on this VM.** In v1 every write was its own fsynced
redb transaction, and redb runs one write transaction at a time, so small PUTs
stopped at about 1,500 per second whatever the concurrency. Group commit
(v1.1) has a single writer thread commit every waiting write in one
transaction; a write that fails (a precondition, say) gets its error, and the
rest of the batch is applied again without it.
`examples/bench` with 1 KiB objects, 8 seconds, one run each:

| concurrency | `2e4d60c` ops/s | p50 ms | p99 ms | `43b3ae0` ops/s | p50 ms | p99 ms |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | 667 | 1.5 | 2.3 | 658 | 1.5 | 2.6 |
| 4 | 1602 | 2.4 | 4.5 | 2138 | 1.8 | 3.2 |
| 16 | 1562 | 10.0 | 21.3 | 4758 | 3.2 | 6.5 |
| 64 | 1568 | 40.4 | 85.2 | 6763 | 9.3 | 16.7 |

One client still waits for its own fsync. With many clients, each fsync
carries a whole batch: at 32 clients, in a traced run, the server used about
1.6 cores and the client 1.8, so together they nearly fill the VM, and higher
numbers need the client on another machine.
`aether_metadata_writes_total / aether_metadata_commits_total` is the average
batch size.

On a laptop with a slower fsync (Intel i5-8250U, 8 threads, 15 GB, NVMe with
btrfs, Linux 7.2; 1 KiB objects, 8 seconds per phase, server and client on the
same machine) the effect is larger:

| | clients | PUT ops/s | p50 ms | p99 ms |
|---|--:|--:|--:|--:|
| before | 1 | 146 | 6.4 | 16.1 |
| after | 1 | 150 | 6.4 | 9.4 |
| before | 32 | 166 | 190.6 | 254.5 |
| after | 32 | 1926 | 15.1 | 47.3 |

**Large writes: hashing and syncing.** A 64 MiB PUT is hashed three times
(MD5 for the ETag, CRC32C for the block, and the CRC32 that the SDK sends) and
then synced to disk: about 465 MiB/s over 4 uploads on their own, and 290 MiB/s
after the small-object phases.

**Reads: mostly the page cache.** Freshly written data is still in memory, so
these reads measure the request path rather than the disk. Reads from a cold
cache are bounded by the disk.

**Listing** returns about 575 pages of 1000 keys per second (over half a
million keys per second) with 8 clients, from a prefix of 60,000 keys.
