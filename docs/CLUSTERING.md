# Clustering design (v3)

**Status: reviewed; step 0 (single-node groundwork) in progress.** It turns
the single node of v1–v2.1 into a cluster of 3 or more nodes that keeps
working when a node or a disk fails. The fixes from the review are folded into
the sections below and listed in [Review](#review). [DESIGN.md](DESIGN.md) describes the single node this builds on;
the roadmap items are in [ROADMAP.md](ROADMAP.md#v3-clustering).

## Goals

- **Survive the loss of any one node or disk** without losing acknowledged
  writes or becoming unavailable, with the default of 3 copies.
- **Same S3 semantics as one node:** strong read-after-write consistency for
  objects and listings, conditional writes, versioning and quotas, as today.
- **Any node serves any request.** Clients and load balancers need no notion
  of a leader.
- **A single node is a cluster of one.** Existing data directories become
  clusters in place. There is no export or import, and the storage and
  metadata code stay.
- **Still simple to operate:** one binary, one config file, a few `aether
  cluster` commands, and no external coordination service.

## Non-goals (for v3)

- Erasure coding. Copies first; erasure coding is listed under "Later" because
  it makes repair and small objects harder.
- Geo-distribution over high-latency links. Nodes are expected to be in one
  data centre or region, a few milliseconds apart. Zones (racks, rooms) are
  supported for placement.
- More than about 50 nodes. One Raft group holds all metadata. Sharding
  metadata by bucket is possible later without changing the block layer.
- Automatic removal of failed nodes by default (see
  [Failure handling](#failure-handling)).

## Overview

Every node runs the same binary with the same components as today: the S3
gateway, the admin API, the engine, a metadata replica and a block store. Two
things are new:

1. **Metadata is replicated with Raft** ([openraft]). The redb tables become
   the state machine of one Raft group. The `meta::Command` values that every
   write already goes through become the Raft log entries.
2. **Blocks are stored on 3 nodes**, chosen by hashing the block id over the
   cluster layout. No metadata records where a block lives. The layout decides,
   so adding nodes needs no metadata rewrite.

```
            S3 clients / load balancer (any node)
        ┌───────────────┬───────────────┬───────────────┐
   ┌────▼────┐     ┌────▼────┐     ┌────▼────┐
   │ node A  │     │ node B  │     │ node C  │   gateway + engine on each
   │ (leader)│◄───►│         │◄───►│         │   Raft: metadata log
   ├─────────┤     ├─────────┤     ├─────────┤
   │ meta    │     │ meta    │     │ meta    │   same redb tables, replicated
   │ blocks  │     │ blocks  │     │ blocks  │   each block on 3 nodes (HRW)
   └─────────┘     └─────────┘     └─────────┘
          internal API (port 9002): Raft RPCs, block put/get, health
```

## Where everything lives

Every node keeps, in its own data directory:

| File | Contents | Replicated |
|---|---|---|
| `meta.redb` | All metadata: buckets, objects, versions, uploads, keys, block reference counts, GC queue. The same tables as today, complete on every node, voters and learners alike | Yes: identical on every node, through the Raft log |
| `raft.redb` | The Raft log and vote | Yes: it is the replication |
| `node.redb` | This node's own state: node id, last scrub, repair queue, staging leases | No |
| `blocks/` | The blocks this node owns: about 3/N of all data | Each block on 3 nodes |

A write is acknowledged only when its metadata is on disk on a majority of
voters and its blocks on at least 2 of their 3 owners:

```
client ──PUT──► B
                B ── blocks ──► their 3 owners          waits until 2 have them on disk
                B ── command ──► A (leader)
                A ── log entry ──► other voters         waits until a majority has it on disk
                every node applies the entry to its meta.redb, in log order
client ◄─200── B
```

So no data and no metadata exist on one node only. When a node fails, the
others still hold all metadata and at least one other copy of each of its
blocks (see [Failure handling](#failure-handling)). When it returns, it
replays the log entries it missed, or installs a snapshot if it is too far
behind, and repair restores the copies it missed. A replacement node starts
from a snapshot and receives its share of blocks by rebalancing.

## Metadata: one Raft group

### What is replicated

The Raft log carries batches of `meta::Command`. The writer thread already
gathers up to 256 waiting commands into one transaction (group commit). In a
cluster, the leader's writer proposes that batch as one log entry, so one
round trip and one fsync per node cover the whole batch.

The state machine applies an entry with the existing `apply_batch`. Commands
carry every non-deterministic input (times, generated ids, GC due times), and
a failing command is dropped and the rest re-applied. So every replica that
applies the same entries ends up with the same tables, and each command gets
the same outcome (success, `PreconditionFailed`, `QuotaExceeded`, ...). Only
the proposing node returns the outcomes to clients.

The last applied log id is stored in the `system` table **in the same redb
transaction** as the commands it applied. A crash therefore never applies an
entry twice or skips one.

**Version ids and times come from the leader.** S3 lists versions newest
first, and lifecycle rules use that order and those times ("keep the 3 newest
noncurrent versions", "expire 30 days after becoming noncurrent"). If every
gateway stamped its own commands, clock differences between nodes would order
versions differently from the order they were committed in, and such a rule
could delete the wrong versions. So the writer that proposes a batch stamps
every command that creates a version (PutObject, CopyObject, CompleteUpload
and delete markers): Last-Modified is its clock, never earlier than the
previous stamp, and the version id is a UUIDv7 built from that time and a
counter, so ids increase strictly in commit order. The last stamp is stored
with the commands, so a restarted node or a new leader continues after it.
Followers apply stamped commands as they are.

### Storage

- **Raft log and vote:** a separate `raft.redb` in the data directory, so
  appending to the log (fsynced before acknowledging) does not contend with
  state-machine transactions. Applied entries are purged once a snapshot
  covers them.
- **Node-local state:** `node.redb` holds what belongs to one node (node id,
  last scrub, repair queue, staging leases). It is never replicated or
  snapshotted, so `meta.redb` holds only state that every replica shares.
- **Snapshots are exact copies** of every replicated table, including the GC
  queue and unreferenced blocks. They are *not* the backup snapshot:
  `aether backup` leaves those out because a restored node has no deletions
  pending, but a replica without them would differ from the leader (its GC
  would never delete those blocks, and the same command could succeed on one
  node and fail on another). A digest over the replicated tables checks that
  two replicas at the same applied index agree; tests use it, and so does
  `aether cluster status`.
- **When:** a snapshot is built when the log since the last one exceeds
  `cluster.snapshot_log_bytes` (default 1 GiB), not every N entries. One
  entry is a whole group-commit batch, and at about 700 batches per second
  ([BENCHMARKS.md](BENCHMARKS.md)) 10,000 entries would mean a full copy of
  the metadata every 15 seconds. A follower receives a snapshot only when it
  needs entries that were already purged; it is streamed from one read
  transaction and installed by swapping the file in.

### Writes

1. Any node receives the S3 request, stages the object's blocks on their
   owners (see [Blocks](#blocks-three-copies)), and builds the command as
   today.
2. If it is not the leader, it forwards the command over the internal API to
   the leader, which feeds it to its writer like a local write.
3. The leader's writer batches, proposes, and returns each command's outcome
   once the entry is committed by a majority and applied on the leader.

A client that loses the connection mid-write may see either result, as with
S3 today. Retried `CompleteMultipartUpload` calls are already idempotent.

### Reads: linearizable everywhere

S3 is strongly consistent: a GET after a successful PUT sees it. Reads go
through openraft's read index (`ReadPolicy::ReadIndex`):

- **On the leader:** `ensure_linearizable`, then read the local redb snapshot.
- **On a follower:** ask the leader for its read log id (one small RPC), wait
  until the local state machine has applied up to it, then read locally. This
  is openraft's follower read. It spreads read load across nodes without
  giving up consistency.

The read index is shared: concurrent reads on a node wait on one
outstanding request, so a burst of GETs costs one round trip, not one each.
`ReadPolicy::LeaseRead` (no round trip, relies on bounded clock drift) can be
enabled with `cluster.lease_reads = true` for read-heavy clusters that accept
that trade-off. It is off by default.

### Voters and learners

Up to 5 nodes are Raft voters (3 by default, chosen across zones). The others
are learners: they receive the log and serve reads and writes, but do not
vote. This keeps commit latency flat as the cluster grows. Voter changes use
openraft's joint consensus (`add_learner`, then `change_membership`).

## Blocks: three copies

### Placement

Each block is stored on the first `R` nodes (`cluster.replicas`, default 3)
in **rendezvous (HRW) order** for its id:
`score(node) = hash(block_id ‖ node_id) × weight(node)`, where the weight
comes from the node's capacity. Block ids are random UUIDs, so blocks spread
evenly, and adding or removing a node moves only about 1/n of them.

With zones configured, the `R` copies go to distinct zones first (HRW order
within each zone). With fewer nodes than `R`, every node holds a copy, and
the write quorum is at most the number of nodes, so a cluster of one keeps
today's single copy.

Nothing in `ObjectRecord` records locations. `BlockRef` stays as it is
(`id`, `len`, `crc32c`, `stored_len`). Locations are a function of the
layout, so moving data never rewrites object metadata.

### Writing a block

The gateway node streams each block to its `R` owners in parallel over the
internal API. Each owner writes it as today (tmp file, fsync, rename) and
answers with the CRC32C. The block counts as written once **W** owners
(`cluster.write_quorum`, default 2 of 3) have it.

- A missing copy (an owner that was down or slow) is recorded in the
  gateway's repair queue in `node.redb`, so it survives a restart, and the
  repair job copies it as soon as the owner is back. If the gateway is lost
  as well, the block-list comparison ([Rebalancing and repair](#rebalancing-and-repair))
  finds the missing copy.
- **Staging leases:** each owner records the blocks it receives for an
  upload that is not committed yet, with a lease that the gateway renews
  while the upload runs (10 minutes by default). The orphan sweeper leaves
  leased blocks alone, so a slow multi-gigabyte upload cannot lose its copies.
  If the gateway dies, the lease expires and the blocks become ordinary
  orphan candidates.
- If fewer than W owners succeed, the upload fails with 503 `SlowDown` and the
  copies that were written are left to the orphan sweeper.
- Encryption happens on the gateway before blocks leave it, so blocks are
  encrypted in transit between nodes and at rest. Every node needs the same
  master key, which the config check enforces at join.

Inline objects (up to 4 KiB) live in the metadata, so Raft replicates them.

### Reading a block

Read from the owner with the best latency (a local copy first), checking the
CRC32C as today. If a copy is missing, unreachable or corrupt, try the next
owner and queue a repair. A read fails only if all `R` copies are unreadable.

### Deleting blocks

The `blocks` reference counts and `gc_queue` are metadata, so they are
replicated. Only the leader runs the GC job: it proposes `ForgetBlocks` for
due blocks. Each node, after applying it, deletes its own copies of those
blocks. If a node crashes in between, the orphan sweeper removes the files
later, because the block is no longer known.

The orphan sweeper deletes a block file only if it is older than the grace
period, has no live staging lease, and is unknown to a state machine that has
applied at least up to the leader's commit index at the start of the sweep (a
read index). A node catching up after downtime therefore never takes a
recently committed block for an orphan. Backups on any node pause GC
cluster-wide: the pause is a replicated flag with an expiry.

## Layout and membership

The **cluster layout** lists every node: its id, internal address, zone,
capacity and role (voter or learner). It has a version number and lives in
the replicated state machine (`system` table), so every node agrees on it.
Changes are commands: `AddNode`, `UpdateNode` and `RemoveNode`.

```sh
aether cluster init                          # on the first node (existing data stays)
aether cluster join https://node-a:9002      # on each new node; needs cluster.secret
aether cluster status                        # nodes, roles, health, rebalancing progress
aether cluster remove <node-id>              # drain a node, or forget a dead one
```

- **Node identity:** a random node id is created on first start and stored in
  the data directory, so a node keeps its identity across restarts and
  address changes.
- **Internal API:** HTTP/2 on its own port (`cluster.listen`, default 9002).
  It carries Raft RPCs, block put/get/list, read-index requests and health.
  Every request is authenticated with the shared `cluster.secret` (an HMAC
  over the request), and uses TLS when `tls.cert_file` is set. A node that
  does not know the secret cannot join or read blocks.
- **Joining** adds the node as a learner, waits for it to catch up (snapshot,
  then log), then adds it to the layout. If there are fewer than 3 voters in
  distinct zones, it is promoted. Adding the node changes the layout, which
  starts rebalancing.

## Rebalancing and repair

A layout change moves block ownership. Each node walks its local blocks and,
for each one, compares its owners under the new layout with where the block
already is:

1. **Copy** the block to new owners that lack it (throttled with
   `cluster.rebalance_bytes_per_sec`).
2. **Report** progress to the leader. Once every node reports that all its
   blocks are in place under version N, the leader commits "layout N settled".
3. **Drop** copies a node no longer owns, only after the layout is settled
   and a grace period has passed.

Until then, readers try the owners under every layout version that has not
settled yet, newest first, so blocks stay readable however many changes are
in flight (adding two nodes one after the other, say). Versions settle in
order, oldest first.

**Repair** has three sources:

- the repair queues, for copies a write missed;
- a block-list comparison that each node runs every hour
  (`cluster.anti_entropy_interval`) with the other owners of its blocks: they
  exchange hashes of block-id ranges and list only the ranges that differ, so
  a copy missing for any reason is found within the hour;
- the weekly scrubber, which re-reads every local copy and replaces a corrupt
  one from another owner.

`aether cluster status` and the metrics report under-replicated blocks.

## Failure handling

| Failure | Effect | Recovery |
|---|---|---|
| Leader crashes | Writes pause for an election (about 1–2 s with default timeouts); reads on followers wait for the new leader's read index | Automatic |
| A follower crashes | No visible effect; its block copies are read from other owners; new writes reach 2 of 3 owners | Automatic catch-up when it returns; repair fills the copies it missed |
| A disk fails on one node | Same as a crash for that node's blocks | Replace the disk; the node rejoins empty and repair refills it, or `cluster remove` it |
| A node is gone for good | Its blocks have 2 copies until handled | `aether cluster remove` → rebalancing makes third copies elsewhere |
| Network partition | The side with a majority of voters keeps working; the minority answers 503 for writes and linearizable reads | Heals automatically; minority nodes catch up from the log |
| Two of three nodes down | No metadata quorum: writes and reads fail with 503 | Bring a node back. If neither can come back, `aether cluster recover` on the survivor makes it the only voter: it keeps everything it had applied, but writes it had not yet received are lost. Losing the disks of two of three nodes at once is beyond what 3 copies protect against |

Nodes report health to the leader every second. A node is shown as down after
10 seconds, but it stays in the layout. Data is not moved off a node that may
come back, because rebalancing a whole disk for a reboot costs more than it
protects. `cluster.auto_remove_after` (unset by default) removes a node
automatically after a long absence, for operators who want that.

## Background jobs

| Job | Where it runs |
|---|---|
| Garbage collection, lifecycle rules, abandoned-upload cleanup | The leader only. Its decisions are commands, so they replicate |
| Orphan sweeper, scrubber, repair, rebalancing | Every node, on its own blocks |
| Backups | Any node: a consistent metadata snapshot, plus each block read from any owner |

## Upgrading a single node

`aether cluster init` on an existing node:

1. creates its node id, `raft.redb` and a one-node layout (with `R` = 1
   effectively, since there is one node);
2. treats the current `meta.redb` as the state machine at log index 0;
3. starts the internal API.

Adding a second and a third node then brings every block to 3 copies through
ordinary rebalancing. A node that never runs `cluster init` keeps working as
today, and clustering stays off.

## Configuration

```toml
[cluster]
listen = "0.0.0.0:9002"     # internal API
secret = ""                 # shared by every node; prefer AETHER_CLUSTER__SECRET
zone = "rack-1"             # optional; copies go to distinct zones first
capacity = "8TB"            # placement weight; default: the data disk's size
replicas = 3                # copies of each block
write_quorum = 2            # copies written before an upload succeeds
lease_reads = false         # linearizable reads by lease instead of read index
rebalance_bytes_per_sec = "100MB"
snapshot_log_bytes = "1GiB"  # build a metadata snapshot when the log grows past this
anti_entropy_interval = "1h" # compare block lists with the other owners
# auto_remove_after = "24h"
```

## Observability

- **Metrics:** Raft term, role, commit and applied indexes; replication lag
  per follower; read-index latency; forwarded writes; block copies written per
  node; under-replicated blocks; rebalance progress; internal API errors.
- **Admin API and console:** a Cluster page with nodes, roles, zones, health,
  capacity and usage, layout version and rebalancing progress.
- **Logs:** leadership changes, layout changes and repair actions at `info`.

## Testing

- **Storage:** openraft's storage test suite runs against the redb-backed log
  and state machine.
- **Determinism:** every test that runs commands on several replicas, or
  installs a snapshot, compares the replicas' state digests.
- **In-process clusters:** integration tests start 3 to 5 nodes in one process
  over an in-memory network that can drop, delay and partition messages. They
  check acknowledged writes survive leader changes, partitions and restarts,
  and that histories of concurrent operations stay linearizable.
- **Real processes:** a Docker Compose cluster of 3 nodes in CI. It runs the
  existing client and Ceph s3-tests suites against a load balancer, then kills,
  restarts and partitions nodes during a write load. It finally verifies every
  acknowledged object byte for byte and that every block has 3 copies.

## Implementation plan

One PR each, as in the [roadmap](ROADMAP.md#v3-clustering):

0. **Single-node groundwork (in progress):** `node.redb` for node-local
   state; version ids and Last-Modified stamped by the writer in commit order;
   exact metadata snapshots and a state digest. These change nothing for
   clients, and on one node they already keep version order right when the
   clock steps back.
1. **Internal API and node identity:** port 9002, `cluster.secret`
   authentication, node ids, health.
2. **Raft metadata:** openraft over `raft.redb` and the existing tables;
   the leader stamps and proposes batches; leader forwarding; follower reads;
   `cluster init` / `join`; exact snapshots by log size. Blocks still local,
   so this step runs only in tests.
3. **Layout and placement:** layout commands, HRW with zones and weights,
   block put/get over the internal API, W-of-R writes, reads with failover,
   the persistent repair queue and staging leases.
4. **Rebalancing and repair:** layout versions (reads across every unsettled
   one), throttled moves, settle and drop; block-list comparison; scrubber
   repair from replicas; the orphan sweeper gated on the read index;
   under-replication metrics.
5. **Operations:** `aether cluster` commands including `recover`, Cluster
   page in the console, cluster-wide GC pause for backups, the Compose chaos
   test in CI, and docs.

## Review

The first draft had these problems. Each is fixed in the section named.

| Problem | Fix | Step |
|---|---|---|
| Version ids and Last-Modified came from the clock of the node that took the request, so versions could be ordered differently from their commits, and lifecycle rules could expire the wrong ones | [Stamped by the proposing writer in commit order](#what-is-replicated) | 0, 2 |
| Raft snapshots reused the backup snapshot, which leaves out the GC queue and unreferenced blocks, so a follower installed from one would differ from the leader | [Exact snapshots and a state digest](#storage) | 0, 2 |
| A full snapshot every 10,000 entries, each entry a batch: about every 15 seconds | [Snapshots by log size, sent only when needed](#storage) | 2 |
| The scrubber writes its last-run time into `meta.redb` outside the command model, so replicas would differ | [`node.redb` for node-local state](#storage) | 0 |
| The orphan sweeper could delete copies of an upload in progress on another node, or recent blocks on a node catching up | [Staging leases; sweeper gated on the read index](#deleting-blocks) | 3, 4 |
| Reads fell back only one layout version, so two changes in a row could make blocks unreadable | [Owners under every unsettled version](#rebalancing-and-repair) | 4 |
| The queue of missing copies lived in memory, and only the weekly scrub found the rest | [Persistent repair queue; hourly block-list comparison](#rebalancing-and-repair) | 3, 4 |
| No way back after losing two of three nodes | [`aether cluster recover`](#failure-handling) | 5 |

## Decisions to confirm

1. **3 copies, written to at least 2** (`replicas = 3`, `write_quorum = 2`).
   Requiring all 3 would block every upload while any node is down.
2. **Strong reads by read index on every node**, with lease reads optional.
   Forwarding every read to the leader is simpler but makes it a bottleneck.
3. **Shared cluster secret plus optional TLS** for node-to-node traffic,
   rather than requiring mutual TLS certificates per node.
4. **Failed nodes stay in the layout until an operator removes them**
   (optional automatic removal), rather than starting a rebalance on every
   outage.
5. **Up to 5 voters**, with the other nodes as learners.

[openraft]: https://github.com/databendlabs/openraft
