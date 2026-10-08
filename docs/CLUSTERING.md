# Clustering design (v3)

**Status: proposal, for review before any code.** It turns the single node of
v1–v2.1 into a cluster of 3 or more nodes that keeps working when a node or a
disk fails. [DESIGN.md](DESIGN.md) describes the single node this builds on;
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

### Storage

- **Raft log and vote:** a separate `raft.redb` in the data directory, so
  appending to the log (fsynced before acknowledging) does not contend with
  state-machine transactions. Applied entries are purged once a snapshot
  covers them.
- **Snapshots:** the existing `MetaStore::snapshot` (used by `aether backup`)
  copies every table from one read transaction into a new database file. A
  snapshot *is* a backup's `meta.redb`. It is streamed to a lagging follower
  and installed by swapping the file in. Snapshots are built every 10,000
  entries by default.

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

- A missing copy (an owner that was down or slow) is recorded in a local
  repair queue on the gateway, and the repair job copies it later. The read
  path and the scrubber find any copy that is still missing.
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
later, because the block is no longer known. Backups on any node pause GC
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

Until then, readers try owners under the new layout first and fall back to
the previous one, so nothing is unreadable during a move.

**Repair** reuses the same machinery: the scrubber already re-reads every
local block weekly. In a cluster, a corrupt or missing copy is fetched from
another owner and rewritten, and blocks with fewer than `R` copies are copied
to the owners that lack them. `aether cluster status` and the metrics report
under-replicated blocks.

## Failure handling

| Failure | Effect | Recovery |
|---|---|---|
| Leader crashes | Writes pause for an election (about 1–2 s with default timeouts); reads on followers wait for the new leader's read index | Automatic |
| A follower crashes | No visible effect; its block copies are read from other owners; new writes reach 2 of 3 owners | Automatic catch-up when it returns; repair fills the copies it missed |
| A disk fails on one node | Same as a crash for that node's blocks | Replace the disk; the node rejoins empty and repair refills it, or `cluster remove` it |
| A node is gone for good | Its blocks have 2 copies until handled | `aether cluster remove` → rebalancing makes third copies elsewhere |
| Network partition | The side with a majority of voters keeps working; the minority answers 503 for writes and linearizable reads | Heals automatically; minority nodes catch up from the log |
| Two of three nodes down | No metadata quorum: writes and reads fail with 503 | Bring a node back. Losing the data of two of three nodes at once is beyond what 3 copies protect against. Restore from a backup |

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

1. **Internal API and node identity:** port 9002, `cluster.secret`
   authentication, node ids, health.
2. **Raft metadata:** openraft over `raft.redb` and the existing tables;
   leader forwarding; follower reads; `cluster init` / `join`; snapshots from
   `MetaStore::snapshot`. Blocks still local, so this step runs only in
   tests.
3. **Layout and placement:** layout commands, HRW with zones and weights,
   block put/get over the internal API, W-of-R writes, reads with failover.
4. **Rebalancing and repair:** layout versions, throttled moves, settle and
   drop; scrubber repair from replicas; under-replication metrics.
5. **Operations:** `aether cluster` commands, Cluster page in the console,
   cluster-wide GC pause for backups, the Compose chaos test in CI, and docs.

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
