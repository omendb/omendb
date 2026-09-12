# ADR 0006: Deployment-aware storage and durability

- **Status:** accepted target architecture; implementation staged
- **Scope:** SeerDB physical storage, durability, caching, and deployment shapes
- **Depends on:** [ADR 0001](0001-seerdb-transaction-contract.md),
  [ADR 0002](0002-seerdb-mvcc-version-storage.md), and
  [ADR 0003](0003-seerdb-commit-recovery-state-machine.md)
- **Refines:** [ADR 0004](0004-group-commit-publication-lane.md)

## Context

OmenDB is not required to preserve its current physical storage design. The
single-node implementation has established transaction, recovery, and fault
contracts, but its page publication, logging, cache translation, and I/O policy
remain replaceable.

Current hardware makes one physical policy inappropriate for every deployment:

- modern servers combine large DRAM with high-IOPS NVMe SSDs;
- local and embedded deployments should avoid network dependencies;
- highly available deployments need a replicated durability boundary;
- disaggregated/serverless deployments need compute-independent durable state;
- object storage is extremely durable and inexpensive but has unsuitable
  latency and request economics for fine-grained OLTP writes;
- future CXL/RDMA memory tiers should be usable without putting fabric-specific
  semantics into transactions or SQL.

The architecture therefore separates the **logical transaction contract** from
the **durability and materialization strategy** used by a deployment.

## Decision

### 1. One transactional engine, not a storage-engine plugin matrix

SeerDB remains OmenDB's ordered transactional KV engine. OmenDB will not grow a
first-party RocksDB/Pebble/Fjall/etc. backend matrix. Different deployments may
use different durability transports, cache tiers, and materialization services,
but all must implement the same SeerDB transaction, snapshot, CSN, LSN, and
change-stream semantics.

The durable commit decision remains the visibility authority. Physical page
materialization, cache residency, checkpoints, object-store archival, and
replication are subordinate to that decision.

### 2. RAM is the hot tier; local NVMe is the primary local capacity tier

The default local/server architecture is larger-than-memory rather than
in-memory-only:

```text
transaction / ordered KV
        |
logical MVCC + version history
        |
ordered B-tree indexes
        |
low-overhead buffer translation
        |
DRAM hot set
        |
local NVMe durable pages/segments
```

The target is near in-memory overhead while the working set is cached and
graceful degradation when it is not. B-trees remain the default ordered local
structure: current research shows well-engineered pageable B-trees remain
competitive on modern RAM+NVMe systems and avoid the read/compaction
amplification of adopting an LSM by default.

The current 4 KiB page format is an alpha implementation detail, not a permanent
architecture requirement. Variable/adaptive node sizes, page packing, prefix
compression, and other layouts must be benchmarked before format stability.

### 3. Local writes remain out-of-place

The local physical store continues in the out-of-place direction. New page
images are placed into append-oriented storage regions and become reachable
through durable mappings/checkpoints; old locations are reclaimed later.

This aligns the DBMS write pattern with SSD behavior and leaves room for:

- sequential/batched eviction writes;
- page compression and packing;
- placement by expected lifetime;
- Zoned Namespace (ZNS) and Flexible Data Placement (FDP) devices;
- lower DBMS and device write amplification.

In-place B-tree page overwrite is not the target.

### 4. Commit is log-authoritative; page materialization leaves the latency path

The target commit shape is:

```text
validate / certify
      |
produce complete redo/change/decision data
      |
make the commit decision durable
      |
publish Committed(CSN) and acknowledge
      |
async page/version/checkpoint materialization
```

A commit must not require every durable page structure to be synchronized before
acknowledgement when the durable log contains everything required to recover the
transaction.

ADR 0004's group-publication lane is the current correct implementation, but it
is **not the permanent hardware policy**. The durability scheduler may differ by
deployment while preserving the state machine above.

For local NVMe, benchmark at least:

- current group commit;
- autonomous/parallel commit in which workers issue independent small durable
  log writes and acknowledgement is parallelized;
- adaptive batching that changes policy with concurrency and observed device
  latency.

No mechanism wins by design fiat; the measured best policy becomes the local
implementation.

### 5. HA uses a replicated log, not synchronous page replication

The regional HA target is a single logical writer per shard/range with a
quorum-replicated append log:

```text
writer
  |
  +---- durable append ----> log replica A (NVMe)
  +---- durable append ----> log replica B (NVMe)
  +---- durable append ----> log replica C (NVMe)
              |
       quorum commit decision
              |
      page/cache materializers
              |
       immutable archive
```

The replicated log is the short-term durability and ordering service. Page
replicas/materializers may lag because they are rebuildable from a checkpoint
plus the committed log.

The design should require one network round trip to a quorum in the normal
single-region case and tolerate stragglers. BtrLog is a reference design, not a
dependency or API to copy.

### 6. Disaggregated/serverless mode separates log, cache/materialization, and
object storage

A future disaggregated deployment uses three roles:

1. **durable log service** — ordered replicated commits;
2. **page/cache service** — SSD-backed materialized state optimized for reads;
3. **object storage** — immutable checkpoints, archived logs, cold history,
   backups, and large immutable artifacts.

Compute may be replaced or resized without moving authoritative durable state.
Local NVMe attached to compute or cache nodes is a performance tier, not the
only copy of the database.

This resembles the useful separation found in Neon/Socrates/Aurora-style
systems without requiring PostgreSQL page semantics.

### 7. Object storage is first-class but not the fine-grained OLTP write device

Object storage is supported as a durable **immutable-object tier**. The engine
must batch data into suitably large immutable segments/checkpoints before
upload. It must not map one B-tree page update to one object mutation.

A cost-first object-native deployment may eventually acknowledge against an
object-native log/LSM service, but that is a distinct measured deployment
profile. It must not degrade the local NVMe engine or force an LSM physical
layout on every deployment.

Object storage is immediately appropriate for:

- backups and point-in-time recovery;
- WAL/log archival;
- checkpoints/snapshots;
- cold MVCC/version history;
- large immutable blobs;
- disaggregated rebuild/bootstrap.

### 8. Buffer and I/O mechanisms are hardware policies behind stable semantics

The buffer layer keeps explicit logical page identities. The implementation may
change from the current mapping/cache design to a faster translation scheme.
Predictive translation, pointer swizzling, direct arrays, and virtual-memory
assisted caches are all benchmark candidates. Prefer portable mechanisms when
performance is comparable.

Linux should have an asynchronous I/O path capable of exploiting `io_uring` and
high queue depth. A portable synchronous/threaded fallback remains necessary
for macOS, Windows, embedded use, and correctness tests. Direct/unbuffered I/O is
a server optimization to measure, not a universal default: embedded/local use
may benefit from the OS page cache while large server deployments often benefit
from explicit cache ownership.

The I/O API must expose intent such as random/sequential access, priority,
expected lifetime, and evictability so newer devices and memory tiers can use
that information without changing SQL or transaction code.

### 9. Deployment profiles

The product architecture recognizes these profiles even when a profile is not
yet implemented:

| Profile | Commit durability | Hot capacity | Durable capacity | Object storage |
| --- | --- | --- | --- | --- |
| embedded/local | local log on durable storage | process RAM | local SSD/file | optional backup |
| single-node server | local NVMe log | managed DRAM cache | local NVMe | backup/archive |
| regional HA | quorum replicated log | per-node RAM | local/page-service NVMe | backup/archive |
| disaggregated | replicated log service | compute/cache RAM | cache/page-service NVMe | authoritative checkpoints/archive |
| global | per-range replicated log plus distributed transaction protocol | regional caches | regional storage | archive/bootstrap |

These profiles share logical formats and APIs where doing so does not harm
performance. They need not share one physical checkpoint representation.

## Hardware principles

- Optimize the uncontended path for modern many-core x86-64 and AArch64.
- Treat cache locality and cross-core ownership as first-class costs.
- Use runtime-dispatched vector kernels (portable scalar fallback, NEON/SVE2,
  AVX2/AVX-512 where profitable) rather than compiling the whole database for
  one CPU model.
- Keep I/O queueing explicit enough to overlap CPU and storage work.
- Avoid allocation on per-row/per-key hot paths where bounded arenas or reused
  transaction buffers suffice.
- NUMA/CXL awareness belongs in placement/runtime policy, not logical row or
  transaction formats.
- GPUs are not an OLTP requirement. Future analytical/vector operators may use
  accelerators behind the batch execution boundary.

## Acceptance gates

Before replacing a working storage mechanism, measure it on at least:

- cached point reads and updates;
- larger-than-memory random reads;
- TPC-C/TPC-B-style contended transactions;
- sequential/range scans;
- 1/4/16+ concurrent committers;
- consumer NVMe and datacenter NVMe where available;
- Linux async/direct I/O and portable buffered I/O;
- crash/reopen and torn/ambiguous I/O fault matrices;
- bytes written at the DBMS and device layers;
- CPU, allocations, p50/p95/p99 latency, throughput, and recovery time.

For cloud profiles additionally measure network bytes, object operations,
replicated-log latency, cache hit rate, recovery/bootstrap time, and monetary
cost.

## Research inputs

The direction is informed by, but not coupled to, the following systems/work:

- LeanStore / Umbra — memory-optimized larger-than-memory B-tree engines;
- *B-Trees Are Back* (SIGMOD 2025) — efficient pageable variable-record nodes;
- *Moving on From Group Commit* (SIGMOD 2025) — autonomous commit on NVMe;
- *Predictive Translation* (SIGMOD 2026) — practical low-overhead buffer lookup;
- *How to Write to SSDs* (VLDB 2026) — end-to-end out-of-place SSD writes;
- BtrLog (VLDB 2026) — quorum SSD logging plus asynchronous object archival;
- OceanBase Bacchus (2026) — shared logging/cache services with object storage;
- Neon, Aurora/Socrates, and Aurora DSQL — separation of log durability from
  materialized storage;
- SlateDB — object-native LSM trade-offs;
- FoundationDB — minimal ordered transactional KV layering.

## Consequences

- The current group-commit/page-publication implementation is a qualified
  baseline, not a permanent architecture constraint.
- SeerDB stays useful locally without requiring cloud services.
- Cloud durability does not force remote block storage onto the local fast path.
- Object storage becomes a designed tier rather than an afterthought.
- Physical formats may change repeatedly before a stability promise.
- New hardware can be exploited behind explicit I/O/cache/runtime boundaries
  instead of leaking device assumptions into the relational layer.
