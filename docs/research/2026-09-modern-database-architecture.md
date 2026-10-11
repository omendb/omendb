# Modern database architecture research — September survey and October update

**Status:** research input, not architecture or performance qualification. The
September survey informed ADRs 0006–0008; ADR 0013 subsequently replaced the
universal ordered-KV boundary. Local-kernel mechanisms and pgrust source were
rechecked on 2026-10-11. Dated vendor observations below are not automatically
current; recheck a source when its claim becomes implementation-critical.

## Executive synthesis

The strongest direction across current systems and recent database research is
not one universal storage architecture. It is a stable transaction/query model
combined with deployment-aware physical policy:

- RAM is the latency tier, not the only capacity tier;
- local NVMe remains an excellent OLTP capacity and log device;
- out-of-place B-tree storage is a strong local default on modern SSDs;
- local NVMe can favor autonomous/parallel log commit instead of traditional
  group commit;
- HA/disaggregated systems benefit from separating a replicated durable log
  from page/materialization services;
- object storage is excellent for immutable checkpoints, history, backups, and
  archival, but poor as a fine-grained random-write OLTP substrate;
- object-native LSMs are valuable for cost-first/disaggregated profiles, not a
  reason to make every local database an LSM;
- ordered transactional KV is a useful facade, but a shared kernel may host
  specialized row/index layouts without making every representation an opaque KV;
- a modern server should own sessions, worker scheduling, memory/I/O admission,
  and typed execution rather than recreate process-per-connection behavior;
- global scale should partition the logical ordered keyspace into ranges and
  keep single-range transactions on the local fast path.

These observations produced:

- ADR 0006 — deployment-aware storage and durability;
- ADR 0007 — runtime, execution, and contention;
- ADR 0008 — distribution, ranges, and global topology.

## Neki / PlanetScale

Sources:

- <https://planetscale.com/blog/introducing-neki> (2026-09-10)
- <https://planetscale.com/blog/what-is-a-neki-router> (2026-09-01)
- <https://planetscale.com/blog/what-is-a-data-topology> (2026-08-17)
- <https://planetscale.com/blog/the-lifecycle-of-a-sharded-postgres-query>
  (2026-09-10)

Neki is now publicly available as a Platform Preview. Its architecture keeps
**real PostgreSQL on every shard** (one primary plus replicas), while a fleet of
routers exposes one PostgreSQL endpoint and builds a distributed plan. Each
selected PostgreSQL shard then builds its own local PostgreSQL plan. Sidecars
own backend connection pooling and the control plane owns failover, upgrades,
online schema work, and resharding.

Its data topology makes several concepts explicit that remain useful even in a
native engine:

- routing-key transforms separate relational values from physical range
  selection;
- shard groups co-locate related tables;
- topology is live/cached routing state that changes during movement;
- an authoritative unsharded group exists for database-wide metadata;
- users can remain unsharded until scale requires distribution.

For OmenDB, copy the **logical concepts**, not the proxy-over-Postgres cost. A
future OmenDB router/planner should send typed plan fragments to OmenDB nodes,
not SQL that invokes a second independent planner. Owning sessions and storage
also avoids much of the hidden backend-state virtualization required by Neki
and Multigres.

## Multigres / Supabase

Repository: <https://github.com/multigres/multigres>

Relevant current design/work:

- `docs/sharding/design.md`: database -> table group -> shard -> pooler cohort;
  hash/range/lookup/multicol/reference placement;
- connection/session scrubbers for leaked GUCs, prepared statements, advisory
  locks, temp objects, and holdable cursors;
- elastic regular/reserved connection quotas;
- dedicated control-plane connection capacity;
- distributed failover/orchestration work.

The important lesson is the engineering tax of transparent pooling around
stateful PostgreSQL processes. OmenDB should preserve explicit logical session
state and should not create a hidden reusable backend-session abstraction that
must later be scrubbed for correctness.

Multigres's table-group/co-location design is nevertheless a useful reference
for ADR 0008 placement groups.

## pgrust

Sources:

- <https://github.com/malisper/pgrust>
- <https://pgrust.com/blog/how-we-made-postgres-hundreds-of-times-faster-the-query-engine/>
- <https://pgrust.com/blog/jit-compiling-code-in-5-us/>

The v0.3 README and source were checked at
[`79ad992`](https://github.com/malisper/pgrust/tree/79ad992ede22bcf6ae0c4fdead6fb01eeac5a990)
on 2026-10-11. The project still describes itself as not production-ready.
Its multithreaded, vectorized/fused execution remains relevant, but published
performance and conformance counts are author claims, not OmenDB evidence.
The documented JIT performance is architecture-specific; do not copy its
numbers into a portable execution budget.

Two inspected mechanisms provide more precise lessons:

- [`admission.rs`](https://github.com/malisper/pgrust/blob/79ad992ede22bcf6ae0c4fdead6fb01eeac5a990/crates/backend/tcop/postgres/src/admission.rs)
  retains admission across a transaction, rather than parking later statements
  while they hold locks needed by queued work. OmenDB should preserve that
  invariant without copying pgrust's session exemptions or bypass policy.
- [`flushpipe.rs`](https://github.com/malisper/pgrust/blob/79ad992ede22bcf6ae0c4fdead6fb01eeac5a990/crates/backend/access/transam/transam_xlog/src/flushpipe.rs)
  implements directed durable-LSN completion waits and a progress fallback. Its
  module explicitly keeps lock release/visibility/acknowledgment after durability,
  despite broader early-lock-release language in the README. Deferred-tail APIs
  are not proof of an integrated alternative commit protocol.

Benchmark completion scheduling separately from changing transaction ordering.
SeerDB still owes both WAL and undo durability plus contiguous visibility; a
faster wait queue cannot remove any of those obligations. Full PostgreSQL
behavior remains pgrust's goal, while OmenDB deliberately bounds compatibility
at external interfaces.

## Turso

Sources:

- <https://turso.tech/blog/a-new-modern-version-of-postgres-in-rust>
  (2026-07-16)
- <https://github.com/tursodatabase/turso>
- <https://turso.tech/blog> (MVCC/concurrent-write updates through August 2026)

Turso is now a first-class architectural comparator. In July 2026 the project
announced a PostgreSQL frontend compiled onto the same Rust database core as its
SQLite frontend — described as an "LLVM of databases". The direction is a
common database-specific VM/IR with multiple frontends rather than one SQL
parser being the engine boundary.

Relevant lessons for OmenDB:

- keep parser/frontend compatibility separate from the core execution/storage
  machine;
- a typed database IR is a strong boundary for interpretation, vectorization,
  and future cheap JIT tiers;
- async I/O, embedded/file/server operation, CDC, MVCC, and incremental view
  maintenance can live below multiple SQL frontends;
- embedded/local support does not require giving up a server architecture if
  the core is factored correctly.

OmenDB should not copy SQLite's physical constraints merely to gain embedding.
Its target remains a shared transaction/storage kernel with specialized physical
representations, designed first for RAM/NVMe. An ordered-KV facade is one use of
that kernel, not its universal physical model.

## Local OLTP storage engines and current papers

### LeanStore / Umbra and B-trees

References:

- LeanStore publications / code: <https://github.com/leanstore/leanstore>
- "B-Trees Are Back: Engineering Fast and Pageable Node Layouts" — SIGMOD 2025
- "How to Write to SSDs" — PVLDB 2026:
  <https://arxiv.org/pdf/2603.09927> (extended v3, checked 2026-10-11);
  artifact: <https://github.com/LeeBohyun/ZLeanStore>.

The 2026 SSD work is especially strong evidence for retaining SeerDB's
out-of-place direction: converting LeanStore to an out-of-place write design
improves throughput substantially and cuts device writes by multiples across
YCSB/TPC-C, while naturally supporting ZNS/FDP devices.

Takeaway: do not switch to an LSM merely because SSDs/object storage are modern.
A carefully engineered B-tree can be the right local OLTP structure. Page/node
layout and buffer translation remain open measurement questions; the current
4 KiB node is not sacred.

The artifact's placement/GC techniques do not make SeerDB's current append-only
page arena reclaimable. A placement map, retained checkpoints and readers all
constrain image lifetime. Measure logical, host and flash writes separately;
compression/page packing and lifetime-aware placement are experiments after
that reachability contract is qualified. Results obtained with direct-device
I/O or device-specific ZNS/FDP controls do not establish filesystem behavior on
macOS or commodity buffered I/O.

### Autonomous commit

Source:

- "Moving on From Group Commit: Autonomous Commit Enables High Throughput and
  Low Latency on NVMe SSDs" — SIGMOD 2025:
  <https://2025.sigmod.org/toc-3-3.html>

Modern NVMe exposes enough write parallelism that serial group-commit
acknowledgement can become the bottleneck. Autonomous commit lets workers issue
smaller durable log writes independently and parallelizes commit-state
acknowledgement.

OmenDB consequence: ADR 0004 group commit remains a qualified fallback/current
implementation, but local NVMe must benchmark autonomous/parallel and adaptive
commit policies before the architecture is frozen.

### Predictive Translation

Reference:

- "Predictive Translation: High-Performance Buffer Management Without the
  Trade-Offs" — SIGMOD 2026:
  <https://db.in.tum.de/~zinsmeister/papers/predictive-translation.pdf>.
- Reproducible PrediCache artifact:
  <https://github.com/umbra-db/predictivetranslation-sigmod2026>.
  Paper and artifact checked 2026-10-11; the artifact is Linux-oriented and is
  not a demonstrated portable replacement for SeerDB's translation baseline.

The work targets the page-id -> buffer-frame lookup overhead that becomes
visible when storage is fast and the working set is large. Its broader lesson
is more important than one exact algorithm: page translation is hot-path CPU
work on modern NVMe, so SeerDB must profile and benchmark translation designs
instead of assuming a generic hash table is negligible.

### Hybrid MVCC and large transactions

"Swan: Hybrid MVCC Management for Efficient Transaction Processing in
LSM-Tree-Based Key-Value Stores", PVLDB 19(9), 2026:
<https://www.vldb.org/pvldb/vol19/p1977-guo.pdf>;
artifact: <https://github.com/LAccordeur/swan>. Paper checked 2026-10-11.

Swan separates common small transactions from large writes to trade visibility
lookup cost against memory/commit cost. Its stash and memtable mechanisms are
LSM-specific and do not justify replacing SeerDB's B-tree. The useful experiment
is narrower: compare the persistent-undo baseline with memory-optimized recent
versions plus a bounded durable fallback, under large writes, long snapshots,
abort/recovery and pressure. Neither alternative may acknowledge before its
required durability or reclaim a version still needed by a reader.

CedarDB's [2026-10-08 release notes](https://cedardb.com/docs/releases/#v2026-10-08)
report undo-segment reuse and reduced recovery memory. Those are unmeasured
vendor observations here, but reinforce tracking retained-history memory and
recovery bytes rather than measuring only foreground commit throughput.

## Cloud/disaggregated OLTP

### Cloud OLTP architecture/cost study

Source:

- Haubenschild & Leis, "OLTP in the cloud: architectures, tradeoffs, and cost",
  VLDB Journal 2025:
  <https://doi.org/10.1007/s00778-025-00913-z>

The paper compares classic local storage, in-memory, HADR, remote block, Aurora-
like, and Socrates-like architectures using workload/performance/durability
constraints and actual cloud hardware economics.

Important conclusions for OmenDB:

- no one physical deployment is cost-optimal for every workload;
- local NVMe is extremely attractive for OLTP performance;
- object storage is cheap/durable but unsuitable as primary random OLTP storage
  because latency and request cost are too high;
- object storage is the obvious archival/backup tier;
- a promising cloud-native combination is **NVMe page cache backed by object
  storage plus a separate redundantly replicated WAL/log service**.

That combination is the core of ADR 0006's regional/disaggregated profiles.

### BtrLog

Source:

- "BtrLog: Low-Latency Logging for Cloud Database Systems" — VLDB 2026:
  <https://vldb.org/2026/program.html>

BtrLog replicates appends to a quorum of SSD-backed log nodes in one network
round trip, then asynchronously archives large segments to object storage. This
is a strong reference for an OmenDB regional durability service because it
keeps object-store request latency completely off the commit path.

### Neon

Sources:

- <https://neon.com/storage>
- <https://github.com/neondatabase/neon/blob/main/docs/pageserver-storage.md>

Neon separates PostgreSQL compute from WAL safekeepers and SSD pageservers;
immutable page-history layers are uploaded to object storage and can be fetched
back into SSD caches. The useful OmenDB lesson is the tier split, not Postgres
page semantics: durable log, materialized/read-optimized SSD state, immutable
object-store history.

### OceanBase Bacchus

Source:

- <https://arxiv.org/abs/2602.23571>

Bacchus is an important counterexample to the local B-tree direction: for
object-storage-centric shared storage it deliberately uses an LSM, a shared
Paxos append-log service, and a shared block-cache service so compute can remain
stateless and cache/storage/log can scale independently.

OmenDB consequence: an object-native/cost-first profile may eventually warrant
a different physical materializer. It does **not** justify making an LSM the
universal SeerDB local layout.

## Ordered KV and distributed SQL

### FoundationDB

Sources:

- <https://apple.github.io/foundationdb/technical-overview.html>
- <https://apple.github.io/foundationdb/layer-concept.html>

FoundationDB demonstrates that ordered binary keys, opaque values and true
transactions can support higher-level data models. It also demonstrates the
value of deterministic simulation and explicit conflict ranges. That is evidence
for a useful facade, not a requirement that every OmenDB physical representation
use it. SQL/table/index meaning stays above SeerDB; specialized physical access
methods share its transaction, log, buffer and lifetime services.

### CockroachDB / TiKV / Yugabyte-style ranges

References:

- CockroachDB design: <https://github.com/cockroachdb/cockroach/blob/master/docs/design.md>
- TiKV architecture: <https://tikv.org/docs/4.0/concepts/architecture/>

The recurring useful primitive is an ordered keyspace divided into contiguous
ranges/regions/tablets, each independently replicated and movable. That is the
basis of ADR 0008.

OmenDB differs by preserving a stronger local engine first and adding the range
layer only when distribution is enabled. It should not pay one Raft group/range
routing tax in embedded/single-node mode.

## Runtime / hardware references

### ScyllaDB / Seastar

Source:

- <https://www.scylladb.com/product/technology/shard-per-core-architecture/>

Shard-per-core demonstrates the upside of ownership, cache locality, explicit
message passing, and per-core resource scheduling. OmenDB should adopt the
locality lesson but not permanently partition every ordered tree by CPU: ADR
0007 instead uses home-worker affinity for short work plus stealable batch tasks.

### TigerBeetle

Sources:

- <https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/ARCHITECTURE.md>
- <https://docs.tigerbeetle.com/concepts/performance/>

Useful principles:

- everything has a bound;
- static/reused memory removes allocator/tail-latency noise;
- direct asynchronous I/O and explicit in-flight operation limits;
- cache-line-aware compact structures and batching;
- deterministic state-machine design and simulation-friendly effects.

OmenDB should borrow those engineering disciplines without copying
TigerBeetle's single-threaded/accounting-specific execution model.

## Hardware matrix to benchmark

Before stable physical formats or scheduler policy, run representative tests
across:

| Environment | Key questions |
| --- | --- |
| laptop/embedded, buffered file I/O | page-cache vs explicit buffer ownership; tiny-memory behavior |
| Apple Silicon / AArch64 local SSD | allocation/cache behavior; portable async fallback; SIMD |
| Linux consumer NVMe | buffered vs O_DIRECT; sync classes; io_uring queue depth |
| datacenter NVMe | group vs autonomous/adaptive commit; larger queue depth; FDP/ZNS when present |
| RAM-heavy server | translation overhead and cache locality when almost everything is resident |
| larger-than-memory server | eviction policy, read amplification, write amplification, tail latency |
| regional 3-node HA | quorum-log latency/stragglers, follower reads, rebuild/catch-up |
| disaggregated | log service, SSD page cache/materializer, object-store checkpoint economics |
| multi-region | locality, WAN commit latency, failover, witness/quorum placement |

Metrics should always include throughput, p50/p95/p99, CPU, allocations,
DB/device bytes written, memory residency, recovery time, and—where relevant—
network/object requests and monetary cost.

## Current OmenDB implications

The existing architecture contains several correct foundations that should
survive redesign: distinct TxnId/CSN/LSN identities, logical MVCC separate from
physical page versions, a shared transaction/storage kernel, external PostgreSQL
compatibility boundaries, and out-of-place local storage. Retained snapshot plus
change-stream handoff is a target contract, not something established by the
current position-only `snapshot_export` API.

The following are explicitly transitional and should be replaced/benchmarked:

- database-wide `Arc<RwLock<RelationalDatabase>>` in pgwire;
- `spawn_blocking` per query instead of the real bounded worker runtime;
- retry-only hot-key OCC behavior for READ COMMITTED workloads;
- full-table/index scans for routine FK enforcement;
- whole-catalog marker as the long-term DDL invalidation unit;
- row-oriented `Vec<Row>` analytical execution and offset-rescanning morsels;
- executing dummy-value queries during Describe/type inference;
- assuming group commit is optimal on every local SSD;
- assuming 4 KiB pages, current translation, and current buffer policy are
  stable-format decisions;
- treating object storage as merely backup rather than a designed immutable
  checkpoint/history/bootstrap tier.

The target remains intentionally revisable until measurements and recovery
qualification establish the design rather than merely making the current code
hard to replace.
