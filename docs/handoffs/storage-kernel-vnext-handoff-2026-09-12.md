# OmenDB storage-kernel vNext handoff — 2026-09-12

## Purpose

Continue the OmenDB redesign and implementation from the current `storage-kernel-vnext` branch without relying on the previous chat. The project is intentionally free to replace existing architecture, internal APIs, and disk formats until correctness and measurements justify stability.

The central conclusion of the research/design pass is that **current SeerDB is a useful correctness/reference implementation but is not the optimal long-term architecture for the OmenDB we now want**. Keep the strongest semantics, tests, fault/recovery machinery, and useful low-level codecs; replace the ownership/concurrency/publication architecture aggressively.

Do not interpret this handoff as permission to preserve something merely because it exists. Conversely, do not rewrite working low-level code when its ownership model still fits. The rule is: **salvage invariants, tests, evidence, and good mechanisms; rewrite structurally wrong boundaries.**

---

## Resume procedure for a fresh session

1. Open `omendb/omendb` and inspect the **current** branch state. Do not assume the SHA in this handoff is still the head.
2. Work on `storage-kernel-vnext` unless the repo has already cut over or a newer explicit plan supersedes it.
3. Read, in order:
   - `docs/architecture.md`
   - `docs/adr/0013-storage-kernel-and-access-methods.md`
   - `docs/plans/storage-kernel-vnext.md`
   - GitHub issue `#1` (“Implement the post-2026 architecture vertical slice”)
   - earlier relevant ADRs: 0001, 0002, 0003, 0006, 0007, 0008, 0009, 0010, 0011, 0012.
4. Check for any repo-level `AGENTS.md` or new instructions before changing code. There was no root `AGENTS.md` on `storage-kernel-vnext` when this handoff was written, but that can change.
5. Check CI on the latest branch head before adding more code. Fix branch-local failures before stacking more architecture on top.
6. Continue the implementation sequence from the “Immediate next work” section below.

Useful qualification commands from CI:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo test --workspace --all-features --all-targets
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo check --locked --workspace --all-features --all-targets
cargo test --locked --workspace --all-features --all-targets
```

The CI also runs the live PostgreSQL differential and a bounded release perf smoke workload.

---

## Current repository state

Repository: `omendb/omendb`

Working branch: `storage-kernel-vnext`

The branch was created from current `main` specifically as temporary migration scaffolding. It is **not** intended to become a permanent alternate engine branch or backend matrix.

Immediately before adding this handoff document, the source-code head was:

```text
9b3ff9bbfd1932435b76cf502f27c37bf3de93db
style(seerdb-vnext): apply rustfmt to frame tests
```

At that point the branch was 15 commits ahead of `main` and 0 behind. The handoff document commit itself will advance the branch by one more commit.

Changed files relative to `main` at the source-code head:

```text
crates/seerdb/src/lib.rs
crates/seerdb/src/vnext/frame.rs
crates/seerdb/src/vnext/ids.rs
crates/seerdb/src/vnext/mod.rs
crates/seerdb/src/vnext/object.rs
crates/seerdb/src/vnext/translation.rs
docs/adr/0013-storage-kernel-and-access-methods.md
docs/plans/storage-kernel-vnext.md
```

The architecture/design work from the broader redesign (ADRs 0006–0012, architecture updates, multimodal/HTAP direction, etc.) is already on `main`; ADR 0013 and the first vNext code are on `storage-kernel-vnext`.

### CI state at handoff

The first translation-head run (`815ed2d...`) established that:

- MSRV `cargo check` and tests passed;
- perf smoke passed;
- the live PostgreSQL differential passed;
- Linux/macOS “rust” jobs stopped at `cargo fmt --check` only.

The rustfmt output identified formatting changes in `vnext/frame.rs`, `vnext/object.rs`, and `vnext/translation.rs`. Those were applied in the subsequent commits ending at `9b3ff9bb...`.

Latest CI run at the moment this document was prepared was queued for `9b3ff9bb...`. **First action in the next session: inspect that run.** If red, fix it before proceeding.

---

# Architectural conclusion

## SeerDB should remain, but its role changes

Do **not** continue treating SeerDB as “the generic ordered transactional KV database underneath OmenDB” where everything must cross this universal boundary:

```text
TreeId + ordered key bytes + opaque value bytes
```

That narrow waist is excellent for systems such as FoundationDB whose goal is a generic transactional KV substrate with independent higher-level layers. It is too restrictive for an integrated database targeting:

- very low-overhead OLTP on RAM + NVMe;
- compact canonical row storage;
- scalar primary/secondary indexes;
- vector ANN + exact fallback;
- BM25/full-text;
- graph projections/traversal;
- JSON/document indexing;
- HTAP/columnar representations;
- incremental views;
- local, HA, disaggregated, and later distributed deployments;
- one transaction/log authority across authoritative state.

The new definition is:

> **SeerDB is the shared transaction/storage kernel. Ordered KV is one first-class facade/access method, not the universal physical model.**

Conceptually:

```text
                         OmenDB
                           |
                transaction/storage kernel
            /        |         |          \
      ordered      canonical   search/    derived
      B-tree        rows       vector     structures
         |            |          |           |
         +------------+----------+-----------+
                           |
                  MVCC / log / buffer
                           |
                    RAM -> NVMe -> archive
```

The kernel owns transaction state, durability/logging, page/frame lifetime, buffer management, recovery, storage-object lifetime, retention, and physical resource policy. It must **not** acquire SQL table/column/NULL/vector/BM25/graph/query-planner semantics.

See ADR 0013 for the authoritative statement.

---

# What remains valid from old SeerDB

Keep or adapt, subject to the new ownership model:

- explicit distinct identities:
  - `TxnId` = transaction identity;
  - CSN / `CommitSeq` = logical visibility order;
  - LSN = durable log position;
- snapshot/change-stream semantics;
- crash-state and ambiguous-I/O discipline;
- fault injection and process kill/reopen methodology;
- `durable-fs` primitives and durability-class tests;
- WAL framing/checksum/truncation ideas that do not assume generation publication;
- B-tree **node/page codec** and corruption/property tests as an initial codec;
- out-of-place SSD direction;
- logical vs physical version separation;
- page-map/checkpoint encoding ideas useful for out-of-place materialization;
- large-value/blob codec ideas and tests;
- current semantic reference model;
- PostgreSQL differential suite;
- pgbench/TPC-B and YCSB baselines;
- recovery/fault matrices;
- the atomic `{snapshot CSN, restart LSN}` export contract.

None of those imply keeping the current `DB`, `BufferManager`, `BTree`, or transactional `Runtime` ownership architecture.

---

# What should be replaced rather than polished

## Current buffer manager

Current implementation uses a single mutable buffer manager with a `HashMap<PageCacheKey, frame>` and clock eviction, normally protected through higher-level mutex ownership. It is not the target.

vNext requires:

- concurrent frame metadata;
- direct guard-owned page lifetime;
- no global buffer mutex on cache hits;
- dirty eviction/writeback;
- an explicit page-translation seam;
- object-local and eventually NUMA/tier-aware placement;
- measurable translation overhead.

## Current B-tree ownership

Current `BTree` owns `Vec<Option<Arc<Node>>>` and uses `Arc::make_mut` / logical tree cloning for publication staging. This is fundamentally tied to the generation-COW model.

vNext B-tree should instead own only access-method/root/object metadata and traverse/mutate guarded pages supplied by the buffer kernel.

Initial concurrency direction:

- safe baseline first;
- optimistic reads/version validation;
- hybrid/exclusive mutation guards;
- latch coupling / B-link/Foster-style alternatives should be measured, not assumed;
- do not introduce unsafe page borrowing until the frame lifetime state machine is proven.

## Current transactional Runtime

Current runtime still has global DB/version/status/change/prepare/publish mutexes and a serialized publication lane. The new architecture should separate:

- transaction status/visibility;
- conflict and wait/intents;
- durability scheduling;
- log order;
- page materialization;
- checkpointing/recovery;
- derived structure catch-up.

Ordinary reads/writes must not funnel through one global DB mutex.

## Generation publication as commit authority

This is the biggest storage change.

Target:

```text
transaction prepares logical authoritative mutations
            |
         validation
            |
   durable transaction log decision
       {CSN, LSN}
            |
      transaction visible
            |
 dirty pages / physical structures materialize later
            |
        checkpoint bounds replay
```

A committed transaction must remain committed even if the process dies before the dirty B-tree/page images are flushed. Recovery reconstructs logical state from checkpoint + durable log.

Physical page publication is not the transaction visibility authority.

---

# Authoritative vs derived storage objects

This is an important vNext distinction.

Every storage object has an authority class.

## Authoritative

Loss or lag would change committed semantics. Mutations must be represented in the transaction’s durable recovery information before acknowledgement.

Examples:

- canonical rows/row families;
- primary index if required to locate canonical rows;
- unique/constraint indexes;
- catalog/schema objects;
- any current-snapshot structure for which no exact fallback exists.

## Derived

Carries a covered CSN/catalog frontier and may lag/rebuild/catch up from authoritative state plus committed changes.

Likely examples:

- HNSW/ANN graphs;
- some BM25 acceleration state;
- columnar analytical chunks/projections;
- graph adjacency/CSR projections;
- zone maps/blooms;
- optional materialized search structures.

“Rebuildable” does not automatically mean “derived.” If SQL correctness requires a structure synchronously, either make it authoritative or provide an exact delta/fallback path.

This lets vector/search/HTAP share the transaction/buffer/log kernel without every derived index delaying commits.

---

# Current vNext code already implemented

The implementation is intentionally small and compile-isolated.

## `vnext/ids.rs`

Adds kernel identities independent of access-method semantics:

- `StorageObjectId(u64)`;
- logical `PageId(u64)`;
- object-scoped `PageKey { object, page }`;
- process-local `FrameId(usize)`.

`TxnId`, `CommitSeq`, and `Lsn` are re-exported from the existing proven identity domains rather than duplicated.

Important: the plan mentions deciding whether stable IDs should prohibit zero. The current code does **not** yet enforce nonzero IDs. Do not accidentally treat that as settled.

## `vnext/object.rs`

Adds:

```text
ObjectAuthority::{Authoritative, Derived}
StorageObjectDescriptor
```

This is intentionally minimal. Do not grow it into a speculative runtime plugin registry.

## `vnext/frame.rs`

Defines a metadata-only frame lifecycle before page bytes are exposed.

States:

```text
Free -> Loading -> Resident
                    |    \
                    |     -> Evicting -> Free
                    -> Writeback -> Resident
```

Dirty is a separate bit rather than a state.

Current properties:

- pins prevent eviction/writeback transition;
- optimistic frame versions use even=stable / odd=writer-owned;
- `FramePin::optimistic_version()` captures stable version;
- `FramePin::validate()` verifies unchanged image;
- `FramePin::try_write()` obtains exclusive version ownership;
- obtaining a write latch marks the frame dirty automatically;
- a second writer receives `WriteBusy`;
- writeback is initially conservative: it refuses while any pins are live;
- successful writeback clears `dirty` **before** transitioning back to `Resident`, preventing a new pin from observing the frame as resident with stale dirty state;
- failed writeback reopens `Resident` while keeping dirty state;
- clean unpinned frames can enter eviction.

Do not assume the “no pins during writeback” rule is final. It is the correctness baseline. A later implementation can copy/write a stable image under a page latch while readers continue, if measured complexity is justified.

## `vnext/translation.rs`

Adds the first page-translation baseline:

```text
(StorageObjectId, PageId) -> FrameId
```

Current baseline:

- 64 shards by default;
- each shard is `RwLock<HashMap<PageKey, FrameId>>`;
- page keys are object scoped;
- stale-safe `remove_if(key, expected_frame)` so delayed eviction cannot remove a newer mapping;
- concurrent parallel-update test;
- diagnostic `len`/`is_empty`.

This is **not** a permanent choice. It exists so end-to-end buffer/B-tree measurements have a concrete baseline.

Candidate alternatives explicitly left open:

- sharded/custom optimistic hash;
- predictive/validated translation (SIGMOD 2026 lineage);
- page/index hints or pointer swizzling where justified;
- relation/object-local direct placement;
- direct-array/vmcache-style paths where deployment constraints fit;
- NUMA/tier-aware placement.

Do not select a winner from an isolated microbenchmark. The B-tree/row end-to-end path decides.

---

# Immediate next work

This is the next session’s priority. Do not jump ahead to vector/HTAP or the final WAL format before this vertical slice exists.

## 1. Confirm CI

Inspect the latest run on `storage-kernel-vnext` after the rustfmt fixes. If anything is red, fix it first.

## 2. Implement the safe concurrent frame-content / buffer baseline

Goal: integrate `FrameMeta`, `PageKey`, `FrameId`, and `TranslationTable` into a real in-memory buffer path **without unsafe borrowed page views yet**.

Suggested first shape:

```text
BufferPool
  fixed frame slots
    FrameId
    FrameMeta
    page identity / generation token
    safe page-byte container / latch baseline
    eviction reference bit / policy metadata

  TranslationTable<PageKey -> FrameId>
  free/victim selection
  read/load interface
  dirty/writeback queue or synchronous fake writeback for first tests
```

Requirements:

- cache hit does not take a single global buffer mutex;
- a miss reserves a free/victim frame as `Loading` before I/O;
- a page mapping is published only after bytes + identity are initialized and frame is `Resident`;
- duplicate concurrent misses for the same page must not publish two live authoritative translations without a deterministic winner/cleanup path;
- stale frame references must be detectable (frame identity alone may be reused, so consider a frame incarnation/generation token before returning long-lived handles);
- eviction removes translation conditionally and cannot invalidate a live pin;
- dirty frame cannot be silently discarded;
- failed load/writeback returns to a recoverable state;
- start with safe locks around page bytes; optimize only after profiling.

A fake/in-memory page device is appropriate for the first buffer tests so page lifetime can be tested independently from real filesystem I/O.

### Instrumentation from day one

Track at least:

- hits/misses;
- translation lookups/retries;
- latch/version retries;
- load coalescing / duplicate misses;
- pins;
- eviction attempts/refusals;
- dirty frames;
- writeback attempts/failures;
- bytes read/written;
- frame occupancy.

Later add ns/cycles once benchmark harness is stable.

## 3. Port B-tree read-only lookup onto guarded frames

Once buffer fundamentals are green, reuse the current proven 4 KiB slotted `Node` codec initially, but **do not** reuse the old `BTree` ownership model.

Target first access-method object:

```text
BTreeObject
  StorageObjectDescriptor
  root PageKey/PageId
  allocator/root metadata

lookup(key):
  pin root through buffer
  inspect node
  route to child PageId
  release/couple guards safely
  repeat
```

Acceptance for first B-tree slice:

- point lookup only;
- read-only page loading from fake device / simple device seam;
- corrupted node fails closed;
- lookup results match current B-tree/reference model;
- no `Vec<Option<Arc<Node>>>` tree ownership;
- no generation clone needed for reads.

Then add:

- insert/update/delete;
- split propagation;
- forward range cursor;
- structural concurrency;
- dirty marking independent from durability.

Only after this buffer+B-tree path is measurable should the new transaction log path be layered on top.

---

# Subsequent implementation sequence

Follow `docs/plans/storage-kernel-vnext.md`, broadly:

### Phase A — kernel/frame/translation
Already started. Finish real buffer baseline.

### Phase B — page-resident B-tree
Read-only lookup -> mutation -> splits -> range cursors.

### Phase C — log-authoritative transaction state machine
Target semantic shape:

```text
Active
  -> Validating / Waiting
  -> Prepared
  -> DurableDecision { CSN, LSN }
  -> Visible
  -> Released
```

One durable transaction decision covers every authoritative access-method mutation.

Local durability scheduler remains benchmark-selected among:

- autonomous/parallel commit;
- group commit;
- adaptive batching.

The semantic outcome `{CSN, durable LSN}` must not depend on scheduler.

### Phase D — MVCC + contention
Do not blindly port the current persistent per-key before-image/version store as the only hot path.

Benchmark:

1. current durable per-key version chains;
2. Umbra-like memory-optimized common version metadata with the log as recovery truth;
3. hybrid/persistent fallback for long snapshots, eviction, and large transactions.

Contention target:

- cold keys remain optimistic;
- hot keys can get lightweight queued writer ownership;
- READ COMMITTED waits then refreshes/rechecks rather than generating pathological retries;
- fixed-snapshot modes retain serialization failure where semantically required;
- serializable certification remains layered above.

### Phase E — canonical rows + cross-object atomicity
Implement ADR 0010 compact schema-driven row records.

Do **not** assume row placement yet. Benchmark at least:

- clustered primary B-tree containing compact row/family payload;
- row/heap pages addressed by primary B-tree.

Prove one transaction can atomically update:

- canonical row;
- scalar secondary index;
- unique/constraint index;
- catalog/object metadata;

under one durable commit decision, including kill/reopen at every log boundary.

### Phase F — OmenDB cutover
Create a temporary adapter only long enough to run the existing product oracle:

- typed relational tests;
- live PostgreSQL SQL/wire differential;
- SQLite trace differential if still present;
- DDL/schema/constraint tests;
- dump/restore;
- process crash matrix;
- pgbench/TPC-B;
- YCSB;
- add TPC-C-style locality/contended-row workload.

Once correctness passes and intended regimes show no material regression, cut normal OmenDB to vNext and **delete the old engine path**. Do not maintain two storage engines indefinitely.

---

# Row / execution / runtime redesign after kernel cutover

The broader OmenDB redesign is still active and should shape kernel seams.

## Rows

Current tag-per-`Value` row storage is bootstrap-grade. Target row layout is schema-driven and compact, e.g.:

```text
layout version
flags
null bitmap
offset directory
typed payload
```

Goals:

- fixed-width fields decode without heap allocation;
- variable-width data can be borrowed behind a storage/frame guard;
- avoid storing a type tag for every field when schema already determines type;
- avoid duplicating PK fields in row payload if key already contains them unless intentionally covering;
- one row family by default;
- optional additional families/large-value placement only when benchmarks show benefit.

## Runtime

Long-term server target:

- cloneable concurrent database handle;
- no `Arc<RwLock<RelationalDatabase>>` as user-query concurrency protocol;
- no per-operation Tokio `spawn_blocking` as final CPU scheduler;
- fixed bounded workers;
- home-worker affinity for short OLTP;
- stealable tasks for scans/joins/index builds;
- guaranteed progress capacity for durability/recovery/reclaim;
- concrete memory/I/O/spill/CPU admission accounting.

## Planner/execution

Binder owns semantic truth:

- parameter types;
- output schema;
- statement effects;
- catalog-object dependencies;
- typed logical plan.

Remove SQL-prefix statement classification and dummy-query Describe execution.

One typed IR feeds:

- OLTP micro-plans;
- vectorized typed batch pipelines;
- optional low-overhead JIT only if profiling justifies it.

---

# Multimodal direction

Do not create sibling databases for every modality. The product principle is:

> **One canonical transactional state; many specialized physical representations/access methods.**

## Vector

Old repo: `omendb/omendb-vector`

Do not merge its independent WAL/store/manifest architecture.

Salvage:

- exact kNN oracle;
- recall harness;
- OOD datasets;
- filtered-search benchmark matrix;
- HNSW code only if still competitive;
- SQ8/quantization experiments;
- BM25/filter/hybrid ranking tests;
- RRF/weakest-link research;
- segment-native ANN ideas.

Target in OmenDB:

- transactional vector column/type;
- exact SIMD kNN correctness oracle;
- HNSW first likely RAM ANN baseline;
- predicate-aware filtered ANN;
- quantized traversal + exact canonical rerank;
- DiskANN/PAG-family SSD layouts when corpus exceeds RAM;
- ANN derived state carries covered CSN/catalog frontier;
- exact delta/fallback preserves current-snapshot semantics.

## Full text / BM25

Likely native access method rather than separate search DB.

Planner should combine:

```text
structured SQL predicates
+ BM25/text
+ vector ANN/exact
+ graph/path operators
+ fusion/rerank
```

in one snapshot/plan.

## Graph

Do not build a separate graph storage engine first.

Start with SQL/PGQ-style property-graph definitions over relational vertex/edge tables. Add recursive/path operators. Only add adjacency/CSR-like derived projections when graph workloads justify them.

## JSON/document

Implement PostgreSQL-compatible JSON/JSONB-like type and path/value/existence indexes. Do not fork into an independent document-store transaction engine.

## Time-series/geospatial

Treat primarily as types + access/placement policies:

- partitioning/clustering;
- zone metadata;
- retention/compression;
- spatial indexes;
- specialized execution only where measured.

## Raw KV

Keep the generic ordered transactional KV surface as a SeerDB compatibility/standalone facade. Do not add a separate Redis/FoundationDB personality to OmenDB merely to claim another model.

---

# HTAP / `omendb-olap`

Old repo: `omendb/omendb-olap`

Do **not** merge its independent database engine wholesale. Its current independent WAL + SQLite manifest + Parquet segment authority conflicts with the new one-transaction/log-truth architecture.

Salvage:

- Arrow/Parquet interop;
- segment publication/compaction failure tests;
- streaming/spill/resource-budget lessons;
- benchmark datasets/methodology;
- immutable segment verification patterns;
- query execution evidence.

New HTAP work should live inside the OmenDB monorepo and use the authoritative OmenDB transaction/log frontier.

Initial lower-risk target:

```text
authoritative rows + log
        |
 derived compressed column chunks / projections
        |
 typed batch executor
```

Every analytical representation carries covered CSN/catalog version.

Also preserve the option to evolve toward a CedarDB/Colibri-like single-copy hot-row/cold-column hybrid if CH-benCHmark-style mixed workloads prove it better than row source + derived projection.

Remote analytical workers remain optional for workload isolation/scale and bootstrap through the same `{snapshot CSN, restart LSN}` contract.

---

# Storage/deployment profiles

Do not force one physical storage policy onto every deployment.

Target profiles:

| Profile | Commit durability | Hot tier | Materialized/capacity tier | Object storage |
|---|---|---|---|---|
| embedded/local | local durable log | RAM / OS cache | local file/SSD | backup optional |
| single-node server | local NVMe log | managed DRAM | local NVMe | backup/archive |
| regional HA | quorum replicated log | per-node RAM | local/page-service NVMe | checkpoint/archive |
| disaggregated | replicated log service | compute/cache RAM | cache/page-service NVMe | checkpoint/archive |
| future global | per-range replicated log + distributed tx only when needed | regional caches | regional materialization | archive/bootstrap |

Object storage should be treated as an immutable/high-latency tier for:

- checkpoints;
- archived log/history;
- backups;
- cold immutable analytical chunks;
- replica bootstrap;
- large immutable artifacts.

Do not make S3-like object storage the fine-grained random-write OLTP substrate by default.

CXL/remote memory should remain placement/runtime policy. Do not encode CXL semantics into row/page formats.

---

# Distribution direction (later, not first vNext milestone)

Distribution should not tax local mode.

Long-term unit: logical ordered **ranges** + placement groups, not physical page ranges.

A single-range/local transaction should use the same fast local path. Cross-range transactions allocate distributed coordination only when actually needed.

Online range movement reuses the existing atomic snapshot/log frontier:

```text
snapshot CSN X + restart LSN Y
    -> copy snapshot X
    -> replay changes after Y
    -> catch up
    -> fence old owner / advance epoch
    -> publish topology
    -> reclaim old placement safely
```

Do not implement distribution before the local kernel earns it.

---

# Research references and the design lesson from each

These are references, not architectures to copy wholesale.

## Umbra / LeanStore / CedarDB

Primary references for integrated modern single-node architecture, memory-efficient MVCC, buffer management, B-tree design, larger-than-memory execution, and HTAP/hybrid row-column ideas.

Important lesson: an integrated DB can profit from tighter cross-layer physical co-design than a universal opaque KV boundary allows.

## “B-Trees Are Back” / modern pageable B-tree work

Supports keeping B-trees as the default ordered local structure for RAM+NVMe rather than reflexively switching to an LSM.

## Predictive Translation (SIGMOD 2026 lineage)

Do not hard-code pointer swizzling or hash translation as universal truth. Translation strategy is a measured seam; predictive/validated paths can reduce buffer translation overhead in some regimes.

## “Moving on From Group Commit” / autonomous commit

Group commit is not a hardware law on fast NVMe. Benchmark autonomous/parallel commit and adaptive batching. Keep semantic commit outcome independent from scheduling policy.

## “How to Write to SSDs” / out-of-place DB/SSD co-design

Reinforces out-of-place physical writes, lifetime-aware placement, reduced write amplification, and treating SSD behavior as a co-design target.

## BtrLog / cloud durability research

Supports replicated/quorum log on the critical commit path plus object storage/checkpoints for cheaper immutable capacity.

## FoundationDB

Strong evidence that ordered transactional KV is a valuable public/standalone narrow waist, especially for distributed layering. But OmenDB should not force all internal physical structures through it.

## Neki / Multigres

Useful for PostgreSQL-facing topology, placement groups/table groups, routing, data movement, and the operational problems of distribution. OmenDB should use native OmenDB plan fragments rather than proxying into independent PostgreSQL planners/processes.

## PgRust

Useful execution references: vectorized push execution, very low-overhead JIT, work stealing/cache-aware algorithms, pipelined durability/early contention release concepts. Do not redefine OmenDB as “Postgres rewritten in Rust.”

## Turso / modern common-core work

Useful evidence for a Rust database core with multiple protocol/dialect frontends and MVCC evolution. PostgreSQL-facing compatibility need not imply PostgreSQL internals.

## Aurora DSQL / Neon / disaggregated systems

Strong evidence that durable journal/log authority can be separated from asynchronous storage materialization and compute replacement.

## FASTER / Garnet

Useful for hot-state residency and RAM->SSD->cloud tier thinking. Do not replace OmenDB’s ordered primary structure with an unordered hybrid-log model just because it excels at cache/KV workloads.

## pgvector / Qdrant / DiskANN / PAG-family work

Vector is an access-method/planner problem with strong interactions with filters and storage tier. Exact oracle and rerank are crucial. Filter-aware traversal matters.

## ParadeDB / BM25 systems

Supports deeply integrated full-text search over canonical relational data rather than a separate search DB.

## SQL/PGQ / DuckPGQ

Supports graph semantics over relational vertex/edge tables and graph-specific execution/projections rather than mandatory independent graph storage.

---

# Explicitly unresolved choices

Do not accidentally “finish” these choices in documentation without measurements.

- 4 KiB vs 8/16/64 KiB vs variable/adaptive node/page size.
- clustered PK rows vs heap/row pages + primary B-tree.
- exact buffer-translation default.
- std `RwLock<HashMap>` vs custom/sharded optimistic/lock-free translation.
- clock vs CLOCK-Pro/other eviction policy.
- writeback snapshot/copy model vs conservative pin exclusion.
- B-link vs Foster/Bw-tree-like/conventional split coordination.
- persistent per-key undo vs memory-optimized MVCC vs hybrid.
- retry OCC vs adaptive waiting thresholds.
- group vs autonomous vs adaptive commit.
- buffered vs direct I/O; `io_uring` paths.
- page compression/packing.
- FDP/ZNS usage.
- single-copy hybrid HTAP vs derived columnar projections.
- HNSW vs newer ANN variants per regime.
- when BM25/index structures should be authoritative vs derived+delta.
- exact crate split (`seerdb`, `exec`, `search`, `analytic`) beyond the first kernel.

Crate/package independence must not dictate the database optimization boundary.

---

# Performance/correctness acceptance gates

Every serious replacement needs correctness evidence **and** before/after measurements.

At minimum record:

- throughput;
- p50/p95/p99;
- user/system CPU, cycles/op where possible;
- allocations count/bytes;
- memory footprint;
- buffer occupancy/hit/miss;
- translation/latch retries and cost;
- conflict/retry/wait statistics;
- logical log bytes;
- host bytes written;
- NAND/flash writes where SMART exposes them;
- recovery time vs checkpoint/log distance;
- database/checkpoint size.

ANN work additionally records:

- recall@k;
- build/update cost;
- memory/index size;
- filtered recall and latency;
- exact-rerank overhead;
- OOD datasets, not only easy in-distribution benchmarks.

Cloud/distributed work additionally records:

- network bytes;
- object operations;
- bootstrap/catch-up time;
- replication/reshard cost;
- explicit cost where meaningful.

Workloads should include:

- cached point read/write;
- larger-than-memory random reads;
- sequential/range scans;
- pgbench/TPC-B differential;
- TPC-C-style locality/contention;
- deliberately hot keys;
- 1 / 4 / 16+ committers;
- CH-benCHmark-style mixed OLTP/analytics;
- vector exact-vs-ANN + filtering;
- BM25/hybrid retrieval;
- graph/path traversal later.

Run Linux x86-64/NVMe and AArch64 where practical.

---

# Things not to do

- Do not optimize the old generation-COW engine into permanence.
- Do not build a permanent engine/backend matrix.
- Do not make every access method encode itself as generic KV just to preserve the old abstraction.
- Do not throw away the current correctness/fault/test corpus by starting a new product repo.
- Do not merge `omendb-olap` or `omendb-vector` wholesale; salvage evidence/algorithms/tests.
- Do not add distribution before the local engine is strong.
- Do not make object storage the random-write local OLTP medium by default.
- Do not choose an LSM merely because object-native systems use one; local ordered relational workloads still strongly favor a well-designed B-tree baseline.
- Do not add unsafe borrowed page APIs before frame/eviction/version invariants are proven and covered by tests.
- Do not design a universal plugin ABI/trait hierarchy before two concrete access methods demonstrate the necessary common interface.
- Do not expand SQL breadth while the core storage/runtime architecture is being replaced unless a new SQL feature is specifically required to validate the design.
- Do not call the new engine “SOTA” based on architecture alone. Earn that claim with measurements.

---

# Definition of success for the next major checkpoint

The vNext kernel is ready for OmenDB cutover when all of the following are true:

1. Shared concurrent buffer/page kernel works without one global user-operation mutex.
2. Ordered B-tree reads/writes/range scans operate on guarded buffer pages, not an owned COW tree clone.
3. Durable transaction log decision is sufficient to recover committed logical state before page materialization.
4. Multi-writer MVCC semantics match the reference model.
5. One transaction atomically updates multiple authoritative storage objects/access methods.
6. Compact canonical rows + scalar indexes run the existing OmenDB relational suite.
7. Existing crash/fault/reopen matrix passes.
8. PostgreSQL oracle/differential passes.
9. Intended OLTP workloads show no material regression and preferably major improvement over current SeerDB.
10. Larger-than-memory behavior is credible rather than optimized only for cache-resident benchmarks.
11. Old-vs-new results are documented.
12. Old generation-COW/current transactional implementation is deleted after cutover instead of becoming permanent legacy baggage.

Only then should integrated vector/BM25/HTAP access methods become the main implementation focus.

---

## Suggested first instruction in the new session

Use this handoff plus the live repository as context. Inspect current `storage-kernel-vnext`, current CI, ADR 0013, `docs/plans/storage-kernel-vnext.md`, and issue #1 first. Continue the vNext implementation rather than merely discussing it. Fix any existing branch failures, then implement the safe concurrent frame-content/buffer baseline and begin porting read-only B-tree lookup over guarded pages. Keep the current engine as a correctness oracle only; do not preserve its generation-COW/opaque-KV ownership architecture by default. Benchmark-gate physical choices and update the plan when research or measurements justify changing direction.
