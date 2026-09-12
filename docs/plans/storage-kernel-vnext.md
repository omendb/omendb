# Storage-kernel vNext implementation plan

**Branch:** `storage-kernel-vnext`  
**Architecture:** ADR 0013  
**Goal:** replace the current generation-COW / opaque-KV-centered SeerDB implementation with a shared transaction, log, buffer and physical-storage kernel that can host multiple access methods without losing the existing correctness oracle.

## Strategy

This is a **replacement implementation inside the existing repository**, not a greenfield product repo and not a permanent engine matrix.

The current implementation remains runnable until vNext passes the same semantic, crash/fault and benchmark gates. We will reuse proven code where its ownership model still fits; we will not wrap structurally wrong components merely to reduce diff size.

The first vertical slice deliberately keeps some physical choices conservative (for example the existing 4 KiB node codec may be reused initially) so we can isolate the architectural improvement before benchmarking variable/adaptive pages, different translation schemes and alternate row placement.

## Salvage matrix

### Preserve or adapt first

- `storage::format` identity types and validated framing patterns (`TxnId`, CSN, LSN, page/version/checkpoint IDs), while adding vNext `StorageObjectId` and a wider stable logical `PageId` if required.
- `durable-fs` sync/fault primitives and the existing durability-class tests.
- WAL framing/checksum/truncation utilities where they do not assume generation publication.
- deterministic fault points and process kill/reopen test methodology.
- B-tree **node/page codec** and its corruption/property tests as an initial page format, subject to later page-size/layout replacement.
- blob/large-value codec ideas and tests; placement/lifecycle will be redesigned with the new page/object kernel.
- page-map/checkpoint encoding ideas that support out-of-place physical placement.
- current transaction semantic reference model, snapshot/change-stream tests, pgbench differential, YCSB probes and recovery matrices.

### Rewrite rather than wrap

- **BufferManager ownership:** current manager owns a single mutable `HashMap<PageCacheKey, frame>` + clock and is normally hidden behind `StorageEngine`'s mutex. vNext needs concurrent frame metadata, frame-local latching/version state, dirty eviction/writeback and a replaceable translation fast path.
- **PageGuard semantics:** current guard pins by token but actual bytes are still accessed through the mutable manager. vNext guards directly protect and expose resident frame bytes/typed page views with optimistic validation or exclusive modification.
- **BTree ownership:** current `BTree` owns `Vec<Option<Arc<Node>>>` and `Arc::make_mut` provides copy-on-write staging. vNext B-tree owns only root/object metadata and traverses/mutates pages through the buffer manager using optimistic latch coupling/hybrid guards.
- **Transactional Runtime:** current `Runtime` has global DB/version/status/change/prepare/publish mutexes and a serialized publication lane. vNext separates transaction status, conflict/intents, log scheduling and page materialization so ordinary operations do not funnel through one DB mutex.
- **Generation publication:** manifests/root generations stop defining every commit. Checkpoints/page-map publication bound replay; the durable transaction decision defines commit.
- **Persistent-per-key MVCC as the only common path:** retain its semantics as reference/fallback, but benchmark memory-resident version metadata for short OLTP with log-backed recovery.

## Target module shape

The initial implementation lives under a temporary `seerdb::vnext` namespace so the old engine and its tests remain available during qualification.

```text
crates/seerdb/src/vnext/
  mod.rs
  ids.rs              # StorageObjectId, PageId, FrameId, object authority
  object.rs           # storage-object metadata/lifecycle
  txn/                # transaction state/status/snapshot/intents
  log/                # record framing, commit decision, durability scheduler
  buffer/             # frames, guards, translation, eviction, dirty/writeback
  io/                 # async/sync page device abstraction + out-of-place placement
  checkpoint/         # page map/checkpoint/recovery frontier
  access/
    btree/             # first authoritative access method
    row/               # compact canonical row path once B-tree kernel is stable
  compat/
    ordered_kv.rs      # future TransactionDatabase compatibility facade
```

Do **not** create every directory or trait on day one. Add modules only when the vertical slice needs them; this tree is ownership guidance.

## Milestone A — kernel identities and frame contract

Deliverables:

1. `StorageObjectId` (64-bit, nonzero or explicitly validated), `PageId` (stable logical identity), `FrameId`, `ObjectAuthority::{Authoritative, Derived}`.
2. Reuse existing `TxnId`, `CommitSeq`, `Lsn` rather than duplicate ordering domains.
3. Define page/frame lifecycle states independent of B-tree semantics: free/loading/resident/dirty/writeback/evicting (exact representation benchmarkable).
4. Define the guard invariant:
   - a borrowed page/record view cannot outlive its guard;
   - optimistic reads validate a version before accepting bytes;
   - a write guard owns mutation/version advancement;
   - eviction/writeback cannot invalidate a live guard.
5. Unit/property tests for ID overflow, guard lifetime/state transitions and stale writeback refusal.

No disk-format commitment yet.

## Milestone B — concurrent buffer manager

Start with a practical baseline, then preserve translation as a measured seam.

Baseline:

- fixed frame array;
- per-frame atomic/versioned latch state;
- concurrent translation table sharded by page/object identity;
- CLOCK/second-chance or CLOCK-Pro-like eviction baseline;
- dirty-page queue + asynchronous/worker writeback;
- object-local page placement metadata;
- no global buffer mutex on cache hits.

Instrumentation from the first commit:

- hit/miss;
- translation ns/cycles;
- preferred-frame hit rate if prediction is enabled;
- latch retries;
- dirty/writeback queue depth;
- evictions/refusals;
- bytes read/written;
- NUMA/tier location later.

Then benchmark alternatives:

- predictive/validated translation (SIGMOD 2026 lineage);
- relation/object-local placement + bypass fast path;
- index-resident hints/pointer swizzling where it earns its page-format cost;
- vmcache/direct-array candidates only where platform/deployment constraints fit.

Do not select a universal winner from microbenchmarks alone; B-tree and row end-to-end results decide the default.

## Milestone C — page-resident B-tree

Rewrite B-tree routing around guarded pages instead of an owned `Vec<Arc<Node>>` tree.

Initial scope:

- point lookup;
- insert/update/delete;
- forward range cursor;
- optimistic latch coupling for normal descent;
- hybrid/exclusive guards for structural changes;
- split propagation;
- prefix/key compression only if current codec reuse makes it free; otherwise preserve correctness first;
- page-level dirty marking without synchronous persistence.

The first version may reuse the current 4 KiB slotted `Node` codec to reduce simultaneous variables. Once the new ownership path is qualified, benchmark:

- 4/8/16/64 KiB or variable-size nodes;
- prefix compression/restart points;
- page packing/compression;
- Foster/B-link/other split/merge policies under contention.

## Milestone D — log-authoritative transactions

Implement one transaction state machine above the kernel:

```text
Active -> Validating/Waiting -> Prepared -> DurableDecision(CSN, LSN) -> Visible -> Released
```

Required properties:

- commit durability no longer waits for ordinary page flush/checkpoint;
- one durable decision covers all authoritative object mutations;
- redo is sufficient to reconstruct committed logical state from checkpoint + log;
- transaction status is separate from physical page reachability;
- unknown/corrupt record kinds fail closed;
- derived objects do not delay commit unless SQL semantics require them synchronously.

Local durability scheduler is pluggable internally and benchmarked among:

- autonomous/parallel commit;
- group commit;
- adaptive batching.

The semantic result `{CSN, durable LSN}` is invariant across schedulers.

## Milestone E — MVCC/contention

Preserve the current visibility oracle while replacing the physical common path.

Compare:

1. current durable per-key before-image/version chains;
2. memory-optimized version metadata associated with resident records/pages, with the log as recovery truth and persistent fallback for large/evicted histories;
3. hybrid thresholds for long snapshots and large write transactions.

Contention:

- cold/uncontended keys stay optimistic;
- hot keys may gain lightweight queued writer ownership;
- READ COMMITTED can wait then refresh/recheck;
- fixed-snapshot modes retain serialization failure where required;
- serializable dependency certification remains layered above.

## Milestone F — canonical rows and cross-object atomicity

Implement ADR 0010 row records against vNext.

Do not assume the physical row organization. Benchmark at least:

- clustered primary B-tree with compact row/family payloads;
- row/heap pages referenced by a primary B-tree.

Secondary scalar indexes are B-tree access-method objects.

Acceptance proof: one transaction atomically changes a row, secondary index, unique/constraint index and catalog/object metadata under one durable commit decision, including kill/reopen at every log boundary.

## Milestone G — OmenDB differential cutover

Wire a temporary OmenDB adapter to vNext and run the existing product oracle unchanged where possible:

- typed relational tests;
- live PostgreSQL SQL/wire differential;
- SQLite trace differential;
- schema/constraint tests;
- dump/restore;
- process crash matrix;
- pgbench/TPC-B;
- YCSB and new TPC-C-style workload.

Only after this passes do we migrate the normal OmenDB path and remove the old storage implementation.

## Performance gates

Every serious design choice records at least:

- throughput + p50/p95/p99;
- CPU user/system and cycles/op where available;
- allocation count/bytes;
- memory footprint and buffer occupancy;
- latch/conflict/retry/wait statistics;
- logical WAL/log bytes;
- host bytes written;
- SSD NAND/flash writes where SMART exposes them;
- cache/translation metrics;
- recovery time versus log distance;
- database size and checkpoint overhead.

Run hot/cached and larger-than-memory regimes. A design that wins only when everything is in RAM is not sufficient; a design that optimizes SSD throughput while imposing large cached-hit overhead is also not sufficient.

## First implementation sequence

The next code changes should be deliberately small and compile independently:

1. Add `vnext` namespace with IDs/object authority and tests.
2. Add frame state + guard state machine without any B-tree dependency.
3. Add a concurrent translation/buffer baseline against an in-memory fake page device.
4. Port the current B-tree node codec behind guarded frames for read-only lookup first.
5. Add mutation/split support.
6. Only then add the new log/transaction commit path.

This order lets us profile the buffer/B-tree path before transaction complexity obscures it and keeps every commit reviewable.
