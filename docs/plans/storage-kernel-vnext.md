# Storage-kernel vNext implementation plan

**Branch:** `storage-kernel-vnext`  
**Architecture:** ADR 0013  
**Goal:** replace the current generation-COW / opaque-KV-centered SeerDB implementation with a shared transaction, log, buffer and physical-storage kernel that can host multiple access methods without losing the existing correctness oracle.

## Strategy

This is a **replacement implementation inside the existing repository**, not a greenfield product repo and not a permanent engine matrix.

The current implementation remains runnable until vNext passes the same semantic, crash/fault and benchmark gates. Reuse proven semantics and test oracles where useful; do not wrap structurally wrong ownership merely to reduce diff size.

Research and the first implementation slices changed one earlier assumption: vNext no longer carries the legacy fixed-4-KiB generation-oriented B-tree codec as its native page format. The old B-tree remains the semantic differential oracle, while vNext now has a variable-size slotted B-link page format designed around guarded buffer ownership, right-link split correction and future measured prefix/head/hint optimization.

## Current implementation status

Implemented on `storage-kernel-vnext`:

- vNext logical identities (`StorageObjectId`, 64-bit `PageId`, frame IDs/incarnations) and authoritative/derived object classification;
- atomic frame lifecycle and pin state, exact-incarnation translation references, frame-local page guards, dirty state and writeback/eviction transitions;
- sharded concurrent `PageKey -> FrameRef` translation with no global buffer mutex on cache hits;
- stale-safe duplicate load publication and direct installation of newly allocated dirty pages;
- writeback/install race handling, including lookup waiting during transient writeback instead of removing a valid translation and reloading stale physical bytes;
- native v4 ordered B-link pages with dynamic page size, slotted variable records, cached four-byte key heads, high fences, right sibling links and lazy compaction;
- native vNext B-tree point lookup, leaf-local insert, delete, byte-balanced split propagation, root replacement and bounded forward range traversal;
- page-writer contention resolved at the guard layer rather than leaked into access methods;
- split-heavy old-vs-vNext differential tests, concurrent unique-writer tests, page-format invariant tests and existing workspace/PostgreSQL differential coverage.

The temporary vNext v3 compatibility decoder has been removed. The old production tree itself remains available only as the correctness oracle until cutover.

Not implemented yet:

- resumable guard-aware range cursors;
- page-local structural-modification coordination replacing the temporary per-object split mutex;
- an optimized frame byte latch/read path replacing `std::sync::RwLock` if measurement justifies it;
- background/asynchronous dirty queues and writeback scheduling;
- the physical page-integrity/materialization envelope (checksum/page LSN/physical mapping/checkpoint integration);
- log-authoritative vNext transactions, recovery and MVCC;
- canonical row storage and OmenDB cutover.

## Salvage matrix

### Preserve or adapt

- `storage::format` identity types and validated framing patterns (`TxnId`, CSN, LSN), while vNext owns `StorageObjectId`, 64-bit logical `PageId` and process-local frame identities.
- `durable-fs` sync/fault primitives and existing durability-class tests.
- WAL framing/checksum/truncation utilities where they do not assume generation publication.
- deterministic fault points and process kill/reopen methodology.
- legacy B-tree behavior/tests as a differential oracle, not as vNext page ownership or page format.
- blob/large-value codec ideas and tests; placement/lifecycle will be redesigned with the new page/object kernel.
- page-map/checkpoint encoding ideas that support out-of-place physical placement.
- current transaction semantic reference model, snapshot/change-stream tests, pgbench differential, YCSB probes and recovery matrices.

### Rewrite rather than wrap

- **Buffer ownership:** the old manager owns a mutable map/clock and is normally hidden behind `StorageEngine`'s mutex. vNext owns concurrent frame metadata, guards, translation, eviction and writeback directly.
- **PageGuard semantics:** vNext guards protect resident frame bytes and exact frame incarnation. Access methods do not reach through a mutable global manager.
- **B-tree ownership:** the old `Vec<Option<Arc<Node>>>` + `Arc::make_mut` generation tree is not the vNext structure. vNext owns only object/root/allocation metadata and operates on guarded pages.
- **B-tree page format:** the legacy v3 fixed-4-KiB page is not carried forward. v4 is variable-sized and B-link aware; compression/fence truncation/hints remain benchmarkable format choices before stability.
- **Transactional runtime:** the current global DB/version/status/change/prepare/publish mutex model and serialized publication lane are replaced rather than hidden behind a compatibility wrapper.
- **Generation publication:** manifests/root generations stop defining every commit. Checkpoints/page-map publication bound replay; the durable transaction decision defines commit.
- **Persistent-per-key MVCC as the only common path:** retain semantics as reference/fallback, but benchmark memory-resident version metadata for short OLTP with log-backed recovery.

## Target module shape

The implementation lives under temporary `seerdb::vnext` migration scaffolding while the old engine remains the oracle:

```text
crates/seerdb/src/vnext/
  mod.rs
  ids.rs
  object.rs
  frame.rs
  translation.rs
  buffer.rs
  btree/
    mod.rs
    page_v4.rs
    tree_v4.rs
  txn/                # add only when the transaction slice begins
  log/                # add only with the durable-decision slice
  io/                 # add when physical placement outgrows PageIo
  checkpoint/         # add with page-map/recovery frontier
  row/                # add after B-tree/kernel qualification
  compat/             # future ordered-KV facade only if still useful
```

Do not create speculative trait/module hierarchies. Add a seam when a second real implementation or measurement target requires it.

## Milestone A — kernel identities and frame contract

**Status: implemented baseline; continue stress/model qualification.**

Delivered:

1. `StorageObjectId`, 64-bit stable logical `PageId`, `FrameId`, per-slot `FrameIncarnation`, and `ObjectAuthority::{Authoritative, Derived}`.
2. Reuse of existing `TxnId`, `CommitSeq`, `Lsn` rather than duplicate ordering domains.
3. Frame lifecycle independent of B-tree semantics.
4. Guard invariants:
   - borrowed bytes cannot outlive the guard;
   - frame reuse is protected by exact incarnation;
   - writer ownership advances the frame version and marks dirty;
   - eviction/writeback cannot invalidate a live pin;
   - state + pin count transitions that require zero pins are one atomic decision.
5. Unit/concurrency tests for load, pin, writer exclusion, failed writeback, eviction/reuse and stale translation behavior.

Remaining qualification: deterministic/model-based concurrency exploration once the simulation seam is introduced.

## Milestone B — concurrent buffer manager

**Status: functional synchronous baseline implemented; scheduling/physical I/O optimization remains.**

Current baseline:

- fixed frame array;
- atomic frame lifecycle/pin metadata and versioned writer state;
- sharded translation table keyed by object/page identity;
- CLOCK/second-chance victim baseline;
- dirty writeback and eviction correctness;
- direct new-page installation without a device read;
- transient writeback waits rather than stale translation removal;
- anti-ABA `FrameRef = FrameId + incarnation`;
- no global buffer mutex on cache hits.

Instrumentation includes:

- hit/miss;
- translation retries/stale mappings;
- writeback waits;
- latch retries;
- dirty/occupied/pinned frames;
- evictions/refusals;
- loads/new pages/writebacks;
- bytes read/written.

Next buffer work is benchmark-driven:

- replace `std::sync::RwLock` page-byte latching only if a custom hybrid/operation-aware latch wins end-to-end;
- background dirty queues + bounded writeback workers;
- admission/progress policy when all candidate frames are pinned or structurally required;
- object-local placement/scan hints;
- predictive/validated translation, pointer/hint-assisted paths and direct-array candidates only after baseline measurement;
- NUMA/tier placement later.

Do not select a universal translation/latch winner from isolated microbenchmarks; B-tree/row end-to-end workloads decide defaults.

## Milestone C — page-resident B-link tree

**Status: native v4 core implemented; cursor, stronger stress qualification and SMO refinement remain.**

Implemented:

- point lookup through buffer-owned guards;
- leaf-local insert and delete;
- B-link high-fence/right-sibling correction;
- byte-balanced leaf/internal splits;
- split propagation and atomic root replacement;
- dynamic page sizes instead of a fixed 4 KiB format;
- cached four-byte key heads and a format seam for future prefix truncation;
- lazy compaction of fragmented pages;
- bounded forward range traversal over leaf links;
- a structural-only per-object mutex as a correctness baseline for rare split propagation, while ordinary reads and leaf-local writes remain page-local;
- differential and concurrent tests against the old B-tree oracle.

Before treating Milestone C as qualified:

1. Full CI/Clippy/MSRV green on the native v4 path.
2. Randomized/property old-vs-vNext mutation traces across several page sizes and adversarial key/value distributions.
3. Concurrent insert/read/range stress that repeatedly crosses split boundaries.
4. Small-buffer/eviction stress during traversal and structural modification.
5. Resumable range cursor with explicit restart semantics rather than only a materialized bounded helper.
6. Decide, from measurement, whether to replace the structural mutex with page-local operation-aware SMO coordination immediately or after the transaction/log slice. The page format/search protocol must not depend on the coarse mutex.
7. Benchmark prefix truncation/restart points, fence truncation, slot heads/hints and page sizes before format stabilization.
8. Put checksum/page-LSN/physical integrity in the generic materialization layer rather than reintroducing legacy generation/checksum authority into the access-method header.

The current v4 format is still experimental and may be rewritten aggressively if these measurements favor a different layout.

## Milestone D — log-authoritative transactions

**Status: next major semantic slice after Milestone C qualification.**

Implement one transaction state machine above the kernel:

```text
Active -> Validating/Waiting -> Prepared -> DurableDecision(CSN, LSN) -> Visible -> Released
```

Required properties:

- commit durability no longer waits for ordinary page flush/checkpoint;
- one durable decision covers all authoritative object mutations;
- redo reconstructs committed logical state from checkpoint + log;
- transaction status is separate from physical page reachability;
- WAL-before-data/page-LSN eligibility is enforced by the materialization layer;
- unknown/corrupt record kinds fail closed;
- derived objects do not delay commit unless SQL semantics require them synchronously.

Local durability scheduler remains an internal measured policy among autonomous/parallel commit, group commit and adaptive batching. `{CSN, durable LSN}` is invariant across schedulers.

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
- YCSB and TPC-C-style workload.

Only after this passes do we migrate the normal OmenDB path and delete the old storage implementation.

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

Run hot/cached and larger-than-memory regimes. A design that wins only when everything is in RAM is insufficient; a design that optimizes SSD throughput while imposing large cached-hit overhead is also insufficient.

## Immediate sequence

1. Finish native v4 CI/Clippy/MSRV qualification and delete any remaining dead vNext compatibility code.
2. Add randomized differential, concurrent split/range and small-buffer eviction stress.
3. Add the resumable range cursor and explicit access-method restart semantics.
4. Introduce the physical materialization/integrity envelope needed for out-of-place page placement, page LSNs, checksums and checkpoint mapping.
5. Benchmark the current frame byte latch and translation baseline; replace either aggressively if end-to-end results justify it.
6. Begin Milestone D log-authoritative transaction/recovery work only after the page/buffer invariants are stable enough that transaction debugging is not masking storage bugs.

This sequence keeps the rewrite measurable and reviewable while retaining freedom to replace any implementation choice that does not survive correctness or performance qualification.
