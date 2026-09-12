# Storage-kernel vNext implementation plan

**Branch:** `storage-kernel-vnext`  
**Architecture:** ADR 0013  
**Goal:** replace the current generation-COW / opaque-KV-centered SeerDB implementation with a shared transaction, log, buffer and physical-storage kernel that can host multiple access methods without losing the existing correctness oracle.

## Strategy

This is a **replacement implementation inside the existing repository**, not a greenfield product repo and not a permanent engine matrix.

The current implementation remains runnable until vNext passes the same semantic, crash/fault and benchmark gates. Reuse proven semantics and test oracles where useful; do not wrap structurally wrong ownership merely to reduce diff size.

Research and implementation qualification changed one earlier assumption: vNext no longer carries the legacy fixed-4-KiB generation-oriented B-tree codec as its native page format. The old B-tree remains the semantic differential oracle, while vNext uses a variable-size slotted B-link format designed around guarded buffer ownership, right-link split correction and future measured prefix/head/hint optimization.

The transaction rewrite follows the same rule. Existing transaction/MVCC semantics remain the oracle, but vNext has its own transaction-scoped logical WAL, durable-decision framing and visibility primitives rather than importing generation/root-publication records into the new kernel.

## Current implementation status

Implemented on `storage-kernel-vnext`:

- vNext logical identities (`StorageObjectId`, 64-bit `PageId`, frame IDs/incarnations) and authoritative/derived object classification;
- atomic frame lifecycle and pin state, exact-incarnation translation references, frame-local page guards, dirty state and writeback/eviction transitions;
- sharded concurrent `PageKey -> FrameRef` translation with no global buffer mutex on cache hits;
- stale-safe duplicate load publication and direct installation of newly allocated dirty pages;
- writeback/install race handling, including lookup waiting during transient writeback instead of removing a valid translation and reloading stale physical bytes;
- native v4 ordered B-link pages with dynamic page size, slotted variable records, cached four-byte key heads, high fences, right sibling links and lazy compaction;
- native vNext B-tree point lookup, leaf-local insert, delete, byte-balanced split propagation, root replacement and forward range traversal;
- key-restart resumable range cursor that re-descends from the current root instead of retaining stale page/slot position across eviction or splits;
- page-writer contention resolved at the guard layer rather than leaked into access methods;
- randomized old-vs-vNext mutation differential across multiple dynamic page sizes, small-buffer eviction/range stress, concurrent read/write/split stress and structural fault coverage for delayed root promotion;
- transaction-scoped vNext logical WAL records with versioned framing, CRC32C, complete/incomplete/corrupt suffix classification, exact record-end offsets and fail-closed unknown kinds/versions/flags;
- recovery assembly that accepts interleaved transactions but emits only validated commits with contiguous mutation ordinals, exact count/digest, increasing LSNs and increasing CSNs;
- explicit transaction phases including preappend `Prepared`, `WalAppended`, durable decision, visibility, abort and `RecoveryRequired` for ambiguous outcomes;
- scheduler-neutral append/sync durability seam that fences after uncertain append/sync failures and serializes only baseline physical WAL operations across the fence;
- segmented local WAL device with whole-transaction segment rotation, durable-fs barriers, directory fsync, final torn-tail truncation, retained-segment gap detection and exact-LSN recovery scan;
- ordered commit append lane coupling CSN assignment to physical WAL append order while leaving fsync, MVCC application and visibility publication outside the lane;
- compact MVCC current/undo record envelope with owner `TxnId | frozen CSN`, a sharded transaction-status table and own-write/committed/active/aborted/newer-snapshot resolution;
- contiguous visibility frontier so out-of-order completion cannot let a new snapshot cross an unpublished earlier CSN.

The temporary vNext v3 compatibility decoder has been removed. The old production tree/transaction engine remains available only as the correctness and recovery oracle until cutover.

Not implemented yet:

- true atomic raw-B-tree upsert/current-record replacement required for idempotent logical redo;
- page-local structural-modification coordination replacing the temporary per-object split mutex, if measurement justifies doing it before cutover;
- an optimized frame byte latch/read path replacing `std::sync::RwLock` if measurement justifies it;
- background/asynchronous dirty queues and writeback scheduling;
- the physical page-integrity/materialization envelope (checksum/page LSN/out-of-place physical mapping/checkpoint integration);
- persistent or hybrid before-image/version-store integration for vNext MVCC;
- transaction write intents/conflict validation and visible snapshot reads over undo chains;
- end-to-end log-authoritative application/recovery into authoritative access methods;
- canonical row storage and OmenDB cutover.

## Salvage matrix

### Preserve or adapt

- `storage::format` identity types and validated framing patterns (`TxnId`, CSN, LSN, `VersionId`), while vNext owns `StorageObjectId`, 64-bit logical `PageId` and process-local frame identities.
- `durable-fs` sync/fault primitives and existing durability-class tests.
- WAL framing/checksum/truncation ideas where they do not assume generation publication; vNext owns its actual transaction record kinds and physical WAL device.
- deterministic fault points and process kill/reopen methodology.
- legacy B-tree behavior/tests as a differential oracle, not as vNext page ownership or page format.
- the existing complete-before-image `mvcc::version_store` as a correctness/fallback implementation and benchmark candidate, not automatically the only vNext version layout.
- blob/large-value codec ideas and tests; placement/lifecycle will be redesigned with the new page/object kernel.
- page-map/checkpoint encoding ideas that support out-of-place physical placement.
- current transaction semantic reference model, snapshot/change-stream tests, pgbench differential, YCSB probes and recovery matrices.

### Rewrite rather than wrap

- **Buffer ownership:** the old manager owns a mutable map/clock and is normally hidden behind `StorageEngine`'s mutex. vNext owns concurrent frame metadata, guards, translation, eviction and writeback directly.
- **PageGuard semantics:** vNext guards protect resident frame bytes and exact frame incarnation. Access methods do not reach through a mutable global manager.
- **B-tree ownership:** the old `Vec<Option<Arc<Node>>>` + `Arc::make_mut` generation tree is not the vNext structure. vNext owns only object/root/allocation metadata and operates on guarded pages.
- **B-tree page format:** the legacy v3 fixed-4-KiB page is not carried forward. v4 is variable-sized and B-link aware; compression/fence truncation/hints remain benchmarkable format choices before stability.
- **Transactional runtime:** the current global DB/version/status/change/prepare/publish mutex model is replaced. vNext may keep a narrowly scoped ordered WAL append lane because physical append order and CSN order are one invariant, but it does not serialize validation, page work, fsync or query execution behind that lane.
- **Generation publication:** manifests/root generations stop defining every commit. Checkpoints/page-map publication bound replay; the durable transaction decision defines commit.
- **Persistent-per-key MVCC as the only common path:** retain semantics as reference/fallback, but benchmark memory-resident version metadata for short OLTP with log-backed recovery.

## Target module shape

The implementation lives under temporary `seerdb::vnext` migration scaffolding while the old engine remains the oracle. The concrete module tree is allowed to grow only with real implementation seams; current vNext now includes IDs/object/frame/translation/buffer, native B-tree/cursor, logical WAL + physical segmented device, transaction/recovery, MVCC status/envelope and visibility-frontier modules.

Do not create speculative plugin/trait hierarchies. Add a seam when a second real implementation or measurement target requires it.

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

Instrumentation includes hit/miss, translation retries/stale mappings, writeback/latch waits, dirty/occupied/pinned frames, eviction/refusals, loads/new pages/writebacks and bytes read/written.

Next buffer work is benchmark/materialization-driven:

- once real transaction/page LSNs exist, replace standalone dirty authority with a measured page/materialized-LSN eligibility model rather than inventing a fake LSN from frame version state;
- background dirty queues + bounded materialization workers;
- replace `std::sync::RwLock` page-byte latching only if a custom hybrid/operation-aware latch wins end-to-end;
- admission/progress policy when all candidate frames are pinned or structurally required;
- object-local placement/scan hints;
- predictive/validated translation, pointer/hint-assisted paths and direct-array candidates only after baseline measurement;
- NUMA/tier placement later.

Do not select a universal translation/latch winner from isolated microbenchmarks; B-tree/row end-to-end workloads decide defaults.

## Milestone C — page-resident B-link tree

**Status: correctness baseline substantially qualified; format/performance and SMO policy remain experimental.**

Implemented and qualified in the current test matrix:

- point lookup through buffer-owned guards;
- leaf-local insert and delete;
- B-link high-fence/right-sibling correction;
- byte-balanced leaf/internal splits;
- split propagation and atomic root replacement;
- delayed root-promotion recovery that preserves an already-published B-link sibling chain after a physical write failure;
- dynamic page sizes instead of a fixed 4 KiB format;
- cached four-byte key heads and a format seam for future prefix/fence truncation;
- lazy compaction of fragmented pages;
- forward range traversal over leaf links;
- resumable key-restart cursor safe across eviction, page movement and root splits;
- randomized old-vs-vNext mutation traces across 384–2048-byte pages and variable value sizes;
- range-vs-`BTreeMap` tests under a six-frame eviction regime;
- concurrent readers/ranges while disjoint writers repeatedly split under low residency;
- structural fault injection around failed root promotion;
- a structural-only per-object mutex as a correctness baseline for rare split propagation, while ordinary reads and leaf-local writes remain page-local.

Remaining C work:

1. Keep full CI/Clippy/MSRV green as transaction/MVCC layers begin consuming the tree.
2. Add true atomic upsert/current-record replacement so logical redo can be idempotent without delete-then-insert crash windows.
3. Decide, from measurement, whether to replace the structural mutex with page-local operation-aware SMO coordination before cutover. The page format/search protocol must not depend on the coarse mutex.
4. Benchmark prefix truncation/restart points, fence truncation, slot heads/hints and page sizes before format stabilization.
5. Put checksum/page-LSN/physical integrity in the generic materialization layer rather than reintroducing legacy generation/checksum authority into the access-method header.

The current v4 format remains experimental and may be rewritten aggressively if measurements favor a different layout.

## Milestone D — log-authoritative transactions

**Status: durable log/ordering/recovery foundations implemented; transaction application and crash-qualified visibility remain incomplete.**

Current logical phase model distinguishes clean/pre-I/O and outcome-uncertain states:

```text
Active -> Validating -> Prepared -> WalAppended -> DurableDecision -> Visible -> Released
                 \          \             \
                  -> Aborted  -> RecoveryRequired
```

Implemented foundations:

- transaction-scoped logical mutation records keyed by authoritative `StorageObjectId`;
- separate durable commit decision `{TxnId, CSN, mutation_count, mutation_digest}`;
- versioned length/CRC framing with exact record-end offsets and torn-vs-corrupt suffix classification;
- recovery assembler that emits only fully validated committed transactions;
- physical append separated from `sync_through` so multiple appends can share one durability barrier;
- fence-after-uncertain-append/sync behavior;
- local segmented WAL with whole-transaction rotation, directory durability and reopen/torn-tail repair;
- short ordered append lane coupling CSN assignment to physical commit-decision order under concurrent committers;
- `WalAppended`/`RecoveryRequired` transaction phases so a commit that may have entered the WAL cannot be incorrectly aborted in-process.

Still required before D is complete:

1. True idempotent logical redo into authoritative access methods; raw B-tree upsert is the first prerequisite.
2. WAL-first/current-record application protocol with failpoints around every install/decision/status boundary.
3. Conflict/write-intent validation before append.
4. Recovery replay into access-method state plus process kill/reopen qualification.
5. WAL-before-data/page-LSN eligibility in the out-of-place materialization layer.
6. Checkpoint/replay-frontier integration and log retention.
7. Group/autonomous/adaptive durability scheduling measurements above the same append/sync contract.

The durable transaction decision, not page flush or root-generation publication, remains commit authority.

## Milestone E — MVCC/contention

**Status: visibility ownership/status/frontier contract implemented; version-store/current-record integration remains.**

Implemented contract:

- current/undo records encode `RecordOwner::{Transaction(TxnId), Frozen(CSN)}` and an `undo_head`;
- transaction status is sharded process-local state rebuilt from WAL decisions;
- own active transaction sees its writes;
- other readers resolve transaction-owned records as active/aborted/committed-at-CSN;
- older snapshots see newer committed owners as `NewerCommit` and must follow undo;
- one status publication changes visibility for every record owned by that transaction across objects;
- a contiguous visibility frontier prevents new snapshots from crossing a gap when later commits finish status work before an earlier CSN.

Next E work:

1. Integrate complete before-images/version chains with the vNext record path. Use the existing append-oriented version store as correctness/fallback baseline, but keep the layout replaceable and benchmark memory-resident metadata for short OLTP.
2. Add write-intent/conflict ownership so concurrent writers cannot replace the same current record before validation.
3. Add snapshot lookup/range traversal that follows undo until it finds a visible version or absence.
4. Add status/version freezing and retention-aware GC once visibility is proven.
5. Preserve serializable dependency certification as a layer above the fixed-snapshot MVCC core.

## Milestone F — canonical rows and cross-object atomicity

Implement ADR 0010 row records against vNext.

Do not assume the physical row organization. Benchmark at least:

- clustered primary B-tree with compact row/family payloads;
- row/heap pages referenced by a primary B-tree.

Secondary scalar indexes are B-tree access-method objects.

Acceptance proof: one transaction atomically changes a row, secondary index, unique/constraint index and catalog/object metadata under one durable commit decision, including kill/reopen at every log/application/status boundary.

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

1. Keep the newly combined B-tree/WAL/MVCC/commit-order slice green under stable, MSRV, Clippy, PostgreSQL differential and perf smoke; fix surfaced invariants rather than relaxing tests.
2. Add atomic B-tree upsert/current-record replacement and prove idempotent put/delete replay across repeated recovery application.
3. Integrate the MVCC record envelope with a complete-before-image version-store baseline and write-intent/conflict validation.
4. Prove `install txn-owned records -> append/sync decision -> status publication -> contiguous visibility frontier` with multi-object tests and crash/fault injection at every boundary.
5. Introduce the physical materialization/integrity envelope for checksum, real page LSN, out-of-place page placement, logical-to-physical mapping and checkpoint/replay frontier.
6. Only then optimize durability batching, frame latching, translation, page compression/fence truncation and structural split coordination from end-to-end measurements.

This sequence keeps the rewrite measurable and reviewable while retaining freedom to replace any implementation choice that does not survive correctness or performance qualification.
