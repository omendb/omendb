# Storage-kernel vNext implementation plan

**Branch:** `storage-kernel-vnext`  
**Architecture:** ADR 0013; installation/recovery contract in [ADR 0014](../adr/0014-vnext-installation-and-recovery.md)  
**Goal:** replace the current generation-COW / opaque-KV-centered SeerDB implementation with a shared transaction, log, buffer and physical-storage kernel that can host multiple access methods without losing the existing correctness oracle.

## Strategy

This is a **replacement implementation inside the existing repository**, not a greenfield product repo and not a permanent engine matrix.

The current implementation remains runnable until vNext passes the same semantic, crash/fault and benchmark gates. Reuse proven semantics and test oracles where useful; do not wrap structurally wrong ownership merely to reduce diff size.

Research and implementation qualification changed one earlier assumption: vNext no longer carries the legacy fixed-4-KiB generation-oriented B-tree codec as its native page format. The old B-tree remains the semantic differential oracle, while vNext uses a variable-size slotted B-link format designed around guarded buffer ownership, right-link split correction and future measured prefix/head/hint optimization.

The transaction rewrite follows the same rule. Existing transaction/MVCC semantics remain the oracle, but vNext has its own transaction-scoped logical WAL, durable-decision framing and visibility primitives rather than importing generation/root-publication records into the new kernel.

The integrated baseline is **durable-WAL-first shared current-record installation**. Private staged writes, canonical final effects, intents and validation precede append; all authoritative installs precede status/frontier publication. The two page write-ahead frontiers are necessary but do not replace a structurally complete checkpoint/replay protocol. ADR 0014 resolves the prior contradictory implementation order and defines these boundaries without claiming that the installer or materializer exists.

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
- transaction-scoped vNext logical WAL records with version-2 framing, a fixed-header CRC32C protecting length before tail classification, a whole-record checksum, exact record-end offsets and fail-closed unknown kinds/versions/flags;
- recovery assembly that accepts interleaved transactions but emits only validated commits with contiguous mutation ordinals, exact count/digest, increasing LSNs and increasing CSNs;
- explicit transaction phases including preappend `Prepared`, `WalAppended`, durable decision, visibility, abort and `RecoveryRequired` for ambiguous outcomes;
- scheduler-neutral append/sync durability seam that fences after uncertain append/sync failures and serializes only baseline physical WAL operations across the fence;
- segmented local WAL device with whole-transaction rotation, durable-fs barriers, directory fsync, final torn-tail truncation, retained-segment gap detection and exact-LSN recovery scan; reopened retained segments remain pending synchronization until a new barrier covers them;
- ordered commit append lane coupling CSN assignment to physical WAL append order while leaving fsync, MVCC application and visibility publication outside the lane;
- compact MVCC current/undo record envelope with owner `TxnId | frozen CSN`, a sharded transaction-status table and own-write/committed/active/aborted/newer-snapshot resolution;
- contiguous visibility frontier so out-of-order completion cannot let a new snapshot cross an unpublished earlier CSN;
- append-only vNext undo store for complete `MvccRecord` before-images, with version-2 checksummed framing, contiguous `VersionId`s, strict backward-link validation, torn-final-frame repair, fence-after-uncertain append/sync semantics and an explicit durable `VersionId` frontier; recovery scans bounded individual frames rather than reading the entire retained file into RAM.

The inner MVCC record envelope is still version 1 and has no install identity. Outer vNext WAL/undo version-1 files are rejected; recreate disposable experimental stores or use a separately validated offline migration, not a silent compatibility fallback. The legacy production engine's format is unchanged.

The temporary vNext v3 compatibility decoder has been removed. The old production tree/transaction engine remains available only as the correctness and recovery oracle until cutover.

Not implemented yet:

- true atomic raw-B-tree upsert/current-record replacement required for idempotent logical redo;
- canonical final-write-set normalization shared by live commit and recovery, or MVCC replay/install identity;
- page-local structural-modification coordination replacing the temporary per-object split mutex, if measurement justifies doing it before cutover;
- an optimized frame byte latch/read path replacing `std::sync::RwLock` if measurement justifies it;
- background/asynchronous dirty queues and writeback scheduling;
- the physical page-integrity/materialization envelope, same-image dependency capture, structurally complete checkpoints and persistent mapping publication;
- current-record installation, private read-your-writes integration, snapshot traversal, freezing/GC and hybrid metadata policy over the new vNext undo store;
- transaction write intents/conflict validation and visible snapshot reads over undo chains;
- end-to-end log-authoritative application/recovery into authoritative access methods;
- database/store incarnation binding, directory ownership and checkpointed allocation/status/retention state for the integrated runtime;
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
- **Generation publication:** manifests/root generations stop defining every commit. Structurally complete checkpoints bound replay; the durable transaction decision defines commit. An independently newer eviction image is not recovery authority.
- **Persistent-per-key MVCC as the only common path:** retain semantics as reference/fallback, but benchmark memory-resident version metadata for short OLTP with log-backed recovery.

## Target module shape

The implementation lives under temporary `seerdb::vnext` migration scaffolding while the old engine remains the oracle. The concrete module tree is allowed to grow only with real implementation seams; current vNext now includes IDs/object/frame/translation/buffer, native B-tree/cursor, logical WAL + physical segmented device, transaction/recovery, MVCC status/envelope, append-only undo storage and visibility-frontier modules.

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

- capture real WAL/undo dependencies with the same guarded page bytes; preserve them through reload and splits, and gate materialization on both durability domains;
- keep maximum page dependency LSN distinct from logical replay completion; out-of-order installers on different keys of one page can leave gaps;
- implement checkpoint reference closure before treating individually materialized pages as persistent recovery state;
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
- in-process delayed-root-promotion fault coverage preserving an already-published B-link sibling chain; this is not a durable page-map power-loss proof;
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
2. Add true atomic upsert/current-record replacement without delete-then-insert gaps, preserving duplicate-rejecting `insert` and distinguishing refusal before publication from failure after a split publishes state.
3. Decide, from measurement, whether to replace the structural mutex with page-local operation-aware SMO coordination before cutover. The page format/search protocol must not depend on the coarse mutex.
4. Benchmark prefix truncation/restart points, fence truncation, slot heads/hints and page sizes before format stabilization.
5. Put checksum/page-LSN/physical integrity in the generic materialization layer rather than reintroducing legacy generation/checksum authority into the access-method header.

The current v4 format remains experimental and may be rewritten aggressively if measurements favor a different layout.

## Milestone D — log-authoritative transactions

**Status: durable log/ordering/recovery foundations implemented; transaction application and crash-qualified visibility remain incomplete.**

Current logical phase model distinguishes clean/pre-I/O and outcome-uncertain states:

```text
Active -> Validating -> Prepared -> WalAppended -> DurableDecision -> Visible -> Released
```

Clean refusal can abort before WAL I/O. Once append may have happened, an uncertain failure requires recovery. A post-decision installation failure cannot reverse the durable outcome. `DurableDecision` does not by itself imply that all current records are installed or that a new snapshot may advance.

Implemented foundations:

- transaction-scoped logical mutation records keyed by authoritative `StorageObjectId`;
- separate durable commit decision `{TxnId, CSN, mutation_count, mutation_digest}`;
- version-2 fixed-header/whole-record CRC framing, exact record-end offsets and torn-vs-corrupt suffix classification;
- recovery assembler that emits only fully validated committed transactions;
- physical append separated from `sync_through` so multiple appends can share one durability barrier;
- fence-after-uncertain-append/sync behavior;
- local segmented WAL with whole-transaction rotation, directory durability and reopen/torn-tail repair; reopened bytes require a fresh durability barrier;
- short ordered append lane coupling CSN assignment to physical commit-decision order under concurrent committers;
- `WalAppended`/`RecoveryRequired` transaction phases so a commit that may have entered the WAL cannot be incorrectly aborted in-process;
- standalone append-only complete-before-image storage with its own explicit durable `VersionId` frontier, ready for the materializer to enforce undo-before-data alongside WAL-before-data.

Still required before D is complete:

1. Atomic raw upsert and repeated logical replay qualification under splits/eviction.
2. Shared final-effect normalization, write intents and conflict/admissibility validation before append, including deterministic record/undo/segment limits and bounded installation progress.
3. The ADR 0014 sequence: private writes -> intents/validation -> ordered append -> decision sync -> all current installs -> status publication -> contiguous frontier -> completion/release.
4. Runtime-wide fencing before unresolved intents are released; failpoints at every append/install/status boundary.
5. Same-image WAL/undo dependency capture and materialization eligibility, including inherited split dependencies.
6. Structurally complete checkpoint/replay-frontier integration, durable status/identity state and retention; never recover by combining independently latest pages.
7. Process kill/reopen qualification, then group/autonomous/adaptive durability scheduling measurements above the same append/sync contract.

Materialization invariant: a captured page image must not become durable unless the transaction WAL frontier covers its real WAL dependency LSN and every referenced undo version is at or below the undo store's durable `VersionId` frontier. The page/materialization layer owns this gate; frame version or a boolean dirty flag is not a substitute. These conditions do not prove that all earlier logical effects were installed, nor that a split's reachable graph is complete.

Use a coherent checkpoint plus committed logical WAL suffix as the first recovery model. A quiescent checkpoint is the correctness baseline, not a production pause target; nonblocking checkpoint epochs or structural logging must earn their replacement with fault and performance qualification. Uncheckpointed out-of-place eviction images are working spill state, not independent durable authority.

The durable transaction decision, not page flush or root-generation publication, remains commit authority.

## Milestone E — MVCC/contention

**Status: visibility ownership/status/frontier plus standalone durable before-image storage implemented; current-record integration remains.**

Implemented primitives:

- current/undo records encode `RecordOwner::{Transaction(TxnId), Frozen(CSN)}` and an `undo_head`;
- transaction status is sharded process-local state rebuilt from WAL decisions;
- the owner resolver supports own-write visibility; the integrated private write overlay is still pending;
- other readers resolve transaction-owned records as active/aborted/committed-at-CSN;
- older snapshots see newer committed owners as `NewerCommit` and must follow undo;
- one status publication can change visibility for every record owned by that transaction across objects, once the installer has completed them;
- a contiguous visibility frontier prevents new snapshots from crossing a gap when later commits finish status work before an earlier CSN;
- the vNext `UndoStore` persists complete encoded `MvccRecord` before-images behind stable contiguous `VersionId`s, validates strict backward links, revalidates records on read, repairs an incomplete final frame, fails closed on complete corruption, and exposes a group-syncable durability frontier.

Next E work:

1. Normalize the validated original mutation stream into canonical unique-key final effects, retaining the final ordinal. Share that view between live commit and recovery; acquire intents before reading/replacing predecessors.
2. Define replay identity with final-effect semantics before changing the MVCC envelope. Repeated completed installs must not append duplicate history; crashes between undo append and current replacement may leave reclaimable unreachable undo rather than falsely promising allocation-free recovery.
3. Wire current replacement to the `UndoStore` using durable-WAL-first installation and the raw atomic primitive. Complete all objects before status/frontier publication. Keep tests transient until materialization/checkpoint gates exist.
4. Add private read-your-writes plus snapshot point lookup, then range traversal through the same undo resolver.
5. Preserve tombstones and install evidence through the replay horizon. Checkpoint needed owner outcomes or freeze safely before reclaiming WAL; implement lease-aware status/version GC only after these invariants are proven.
6. Benchmark whether short-lived histories should remain in resident metadata before spilling to durable undo; logical visibility must not depend on that optimization.
7. Preserve serializable dependency certification as a layer above the fixed-snapshot MVCC core.

## Milestone F — canonical rows and cross-object atomicity

Implement ADR 0010 row records against vNext.

Do not assume the physical row organization. Benchmark at least:

- clustered primary B-tree with compact row/family payloads;
- row/heap pages referenced by a primary B-tree.

Secondary scalar indexes are B-tree access-method objects.

Acceptance proof: one transaction atomically changes a row, secondary index, unique/constraint index and catalog/object metadata under one durable commit decision, including kill/reopen at every log/application/status boundary. Object creation/drop and allocation metadata must have explicit recoverable meaning, not be inferred from a later ordered put into a missing object.

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
- database size, checkpoint overhead and commit-pause duration.

Run hot/cached and larger-than-memory regimes. A design that wins only when everything is in RAM is insufficient; a design that optimizes SSD throughput while imposing large cached-hit overhead is also insufficient.

The undo scanner's payload memory is bounded by one frame, but its offset index still grows with retained versions. WAL recovery currently returns the whole retained result vector, and recovery transaction bookkeeping also grows with retained history. Qualify explicit bounds, streaming/indexing and retention before claiming bounded-memory or large-history recovery.

## Immediate sequence

1. Keep the B-tree/WAL/MVCC/undo foundations green under stable, MSRV, Clippy, PostgreSQL differential and perf smoke; fix surfaced invariants rather than relaxing tests.
2. Add atomic B-tree upsert/current-record replacement and prove idempotent raw put/delete replay across split-heavy, tiny-buffer repeated application.
3. Add the shared canonical final-effect reducer, intent ownership and precommit admissibility checks; settle replay identity before appending integration undo.
4. Implement the durable-WAL-first current-record path with multi-object status/frontier publication, private-write visibility and injected failure tests. Until page durability gates exist, use transient page devices and do not claim persistent integration.
5. Implement same-image page dependencies, integrity, out-of-place working placement and a structurally complete checkpoint/page map; preserve owner outcomes, allocation identities and retention before publishing recovery authority. Prove process crash/reopen twice at each boundary, including split/checkpoint and unresolved-intent fencing.
6. Only then optimize durability batching, frame latching, translation, nonblocking checkpoint policy, page compression/fence truncation, version placement and structural split coordination from end-to-end measurements.

This sequence keeps the rewrite measurable and reviewable while retaining freedom to replace any implementation choice that does not survive correctness or performance qualification.
