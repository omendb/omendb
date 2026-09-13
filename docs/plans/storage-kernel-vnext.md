# Storage-kernel vNext implementation plan

**Branch:** `storage-kernel-vnext`  
**Architecture:** ADR 0013; installation/recovery contract in [ADR 0014](../adr/0014-vnext-installation-and-recovery.md)  
**Goal:** replace the generation-COW / opaque-KV-centered SeerDB implementation with a shared transaction, log, buffer and physical-storage kernel that can host multiple access methods without losing the existing correctness oracle.

## Strategy

This is a replacement implementation inside the existing repository, not a
permanent backend matrix. The current engine remains the semantic/recovery
oracle until vNext passes equivalent correctness, fault and performance gates.

Reuse semantics and test oracles where they are strong. Do not preserve an old
physical ownership model merely to reduce diff size. The vNext tree, transaction
log, MVCC envelope, undo store and materialization protocol are experimental and
may change before format stability.

Do not build speculative plugin hierarchies. The kernel owns common transaction,
log, buffer, durability and lifetime services; access methods remain concrete
until a second implementation demonstrates the seam that is actually needed.

## Current implementation status

### Implemented foundations

- stable `StorageObjectId`, 64-bit logical `PageId`, frame IDs/incarnations and
  authoritative/derived object classification;
- concurrent frame lifecycle, exact-incarnation pins, frame-local guards and
  sharded `PageKey -> FrameRef` translation without a global buffer mutex on
  cache hits;
- CLOCK/second-chance eviction baseline, duplicate-load publication handling,
  direct dirty-page creation and writeback/install race handling;
- native variable-size slotted B-link pages with cached key heads, high fences,
  right siblings, lazy compaction, byte-balanced leaf/internal splits, root
  replacement, point lookup, forward range traversal and key-restart cursors;
- atomic B-tree `upsert` distinct from duplicate-rejecting `insert`, including
  larger/smaller replacement, split-on-growth and unchanged-on-oversize behavior;
- pure configured-page-size ordered-record preflight using the real leaf builder,
  so an unrepresentable encoded current record can be refused before WAL I/O;
- idempotent raw logical put/delete replay under repeated application, split
  pressure and tiny-buffer eviction;
- transaction-scoped logical WAL with contiguous mutation ordinals, separate
  commit decisions, CRC32C, exact record-end LSNs and fail-closed parsing;
- WAL outer format v2 with a checksummed fixed header validated before trusting
  record length for torn-tail classification;
- recovery assembly for interleaved transactions with count/digest/ordinal/LSN
  and CSN validation plus reserved-identity rejection;
- explicit transaction phases distinguishing pre-I/O abortability from
  `WalAppended`, durable decision and `RecoveryRequired`;
- short ordered append lane coupling CSN assignment to physical decision order;
  append and sync are bound to the same owned `DurableLog` so higher orchestration
  cannot append to one WAL and accidentally synchronize another;
- segmented local WAL with whole-transaction rotation, directory barriers,
  retained-gap detection, torn-final-record repair and reopen synchronization of
  retained segments;
- MVCC record envelope v2 with `RecordOwner::{Transaction,Frozen}`, complete
  logical value/tombstone, undo head and optional install identity
  `(TxnId, final mutation ordinal)`;
- sharded transaction-status table plus contiguous visibility frontier;
- standalone undo-store outer format v2 with checksummed fixed headers,
  contiguous `VersionId`s, strictly backward predecessor links, frame-by-frame
  recovery scan, torn-tail repair, fencing after uncertain I/O and an explicit
  durable-version frontier;
- one shared canonical final-effect reducer used by live `Transaction` and
  `RecoveredTransaction`, retaining only the final effect per `(object,key)` in
  deterministic order while preserving the final original ordinal;
- sharded nonblocking write-intent ownership with canonical batch acquisition,
  rollback on conflict, same-owner nested ownership and RAII release;
- transient ordered MVCC installation primitive requiring an exact held intent,
  supporting absent put/delete, one-before-image replacement, completed-install
  no-op replay, aborted-current bypass, live snapshot conflicts and strict
  recovery ordering;
- transient durable-WAL-first ordered commit coordinator implementing canonical
  effects -> intents -> deterministic record/object preflight -> ordered append
  -> exact WAL sync -> all authoritative installs -> grouped undo sync -> status
  publication -> contiguous visibility -> release, with a runtime write fence
  established before unresolved intents can drop after post-WAL failures;
- multi-object commit qualification showing old snapshots remain on undo history
  until the new status/frontier is published; oversized values are rejected
  before WAL and WAL-sync/post-decision install failures fence instead of abort;
- point MVCC snapshot resolution through transaction status and multi-hop undo,
  including own installed writes, active/aborted bypass, newer-commit traversal,
  tombstones and fail-closed unknown owners;
- visible MVCC range cursor reusing the point resolver and counting logical
  visible rows rather than physical slots toward batch limits;
- private read-your-writes overlay for staged ordered put/delete, plus a captured
  transaction range overlay that stream-merges private inserts/updates/deletes
  with the transaction's fixed shared snapshot without speculative page install;
- sequential ordered recovery applicator using the same canonical effects,
  intents, installer, undo barrier and frontier as live commit, with strict
  contiguous-CSN replay, allocation-free completed retry, fail-closed aliasing
  of already-visible CSNs and fencing/hidden partial multi-object installs;
- sharded in-process per-page WAL/undo dependency table and `PageIo` decorator
  that refuses physical writeback until both durability frontiers cover the
  recorded requirements; this is a materialization primitive, not checkpoint
  authority, and B-tree mutation attachment/split inheritance is still pending;
- buffer victim contention fix so another loader stealing a just-evicted free
  frame is treated as a retry rather than an invariant failure.

### Not implemented yet

- exact same-image attachment of `PageDependencies` to every transactional page
  mutation and conservative inheritance through leaf/internal/root split paths;
- bounded precommit buffer/allocation progress proving a durable decision can be
  installed without depending on writeback of not-yet-eligible pages;
- integration of the dependency gate with live/recovery WAL and undo frontier
  advancement, plus tests that writeback cannot outrun either domain;
- page integrity/checksum envelope, out-of-place physical mapping and complete
  checkpoint publication;
- persistent recovery from checkpoint + synchronized WAL suffix into
  authoritative access methods; the current recovery applicator is transient;
- runtime-wide read/snapshot admission fencing after a post-decision failure;
  current coordinator fencing is write-admission scope only;
- owner freezing, retention-aware WAL/undo reclamation and physical GC;
- cross-process exclusive writable directory ownership/store-incarnation binding;
- canonical row storage and OmenDB cutover;
- optimized background writeback, durability batching, custom latches,
  translation fast paths or finer-grained SMO coordination.

The integrated ordered transaction/MVCC path remains intentionally qualified only
over transient/non-authoritative page recovery state. The dependency table can
gate working spill writes in-process, but until dependencies are attached to the
exact mutated images and a complete checkpoint is published, persistent dirty
page bytes are not recovery authority.

## Milestone A — kernel identities and frame contract

**Status: implemented correctness baseline.**

Keep:

- exact frame incarnation as the anti-ABA identity;
- pins preventing eviction/reuse;
- writer ownership publishing frame version/dirty state;
- zero-pin transitions as atomic lifecycle decisions;
- no durable semantics derived from frame version itself.

Remaining work is measurement/model-checking driven. Do not replace frame
synchronization simply because a lower-level primitive looks faster in isolation.

## Milestone B — concurrent buffer manager

**Status: functional synchronous baseline implemented; dependency-aware
materialization integration is in progress.**

The buffer currently provides sharded translation, CLOCK victim selection,
dirty writeback, direct new-page installation and transient writeback waits.
A separate sharded logical-page dependency table now retains conservative WAL
and undo requirements across residency changes within one process, and a
`DependencyCheckedPageIo` wrapper blocks physical writes until both frontiers
cover a page's recorded requirements.

The remaining correctness step is to attach those requirements to the exact
image while its page writer is held, preserve/inherit them through every split,
and advance the table's frontiers only from successful WAL/undo barriers. Do not
record dependencies after releasing the page guard: writeback could otherwise
capture bytes before their requirement is visible.

After correctness:

- background dirty queues and bounded materialization workers;
- measured admission/progress policy under pin/dependency pressure;
- custom latch or optimistic-read path only if end-to-end profiles justify it;
- predictive/validated translation, swizzling or direct arrays only if they win
  representative workloads;
- NUMA/tier placement later.

## Milestone C — page-resident B-link tree

**Status: ordered-access correctness baseline substantially qualified.**

Implemented:

- dynamic slotted leaf/internal pages;
- B-link right correction;
- split propagation/root replacement;
- point/range/cursor operations;
- split-heavy differential/stress qualification;
- structural-only per-object mutex as the current SMO baseline;
- atomic raw upsert/current-record replacement;
- configured-page-size record admission preflight;
- repeated mixed logical replay under eviction and splits.

Still required:

1. Attach page dependency metadata under the same guarded mutation and inherit
   source requirements into every newly created split sibling/root that needs
   them.
2. Qualify structural persistence through complete checkpoint graph closure.
3. Benchmark page size, prefix/fence truncation and slot hints before format
   stabilization.
4. Replace the structural mutex only when measured contention justifies a more
   complex page-local protocol.

Do not move checksum/page-LSN authority into the B-tree hot-path format merely
because the B-tree is the first access method.

## Milestone D — log-authoritative transactions

**Status: transient durable-WAL-first ordered baseline implemented; persistent
materialization/progress/fault qualification remains.**

The implemented live write path is:

```text
private staged writes
  -> validate original stream / canonical final effects
  -> acquire canonical write intents
  -> deterministic object/current-record page-fit preflight
  -> ordered CSN assignment + complete WAL append
  -> sync exact durable decision on the same owned WAL
  -> install every authoritative final effect under retained intents
  -> group-sync newly appended undo through the maximum required VersionId
  -> publish transaction status
  -> mark CSN ready / advance contiguous visibility
  -> release transaction state and intents
```

The durable transaction decision is commit authority. A failure after that
boundary is recovery work, not an abort. The coordinator establishes its write
admission fence before the intent guard can be released after an unresolved
post-WAL failure.

Still required before D is persistent-runtime complete:

1. Add page dependency attachment and advance the materialization table's WAL
   frontier immediately after decision sync and undo frontier after grouped undo
   sync.
2. Prove bounded installation progress before the durable decision. A transaction
   must not rely on evicting a page whose own undo dependency cannot be durable
   until later in the same transaction.
3. Add runtime-wide snapshot/read admission fencing or an explicit safe-prior
   read boundary for post-decision failures; the current coordinator fences new
   writes only.
4. Extend deterministic preflight to every remaining resource bound, including
   whole-transaction segment/admission constraints rather than converting a
   clean size refusal into an uncertain I/O outcome.
5. Qualify injected failures at every install/undo/status/frontier boundary with
   the dependency-aware page layer.
6. Only after milestone F's checkpoint work may persistent current-record pages
   become recovery authority.

## Milestone E — MVCC, contention and snapshot reads

**Status: transient ordered MVCC install, point/range snapshots and private
read-your-writes implemented.**

The current logical path proves:

- exact retained install identity + same logical effect is a no-op;
- matching identity + different logical effect is corruption;
- normal replacement appends one complete predecessor before-image;
- aborted current ownership is bypassed by inheriting its undo head;
- active other owners conflict;
- committed predecessors newer than a live writer snapshot conflict;
- recovery requires the writer's recovered committed status and an older
  predecessor commit;
- MVCC delete is a transaction-owned tombstone, not raw tree deletion;
- point reads use transaction status plus undo until a visible value/tombstone or
  logical absence is found;
- range reads reuse that exact resolver and do not count invisible rows toward a
  logical batch limit;
- private point reads consult the latest staged mutation first;
- transaction range cursors capture a canonical private overlay and ordered-merge
  it with the fixed snapshot, suppressing private deletes and replacing matching
  shared keys without speculative shared writes.

Next E work:

1. Keep all read/installer behavior green under concurrent commit/recovery stress
   once page dependencies are attached.
2. Add owner/status freezing only after replay and retention horizons are
   explicit.
3. Add undo/WAL reclamation only with snapshot/CDC/replica/backup leases and
   checkpointed owner outcomes.
4. Benchmark resident short-history metadata versus the durable undo store only
   after the logical contract is crash-qualified.

Serializable dependency certification remains above the fixed-snapshot MVCC
core rather than inside the low-level intent table.

## Milestone F — page materialization and checkpoint recovery

**Status: write-ahead gate primitive implemented; exact image attachment,
integrity, mapping and checkpoint authority remain the persistence blocker.**

For every exact captured image, retain at least:

```text
required WAL LSN
required undo VersionId, if any
```

`PageDependencyTable` and `DependencyCheckedPageIo` now implement monotonic
per-page requirements and a two-frontier physical-write gate inside one process.
They deliberately do not infer requirements from dirty state or page LSNs.

Next attach requirements while the access-method page writer/pin is still held.
Splits must copy inherited requirements to every image containing inherited state
and add the current operation's dependencies to structural pages modified by that
operation. Only successful WAL/undo barriers may advance the table frontiers.

The dependency table is not a checkpoint. It is currently process-local; a crash
discards it. The persistent materialization envelope must serialize equivalent
requirements with page integrity and placement metadata.

Do not use the maximum page LSN as a logical-redo skip watermark. Installation
may finish out of LSN order, and splits move effects between pages.

The first recovery-authority baseline is a structurally complete checkpoint:

1. briefly quiesce install/structural/GC mutation;
2. drain a completely installed contiguous visible frontier;
3. capture all reachable authoritative pages, roots, object metadata,
   logical-to-physical mapping, allocation high-water marks, retained owner
   outcomes and retention metadata;
4. enforce page integrity plus WAL/undo barriers for every captured image;
5. durably publish the checkpoint while retaining the previous complete one;
6. recover one checkpoint, synchronize/validate its retained WAL suffix, replay
   committed canonical effects in contiguous CSN order, validate references,
   then expose visibility.

The transient `OrderedRecoveryApplier` already proves the logical suffix side of
step 6: shared normalization/intents/install identity/undo semantics, strict
contiguous CSN replay, completed-retry idempotence and hidden partial installs.
It is not persistent recovery until checkpoint/page-map authority exists.

Uncheckpointed out-of-place images may be working spill state but do not become
recovery authority merely because they are durable.

After the blocking baseline is proven, measure a coherent nonblocking checkpoint
epoch or structural/physiological logging alternative. Do not retain checkpoint
pauses by inertia.

## Milestone G — canonical rows and OmenDB cutover

Implement ADR 0010 row records only after the transaction/materialization kernel
is crash-qualified.

Benchmark at least:

- clustered primary B-tree with compact row/family payloads;
- row/heap pages referenced by a primary B-tree.

Secondary scalar indexes remain B-tree access-method objects. The acceptance
proof is one transaction atomically changing a row, secondary index,
unique/constraint state and catalog/object metadata under one durable decision,
including kill/reopen at every durability/application/publication boundary.

Then wire a temporary OmenDB adapter and run the existing product oracle:

- typed relational tests;
- live PostgreSQL SQL/wire differential;
- SQLite trace differential where retained;
- schema/constraint and dump/restore tests;
- process crash matrix;
- pgbench/TPC-B plus YCSB/TPC-C-shaped workloads.

Cut the normal OmenDB path over only after correctness gates pass and the new
path has no material regression in its intended regimes. Delete the old storage
implementation rather than maintaining two permanent engines.

## Format and recovery policy

Current experimental vNext formats:

- transaction WAL outer frame: version 2;
- undo-store outer frame: version 2;
- `MvccRecord` envelope: version 2.

Older vNext development formats fail closed. There is no silent dual-format
fallback. Recreate disposable stores or use an explicit validated migration if
one later becomes necessary.

Only a provably incomplete final append may be truncated. Complete bad framing,
unknown versions/kinds/flags, checksum failures, contradictory transaction
metadata or invalid retained references are corruption.

A recovered file's existence is not proof that an earlier directory/file barrier
completed. Reopen must re-establish the durability boundary before publishing a
frontier derived from retained bytes.

## Performance gates

Every serious design choice records, where meaningful:

- throughput and p50/p95/p99;
- CPU user/system and cycles/op;
- allocation count/bytes and memory footprint;
- buffer occupancy, hit/miss/translation/latch/conflict/retry/wait metrics;
- logical WAL bytes, host writes and device/flash write amplification;
- recovery time versus checkpoint/log distance;
- database/checkpoint size and checkpoint pause/overhead.

Run both hot/cached and larger-than-memory regimes. Do not optimize SSD throughput
by imposing large cached-hit overhead, or declare an in-memory winner without
measuring spill/recovery behavior.

Current recovery is not fully bounded-memory: undo payload scanning is bounded
to one frame, but its version-offset index, WAL recovery output and pending/
terminal transaction bookkeeping still scale with retained history. Add explicit
retention/streaming/indexing policy before large-history qualification.

## Immediate sequence

1. Keep the transaction coordinator, private point/range reads, recovery
   applicator, atomic upsert/replay, WAL/undo framing and buffer suites green
   under stable, MSRV, Clippy, PostgreSQL differential and perf smoke.
2. Attach `PageDependencies` under the exact B-tree guarded mutation, inherit
   requirements through leaf/internal/root splits, and wire live/recovery WAL
   and undo frontier advancement into the same shared table.
3. Add deterministic bounded installation-progress admission and dependency-gate
   fault tests so no durable decision can be made unreplayable by buffer pressure.
4. Implement page integrity, out-of-place working placement and a structurally
   complete checkpoint/page map including allocation/status/retention state.
5. Qualify checkpoint + synchronized WAL suffix recovery with the existing
   ordered recovery applicator, including two consecutive reopens at every
   transaction and structural crash boundary.
6. Add canonical rows/cross-object relational qualification and cut OmenDB over.
7. Only then optimize batching, latches, translation, checkpoint concurrency,
   compression/fence truncation, version placement and SMO coordination from
   end-to-end measurement.
