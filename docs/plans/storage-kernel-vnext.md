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
- buffer victim contention fix so another loader stealing a just-evicted free
  frame is treated as a retry rather than an invariant failure.

### Not implemented yet

- the durable-WAL-first transaction coordinator joining intents, preflight, WAL
  durability, grouped undo durability, multi-object installation, status
  publication and visibility-frontier completion;
- private read-your-writes view at the integrated runtime surface;
- point/range snapshot traversal through undo chains;
- same-image page durability dependencies and writeback eligibility;
- page integrity/checksum envelope, out-of-place physical mapping and complete
  checkpoint publication;
- persistent recovery replay from checkpoint + WAL suffix into authoritative
  access methods;
- owner freezing, retention-aware WAL/undo reclamation and physical GC;
- cross-process exclusive writable directory ownership/store-incarnation binding;
- canonical row storage and OmenDB cutover;
- optimized background writeback, durability batching, custom latches,
  translation fast paths or finer-grained SMO coordination.

The current ordered MVCC installer is intentionally qualified only over
transient/non-authoritative page I/O. Until page dependencies and checkpoint
publication exist, persistent dirty-page bytes are not recovery authority.

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

**Status: functional synchronous baseline implemented.**

The buffer currently provides sharded translation, CLOCK victim selection,
dirty writeback, direct new-page installation and transient writeback waits.

Before persistent MVCC integration, extend the frame/materialization boundary so
an exact page image carries its WAL and undo durability dependencies. Those
dependencies must be captured with the same guarded bytes, survive residency
changes, and be inherited by split pages. Only then may eviction/writeback gate
on actual durability frontiers.

After correctness:

- background dirty queues and bounded materialization workers;
- measured admission/progress policy under pin pressure;
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
- repeated mixed logical replay under eviction and splits.

Still required:

1. Preserve page dependency metadata through local update and every split path.
2. Qualify structural persistence through complete checkpoint graph closure.
3. Benchmark page size, prefix/fence truncation and slot hints before format
   stabilization.
4. Replace the structural mutex only when measured contention justifies a more
   complex page-local protocol.

Do not move checksum/page-LSN authority into the B-tree hot-path format merely
because the B-tree is the first access method.

## Milestone D — log-authoritative transactions

**Status: log/ordering/recovery primitives implemented; integrated transaction
runtime incomplete.**

The target live write path is:

```text
private staged writes
  -> validate original stream / canonical final effects
  -> acquire write intents + conflict/isolation/constraint checks
  -> deterministic preflight
  -> ordered CSN assignment + complete WAL append
  -> sync durable decision
  -> append/group-sync required undo + install every authoritative effect
  -> publish transaction status
  -> mark CSN ready / advance contiguous visibility
  -> release intents and ordinary snapshot
```

The durable transaction decision is commit authority. A failure after that
boundary is recovery work, not an abort. Runtime write admission must be fenced
before unresolved intent ownership can be released after a post-decision error.

Immediate D work:

1. Add one coordinator that consumes the canonical final-effect set rather than
   letting individual subsystems rediscover it.
2. Keep all deterministic refusal before the durable decision: identity/encoding
   limits, object availability, access-method representability, value/record
   capacity, allocation progress and whole-transaction WAL-segment bounds.
3. Group undo synchronization across installed effects instead of forcing one
   sync per key.
4. Qualify multi-object decisions, partial installation and every WAL/undo/status
   failpoint over transient page I/O.
5. Only after milestone F's materialization/checkpoint work may the same path use
   persistent authoritative pages.

## Milestone E — MVCC, contention and snapshot reads

**Status: status, visibility, canonical effects, intents, install identity,
undo store and transient ordered installer implemented.**

The current installer proves these local semantics:

- exact retained install identity + same logical effect is a no-op;
- matching identity + different logical effect is corruption;
- normal replacement appends one complete predecessor before-image;
- aborted current ownership is bypassed by inheriting its undo head;
- active other owners conflict;
- committed predecessors newer than a live writer snapshot conflict;
- recovery requires the writer's recovered committed status and an older
  predecessor commit;
- MVCC delete is a transaction-owned tombstone, not raw tree deletion.

Next E work:

1. Add point snapshot resolution: inspect current owner, return if visible,
   otherwise follow `undo_head` until a visible value/tombstone/absence is found.
2. Reuse exactly that resolver for range/cursor traversal; do not implement a
   separate range visibility policy.
3. Add owner/status freezing only after replay and retention horizons are
   explicit.
4. Add undo/WAL reclamation only with snapshot/CDC/replica/backup leases and
   checkpointed owner outcomes.
5. Benchmark resident short-history metadata versus the durable undo store only
   after the logical contract is qualified.

Serializable dependency certification remains above the fixed-snapshot MVCC
core rather than inside the low-level intent table.

## Milestone F — page materialization and checkpoint recovery

**Status: not implemented; this is the persistence blocker.**

For every exact captured image, retain at least:

```text
required WAL LSN
required undo VersionId, if any
```

Capture bytes and dependencies under the same guard/version. Writeback may
proceed only when the actual WAL and undo durable frontiers cover them. Splits
must copy inherited dependencies to every image containing inherited state.

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
   committed canonical effects, validate references, then expose visibility.

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

1. Keep the upsert/replay, canonical write set, intents, MVCC v2 identity,
   transient installer, WAL/undo framing and buffer stress suites green under
   stable, MSRV, Clippy, PostgreSQL differential and perf smoke.
2. Implement the durable-WAL-first transaction coordinator with deterministic
   preflight, canonical intent ownership, grouped undo durability, multi-object
   install, status publication, contiguous visibility and runtime fencing.
3. Implement point snapshot traversal and then range/cursor traversal through
   the same current/undo visibility resolver.
4. Add same-image page dependency metadata and writeback eligibility, then page
   integrity/out-of-place placement and a structurally complete checkpoint/map.
5. Qualify process crash/reopen twice at every transaction and structural
   boundary before persistent vNext current-record pages become authoritative.
6. Add canonical rows/cross-object relational qualification and cut OmenDB over.
7. Only then optimize batching, latches, translation, checkpoint concurrency,
   compression/fence truncation, version placement and SMO coordination from
   end-to-end measurement.
