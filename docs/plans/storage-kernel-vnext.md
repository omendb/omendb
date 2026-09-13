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
- prepared ordered MVCC installation: predecessor visibility/order is validated
  and complete before-images are appended without page mutation, all required
  undo can be group-synchronized, then the exact predecessor is revalidated and
  the prepared current record is installed under the retained intent;
- transient standalone MVCC install compatibility plus dependency-aware install
  that synchronizes the referenced undo head before mutating a persistable page;
- durable-WAL-first ordered commit coordination:
  canonical effects -> intents -> deterministic record/object preflight ->
  ordered append -> exact WAL sync -> prepare every predecessor/undo record ->
  one grouped undo sync -> apply every prepared authoritative effect -> status
  publication -> contiguous visibility -> release;
- post-WAL runtime write fencing before unresolved intents can drop, with clean
  deterministic refusal kept before the durable decision where currently known;
- point MVCC snapshot resolution through transaction status and multi-hop undo,
  including own installed writes, active/aborted bypass, newer-commit traversal,
  tombstones and fail-closed unknown owners;
- visible MVCC range cursor reusing the point resolver and counting logical
  visible rows rather than physical slots toward batch limits;
- private read-your-writes overlay for staged ordered put/delete, plus a captured
  transaction range overlay that stream-merges private inserts/updates/deletes
  with the transaction's fixed shared snapshot without speculative page install;
- sequential ordered recovery using the same canonical effects, intents,
  prepared predecessor state, grouped undo barrier, page installer and visibility
  frontier as live commit, with strict contiguous-CSN replay, allocation-free
  completed retry and fail-closed already-visible transaction identity;
- sharded in-process per-page WAL/undo dependency table plus
  `DependencyCheckedPageIo`, which refuses physical writeback until both durable
  frontiers cover the exact page requirements;
- dependency-aware B-tree upsert that merges the current operation requirement
  while the exact page pin/write guard is held and conservatively inherits source
  requirements through leaf splits, internal splits and root replacement;
- live commit advances the page WAL frontier only after decision sync, prepares
  and synchronizes all required undo before any page mutation, then attaches the
  decision LSN plus resulting actual undo head to every mutated page image;
- dependency-aware recovery requires the retained WAL frontier through the
  decision LSN before replay, group-synchronizes required undo before page
  mutation, and attaches the same exact WAL/undo requirements during replay;
- the previous self-dependency progress cycle is removed: a transaction never
  installs a page that depends on its own not-yet-durable undo and then needs to
  evict that page to finish the same transaction;
- buffer victim contention fix so another loader stealing a just-evicted free
  frame is treated as a retry rather than an invariant failure.

### Not implemented yet

- page integrity/checksum envelope and out-of-place physical page placement/map;
- structurally complete checkpoint publication retaining roots, object metadata,
  allocation high-water marks, page map, owner outcomes and retention state;
- persistent recovery from checkpoint + synchronized retained WAL suffix into
  authoritative access methods; the current page dependency table is
  process-local working-state metadata, not restart authority;
- full deterministic/bounded admission for arbitrary buffer and allocation
  pressure. The transaction's own undo-dependency cycle is fixed, but an
  undersized pool or externally pinned frames may still produce `NoVictim`;
- runtime-wide read/snapshot admission fencing after a post-decision failure;
  current coordinator fencing is write-admission scope only;
- complete failpoint/crash matrix across dependency-aware prepare, undo barrier,
  page application, status/frontier publication, checkpoint publication and two
  consecutive reopens;
- owner freezing, retention-aware WAL/undo reclamation and physical GC;
- cross-process exclusive writable directory ownership/store-incarnation binding;
- canonical row storage and OmenDB cutover;
- optimized background writeback, durability batching, custom latches,
  translation fast paths or finer-grained SMO coordination.

The dependency-aware transaction/recovery path is now sufficient for safe
in-process spill eligibility: the exact working page image carries conservative
WAL/undo requirements and those requirements are covered before writeback is
allowed. It is deliberately **not** recovery authority yet. A crash loses the
process-local dependency map and there is no complete published page graph from
which to reopen.

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

**Status: synchronous dependency-aware materialization baseline implemented.**

The buffer provides sharded translation, CLOCK victim selection, dirty
writeback, direct new-page installation and transient writeback waits.
`PageDependencyTable` retains conservative WAL/undo requirements across
residency changes within one process. `DependencyCheckedPageIo` blocks a physical
write until both frontiers cover the recorded requirement.

Transactional requirements are attached before the affected page pin can be
released. Newly created B-tree split pages inherit conservative source
requirements before split pins are released. Live commit/recovery only advance
frontiers from successful WAL/undo barriers.

Remaining buffer/materialization work:

- page integrity and out-of-place physical placement below logical `PageKey`;
- a persistent checkpoint/page-map envelope containing dependency-equivalent
  recovery metadata;
- explicit admission/progress policy under arbitrary pin pressure or very small
  pools; the self-undo dependency cycle is no longer part of that problem;
- background dirty queues and bounded materialization workers after the blocking
  correctness baseline is crash-qualified;
- custom latch, optimistic-read or translation fast paths only if end-to-end
  profiles justify them;
- NUMA/tier placement later.

## Milestone C — page-resident B-link tree

**Status: ordered-access correctness and dependency propagation baseline
substantially qualified.**

Implemented:

- dynamic slotted leaf/internal pages;
- B-link right correction;
- split propagation/root replacement;
- point/range/cursor operations;
- split-heavy differential/stress qualification;
- structural-only per-object mutex as the current SMO baseline;
- atomic raw upsert/current-record replacement;
- configured-page-size record admission preflight;
- repeated mixed logical replay under eviction and splits;
- same-guard transactional page dependency attachment;
- conservative inherited dependencies on leaf/internal split siblings and new
  roots.

Still required:

1. Qualify structural persistence through complete checkpoint graph closure and
   crash/reopen testing.
2. Benchmark page size, prefix/fence truncation and slot hints before format
   stabilization.
3. Replace the structural mutex only when measured contention justifies a more
   complex page-local protocol.

Do not move checksum/page-LSN authority into the B-tree hot-path format merely
because the B-tree is the first access method.

## Milestone D — log-authoritative transactions

**Status: durable-WAL-first ordered baseline with dependency-aware materialization
implemented; checkpoint authority and broader progress/fault qualification
remain.**

The implemented live write path is:

```text
private staged writes
  -> validate original stream / canonical final effects
  -> acquire canonical write intents
  -> deterministic object/current-record page-fit preflight
  -> ordered CSN assignment + complete WAL append
  -> sync exact durable decision on the same owned WAL
  -> advance in-process page WAL frontier
  -> prepare every effect: validate predecessor + append required complete undo
     (no shared page mutation)
  -> group-sync through the highest undo VersionId referenced by any result
  -> advance in-process page undo frontier
  -> revalidate each prepared predecessor and install every authoritative effect,
     attaching decision LSN + actual resulting undo head to mutated page images
  -> publish transaction status
  -> mark CSN ready / advance contiguous visibility
  -> release transaction state and intents
```

The durable transaction decision is commit authority. A failure after that
boundary is recovery work, not an abort. The coordinator establishes its write
admission fence before an unresolved post-WAL path can return control to ordinary
writers.

Preparing and synchronizing undo before the first page mutation is deliberate.
It preserves one grouped undo barrier while preventing a transaction from
creating pages that depend on its own unsynchronized undo and then requiring
those ineligible pages as eviction victims to finish the same commit.

Still required before D is persistent-runtime complete:

1. Define deterministic admission/resource bounds for remaining dynamic failures
   such as arbitrary pin pressure, minimum usable buffer capacity, allocation
   exhaustion and whole-transaction WAL-segment limits.
2. Add runtime-wide snapshot/read admission fencing or an explicit safe-prior
   read boundary for post-decision failures; the current coordinator fences new
   writes only.
3. Qualify injected failures at every prepare/undo/apply/status/frontier boundary
   with dependency-aware pages and persistent checkpoint authority.
4. Only after milestone F's checkpoint work may persistent current-record pages
   become restart/recovery authority.

## Milestone E — MVCC, contention and snapshot reads

**Status: ordered MVCC prepare/apply, point/range snapshots and private
read-your-writes implemented.**

The current logical path proves:

- exact retained install identity + same logical effect is a no-op;
- matching identity + different logical effect is corruption;
- normal replacement appends one complete predecessor before-image;
- preparation can append history while leaving the shared page unchanged;
- prepared application revalidates the exact predecessor before mutation;
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
- transaction range cursors capture a private overlay and ordered-merge it with
  the fixed snapshot, suppressing private deletes and replacing matching shared
  keys without speculative shared writes.

Next E work:

1. Keep read/prepare/apply behavior green under concurrent commit/recovery stress
   and the persistent crash matrix.
2. Add owner/status freezing only after replay and retention horizons are
   explicit.
3. Add undo/WAL reclamation only with snapshot/CDC/replica/backup leases and
   checkpointed owner outcomes.
4. Benchmark resident short-history metadata versus the durable undo store only
   after the logical contract is crash-qualified.

Serializable dependency certification remains above the fixed-snapshot MVCC
core rather than inside the low-level intent table.

## Milestone F — page materialization and checkpoint recovery

**Status: exact in-process write-ahead materialization gating implemented;
persistent page/checkpoint authority is now the primary blocker.**

For every dependency-aware current page image, the runtime retains at least:

```text
required WAL LSN
required undo VersionId, if any
```

Requirements are merged while the relevant page remains pinned/writer-owned.
Splits copy conservative inherited requirements to siblings and structural
parents/roots as needed. Physical working-page writeback is permitted only when
actual WAL and undo durable frontiers cover those requirements.

The dependency table is not a checkpoint. It is process-local and can be rebuilt
only from authoritative durable state that does not exist yet for vNext pages.
Uncheckpointed written pages may therefore be working spill state but never
restart authority merely because their bytes reached storage.

Do not use a maximum page LSN as a logical-redo skip watermark. Installation may
finish out of LSN order and splits move logical effects between pages.

The next major implementation milestone is a structurally complete checkpoint:

1. define a checksummed page image/envelope and out-of-place physical page map;
2. briefly quiesce install/structural/GC mutation for the first correctness
   baseline;
3. drain a completely installed contiguous visible frontier;
4. capture every reachable authoritative page plus roots, object metadata,
   logical-to-physical mapping, allocation high-water marks, retained owner
   outcomes and retention metadata;
5. prove each captured image satisfies its WAL/undo durability requirements;
6. durably publish one complete checkpoint/manifest while retaining the previous
   complete checkpoint;
7. on reopen, load one complete checkpoint, re-establish durable file/directory
   boundaries, synchronize and validate the retained WAL suffix, replay committed
   canonical effects in contiguous CSN order, validate references, then expose
   visibility;
8. run two consecutive reopens at every transaction and structural crash point so
   a recovery-only latent corruption cannot pass a single reopen test.

`OrderedRecoveryApplier` already supplies the logical suffix-application side of
step 7, including contiguous ordering, exact transaction identity, grouped undo
before page mutation and dependency-aware replay. It remains transient until a
checkpoint/page-map authority exists.

After this blocking baseline is proven, measure a coherent nonblocking checkpoint
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

1. Keep prepared MVCC install, dependency-aware commit/recovery, private
   point/range reads, WAL/undo framing, B-tree split propagation and buffer suites
   green under stable, MSRV, Clippy, PostgreSQL differential and perf smoke.
2. Implement checksummed out-of-place page images plus the logical-to-physical
   page map and structurally complete checkpoint/manifest baseline.
3. Recover checkpoint + synchronized retained WAL suffix through the existing
   ordered recovery applicator, and run the two-reopen crash matrix before any
   vNext working page becomes restart authority.
4. Add deterministic resource/admission bounds and dependency-aware failpoints,
   including arbitrary pin pressure and post-decision read/snapshot fencing.
5. Add owner freezing/retention reclamation only once checkpointed owner outcomes
   and all reader/CDC/replica/backup horizons are explicit.
6. Add canonical rows/cross-object relational qualification and cut OmenDB over.
7. Only then optimize batching, latches, translation, checkpoint concurrency,
   compression/fence truncation, version placement and SMO coordination from
   end-to-end measurement.
