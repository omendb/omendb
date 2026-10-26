# SeerDB architecture: reusable transaction and storage kernel

**Maintained target contract, 2026-10-25; not yet implemented or qualified.**
SeerDB owns this maintained contract in its own crate. See the [kernel roadmap](roadmap.md),
[OmenDB architecture](../../../docs/architecture.md), and [relational consumer](../../../docs/design/relational-engine.md).
[Older storage ADRs](../../../docs/adr/README.md) retain historical rationale, not a parallel specification.

## Purpose and reusable boundary

SeerDB is a general-purpose **durable, concurrent transaction/storage kernel** whose primary integration and performance workload is OmenDB. It should also be useful to independent consumers where its primitives are appropriate.
A separate Rust consumer may use transactional ordered KV; OmenDB can use richer,
schema-aware records and compiled access methods directly. Other data engines may
use the same memory, page, extent, snapshot and transaction facilities **when that
sharing actually wins**; OLAP, vector and remote object workloads may justify
independent stores and commit authorities when separately ingested.

One persistent store runtime owns its transaction identities/status, visibility,
conflict evidence, ordering, WAL, snapshots/history leases, buffer residency,
object/page/extent identity, lifetime, recovery, GC and resource admission.
Do not construct a persistent database from optionally attached coordinators
with independently fenced outcomes. Kernel and access methods are concrete
compiled modules with minimal interfaces; neither a runtime backend/plugin ABI
nor a universal KV/page model is required.

## Library and storage interface

The stable conceptual boundary is a store with transactional capabilities,
logical storage objects, ordered key/value access, guarded page and immutable
extent access, retained snapshots and transaction-complete change streams.
An independent user can operate an ordered transactional KV database without
OmenDB's catalog, SQL binder or server. OmenDB can instead operate clustered
rows and indexes through more specific compiled interfaces while sharing the
same commit, visibility, log and lifetime authority. Do not add a universal
access-method plugin trait or second WAL merely to make this package generic.
The public Rust API and exact resource/retention limits remain experimental;
stabilize them only after a real OmenDB integration and a separate KV-consumer
qualification. A durable object must not require a table schema to be valid.

## Source ownership and dependency direction

Put code in SeerDB **when its contract is meaningful to a Rust storage client
without importing OmenDB, SQL or a relational schema**. That includes storage
algorithms and utilities only when they have a real storage-level consumer,
not speculative general-purpose abstractions. OmenDB may depend on SeerDB;
SeerDB must never depend on OmenDB. Physical format and visibility decisions
belong to the owner of their durable authority.

| SeerDB owns | OmenDB or a specialized engine owns |
| --- | --- |
| Store and storage-object identities, lifetime and leases; transaction IDs, MVCC snapshots, commit decisions, conflict/intents and generic predicate/range-dependency mechanisms | SQL transaction and statement semantics, type/coercion/NULL rules, constraint timing, key and row identity derived from schema |
| WAL/framing, checkpoints, recovery, retention/GC, bounded resource admission for storage operations | SQL catalog/layout versions, table/index definitions and DDL, including when/how to register generic storage dependencies |
| General ordered trees and KV APIs; buffer pool, page/extent allocators, mapping, overflow, compaction and storage-tier I/O | Clustered SQL-row codecs, covering/unique/FK index key codecs and maintenance obligations, relational access-path adapters |
| Reusable transactional snapshots/change streams, storage-level batch/cursor APIs and fault/durability machinery | SQL binder/optimizer/executor, query batches/operators, authorization, sessions, wire protocols and workload-specific scheduling |
| Reusable low-level index or compression primitives **only if** they provide a true cross-workload capability | BM25 tokenization/ranking, ANN/vector semantics, columnar query semantics and proprietary/independent specialized storage layouts |

For example, OmenDB's current `src/seer_direct.rs` is a **relational
adapter**, not a candidate SeerDB module. The `src/row_identity.rs`,
`src/sql_types.rs` and `src/relational.rs` depend on OmenDB's
`TableId`, `ColumnId` and `Value` semantics and remain outside SeerDB.
The current `src/packed.rs` uses OmenDB's fixed-width `Key` and error/fault
contracts; do **not** move it into SeerDB merely because it contains a page
codec. Conversely, any future generic page codec, append/extent I/O or
versioned storage primitive should have a SeerDB-owned implementation,
with OmenDB-specific wrappers and codecs left in OmenDB.

Likewise, the root `src/runtime.rs` schedules **OmenDB work classes**
(schema/scan/client work). SeerDB owns its internal storage admission and
progress resources; it does not need SQL scheduling policy or session types.
Existing `crates/durable-fs` remains the low-level reusable filesystem
durability helper below SeerDB, without owning WAL/transaction policy.

Use the smallest cohesive Rust modules inside existing crates. Do not add a
new workspace crate or dependency on a full database product solely to
share one helper; promote concrete primitives only when an independent
consumer and lifetime/error contract justify them. Test the public SeerDB
Rust library **without OmenDB as a dependency**, plus OmenDB integration
when an operation crosses the boundary. Shared code does not imply
shared source identity, WAL or transactional authority across separately
ingested standalone Vector/OLAP stores.

## Authority, objects, and versions

- Explicit store incarnation and exclusive writer ownership bind all containers,
  manifests and page/extent references. A foreign or incomplete file never
  becomes state merely because its checksum passes.
- Distinct identities for TxnId, commit sequence (CSN), WAL position (LSN),
  storage object/incarnation, page, frame incarnation, logical record/version
  and physical image. Page/slot/offset locators may accelerate lookups but
  cannot be authoritative row or snapshot identity.
- Canonical rows, catalog objects and required constraint/index effects are
  *authoritative*; all participate in one durable decision. Rebuildable
  projections are *derived* only if exact semantics remain reachable by retained
  coverage, complete deltas, or a documented fallback/approximation.
- MVCC is one logical snapshot/visibility service, with compact current metadata
  and bounded recent history. Physical version encoding may vary across access
  methods. Older history is indexed/segmented or otherwise independently
  reclaimable, not permanently all in memory or synchronized per-key per commit.

## Transaction and serializability protocol

1. Admit transaction-grain memory/CPU/I/O/retention resources before acquiring
   locks/intents or long-lived snapshots. Stage mutations privately, with an
   adaptive indexed write overlay for large transactions; no global shared
   mutation before a safe publication protocol.
2. Register point/absence/range/gap/catalog/object and constraint dependencies.
   Decision-ordered serializable certification considers earlier **accepted but
   not yet installed** writers. An acceptance decision and publication of its
   conflict evidence must be one ordered operation with no check/append gap.
   Retention pressure must fail closed, not silently discard conflict evidence.
3. Acquire ordered logical intents and reserve full original WAL, predecessor
   versions, structural splits/root work, overflow, pins, bookkeeping, recovery
   and installer completion capacity **before an irrevocable decision**.
   Deterministic impossible size/space refusal belongs before device entry.
4. Prepare effects and recoverable history outside the short decision lane.
   Certify, assign CSN and append one authenticated complete transaction
   decision in a known order. Durably synchronize it with bounded adaptive
   batching (no fixed sleep for an uncontended writer).
5. Install all authoritative effects and structural closure safely; publish
   readiness and advance the **contiguous visible CSN**. A synchronous success
   acknowledges only after its CSN is covered. History/page images and
   checkpoints materialize off this ordinary ACK path once full log recovery
   has been proved.
6. Ownership of a possibly appended decision survives cancellation,
   disconnection and unwind. Before dropping unresolved intents, admission
   or checkpoint membership, complete or fence the runtime. Unknown outcomes
   need durable request identity and explicit resolution, not blind retry.

**Design candidate, not established optimum:** durable-decision-first installation
is the initial proof path. A safe invisible/preconstructed installation path
overlapping WAL sync can improve latency but substantially complicates abort,
eviction, recovery, memory-model and lifetime proofs. Compare after a complete
reference implementation and profiling; it is not a mandated early rewrite.

**Isolation:** SERIALIZABLE with ordered certification is the reference
correctness mode; READ COMMITTED is a distinct statement-snapshot and
wait/recheck implementation, not an alias for snapshot isolation.
REPEATABLE READ/snapshot isolation must explicitly describe write skew.
The *user-visible default remains unresolved*: measure hot-row conflicts,
long transactions, retry UX and mixed-isolation histories first. A server
cannot generally replay arbitrary multi-statement client work transparently.

## Physical access and hot-path policy

- Initial ordered structure: mutable buffered B+/B-link tree with fine-grained
  guards, high fences/right siblings, schema-driven clustered rows and separate
  authoritative secondary indexes. An ordered KV facade uses the same tree.
- Variable-size records need bounded *physical* cell descriptors and logical
  overflow for oversized keys, separators, fences and values. Admission proves
  fully fenced actual leaf/internal/root split candidates and replay/history
  representability; an empty-leaf fit check is insufficient.
- Page sizes (4/8/16 KiB), slot layout, shortened separators, abbreviated/
  SIMD key comparisons, node searches and BBTree-inspired batched probes are
  measured options, not immutable format requirements. A heap+primary-index,
  PAX or LSM alternative earns adoption through a real indexed mixed workload,
  not a mandatory second production backend.
- The buffer starts from exact-incarnation guarded frames, sharded translation
  and **single-flight Loading ownership before I/O**. A delayed stale load
  must never supersede a newer acknowledged resident/evicted page. Benchmark
  safe optimistic lock coupling and predictive translation, but version counters
  do not make unsynchronized Rust byte access safe.
- Make dirty-page image capture brief and bounded; avoid retaining byte latches
  through slow device writeback. Coalesce updates in RAM and materialize page
  images out of place to NVMe. Large immutable extents may use their own cache
  units, range reads, compression, prefetch and eventually object placement.
- Global memory admission accounts for page/history/query caches, transient
  images, prefetch and compaction; fixed frame count alone is not an RSS bound.
  Preserve protected WAL/installer/reclaim capacity under foreground or
  analytical saturation.

## WAL, checkpoint, recovery, and reclamation

The complete recoverable log plus a **structurally closed published checkpoint**
must reconstruct current values, canonical index/catalog effects, transaction
outcomes and retained history. A separate synchronized before-image file in the
current implementation is required until this complete-log replacement actually
passes history/crash qualification. Large external payloads can require distinct
durable dependencies; do not promise a universal one-fsync transaction.

- Versioned framed WAL identifies original effects, transaction boundaries,
  counts and digests, and store incarnation. Support large transactions
  through bounded framing/spill or deliberate pre-append refusal; do not let
  the current one-segment implementation become a product limit by accident.
- Checkpoint capture binds an exact visible CSN/decision LSN, root/catalog/
  object graph, B-link/fences, overflow/extent closure, allocation/page maps,
  status/history and all leases. Publish checksummed manifest *only after*
  dependent bytes are durable; preserve prior valid authority on failures.
- A conservative quiesced capture is the first correctness proof. Design and
  qualify an **incremental/epoch capture** with bounded pause and temporary
  resource use for multi-TB databases before treating local checkpointing as
  production scalable.
- Recovery validates checkpoint and committed suffix, streams effects with
  bounded memory, refuses complete corruption/foreign references, resolves
  incomplete trailing frames only when provably safe, and replays complete
  transactions through the same logical reducer/installation identity as live
  commits. Crash during recovery, two reopens and subsequent ordinary write
  must work. Repeated failed retries cannot accumulate unbounded orphan pages.
- Physical page map publication and duplicate validation must scale
  subquadratically and preferably incrementally with changed pages; avoid
  writing/validating a complete map on every small checkpoint or mutation.
  Reclamation requires proofs against prior checkpoint, oldest readable
  snapshot, CDC/backup, future replica and outstanding physical reader pins.
- Snapshot/bootstrap is an atomically retained handoff of source incarnation,
  catalog/layout, snapshot CSN, change restart LSN and history/delta leases.
  The current `{CSN, LSN}` exported pair is insufficient on its own.

## Storage tiers and supported IO

Local RAM/NVMe supplies authoritative low-latency decision log and mutable
pages. Portable filesystem I/O is the correctness baseline; Linux async
positional I/O, io_uring, direct I/O, FDP/ZNS, SSD-aware placement/packing
and buffer mapping are measured deployment optimizations. Distinguish real
power-loss durability, kernel sync and process-crash survival.

A secondary S3-compatible tier may hold immutable, content-validated snapshots,
archived logs and cold extents, with cache/range-read, request/egress, lease
and restore economics. Object-store single-key atomicity does not provide
multi-object transaction commit. Remote-primary/disaggregated authority is
a separately designed and qualified profile, never implied by adding an
object-storage provider.

## Source audit and implementation debt (2026-10-25)

These are observed **current code shapes**, not benchmarked magnitudes:
`src/vnext/buffer.rs` holds a frame byte read guard across writeback I/O;
`src/vnext/undo_store.rs` serializes version reads behind a file mutex and
constructs a large in-memory offset index; `src/vnext/segment_log.rs` collects
all WAL frames for replay; `src/vnext/page_map.rs` uses quadratic duplicate
checks; `src/vnext/btree/tree_v4.rs` materializes whole leaves for ranges/splits;
`src/vnext/btree/preflight.rs` tests empty leaves, not split/root feasibility.
The coordinator is not yet a complete persistent restart authority.
Preserve fault/correctness proofs, but replace weak abstractions rather
than retaining them for implementation compatibility.

**Qualification**: end-to-end multi-object/SQL correctness, crash-after-
decision/before-materialization, long-reader/GC, zero-gap CDC, space/ENOSPC,
history growth, checkpoint/recovery cost and p99 under concurrent hot and
larger-than-memory mixed loads. Device-backed matched comparisons against
RocksDB, Fjall, SurrealKV and relational PostgreSQL/MySQL at each appropriate
semantic layer; never infer product performance from paper benchmarks.

