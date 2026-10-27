# SeerDB architecture: reusable transaction and storage kernel

**Maintained target contract, 2026-10-27; not yet implemented or qualified.**
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

Authoritative mutation uses runtime-enrolled, versioned storage effects, not an
unlogged mutable-page escape hatch. OmenDB translates row/index/catalog meaning
into those effects; SeerDB can replay them without invoking a SQL binder or an
application callback. Record/page/extent access may be richer than ordered KV,
but each adopted physical operation must have concrete recovery, dependency and
lifetime semantics before it can participate in a durable decision.

The first integration uses concrete ordered objects, current/history records and
one recovery path. Its independent KV facade exercises those same operations;
guarded payload access is an optional surface, not a second physical engine.
Do not implement every prospective record/extent capability or freeze a generic
access-method interface before those consumers demonstrate a need.

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

## Batched physical access without query-language coupling

Where a real consumer needs them, offer efficient **resumable batches**
for ordered key/value, record and immutable-extent reads. Preserve logical
key or stable object-local identity, version/snapshot information,
bounded row/byte/I/O/work budgets, short-lived guarded byte access and
exclusive continuation across splits, eviction and physical movement.
Do not decode/copy every whole value merely to deliver a batch, rescan
ever-growing prefixes, or hold mutable page latches across executor
suspension. Read selected byte ranges or fields only where the format
supports it.

Expose useful *generic* encoded-block metadata only with proven consumers:
constant/dictionary/bitpacked/RLE-like byte layouts, block summaries or
safe numeric bounds may help upstream operations avoid decoding. SQL
NULL/collation/aggregate semantics, BM25 scoring, ANN ranking and operator
pipelines remain OmenDB or specialized-engine responsibilities; not all
blocks need share B-tree page layout. Compare batch vs direct point operations,
different batch sizes, selectivity, memory/copy/CPU/p99 and larger-than-RAM
I/O before locking interfaces. This is a transferable idea from
[Turbopuffer's October 26, 2026 design](https://turbopuffer.com/blog/fully-vectorized-search-engine),
not a universal storage policy or measured SeerDB speedup.

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

**Required behavior versus implementation choice:** one durable, atomic
multi-object commit outcome; strict acknowledgement/visibility ordering;
read-your-writes; correctly scoped isolation and conflict detection; preserved
history and crash recovery; and bounded resource/failure ownership are
contracts. A globally serialized CSN *append* or lock coupling, a second
undo sync, a particular MVCC representation, and post-sync page installation
are implementation candidates, not user-level requirements. Optimize the
whole commit timeline with these contracts intact.


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
   Deterministic impossible size/space refusal precedes submission of decision bytes.
4. Prepare effects and recoverable history outside the short decision lane.
   Certify, assign CSN and append one integrity-checked complete transaction
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

### Identity, transaction ownership and retry outcomes

The runtime allocates transaction identities; ordinary callers do not choose or
reuse `TxnId`s. Reserve durable identity ranges or use a durable allocation epoch
so restart, aborted work and pruned outcomes cannot alias a previously issued
handle. Store/object incarnations distinguish replacement from name reuse.
A transaction handle owns its status, snapshot/certification registrations,
private effects and resource leases. Clean pre-decision drop/abort releases them
exactly once; possibly appended work transfers to completion ownership or fences
before release. An early refusal must not leave a permanently Active status.

Expose distinct pre-decision refusal, definite abort, committed position and
unresolved outcome classes. A generic error/timeout does not mean rollback.
OmenDB owns client request naming and authentication; SeerDB owns its atomic
association with transaction identity, decision and durable outcome. For an
opt-in retry token, bind its namespace and operation fingerprint before possible
append. Concurrent/retried attempts must resolve the same attempt, not execute a
second transaction; incompatible reuse is an error. The complete recoverable
decision stores token/namespace, fingerprint and transaction/outcome association
atomically with its effects, not in a separately losable index. An absence lookup
is final only with complete retained evidence for that token's domain and no
outstanding append that can still decide it. A clean pre-append refusal needs no
durable negative record; if its in-process proof is lost, absence alone does not
recreate it. Return unresolved/expired unless recovery can prove non-commit.

Retain token/outcome evidence through checkpoint and log reclamation for a declared
retry horizon, independently of row-owner freezing. Lookup distinguishes unresolved,
definitely uncommitted, committed, and expired/foreign handles. Expiration never
turns an old token into permission to execute again. Clients without retained
identity receive honest ambiguous outcomes, not a transparent exactly-once claim.

### Admission, failure and shutdown

Keep transaction workload admission separate from commit/checkpoint membership.
Resource escalation while holding intents/snapshots must either use already
protected capacity or refuse abortable work without waiting in a resource cycle.
Installers have a bounded reserved slow path under foreground, reader-pin, cache
and space pressure; ordinary resource exhaustion after admission is not an
acceptable reason to strand a healthy durable decision. Device/corruption failures
can still require recovery. No byte latch or arbitrary reader pin spans a barrier.
Structural, allocation, freezing and GC mutations join checkpoint capture authority.

Back pre-decision completion ownership with a concrete resource ledger: decision
bytes, status/history entries, structural/overflow allocation, frame/scratch and
worker/IO capacity, including transfer to recovery/completion ownership. Bound
competing structural growth and specify resource acquisition/release order. An
emergency pool is not a proof unless every supported install can progress from it;
overflow solves cell representability, not a shortage of frames or disk space.
Prove an affordable slow path before broadening admitted transaction shapes, not
an unbounded victim retry or a silent switch to another storage algorithm.

The store keeps exclusive ownership until all old runtime/device work is quiescent.
Fencing stops admission and wakes waiters; it is not permission to release the store
lock while a detached worker can still write. Close drains/resolves owned attempts,
or reports recovery-required without claiming a clean close. Snapshot/read entry
points and bounded continuations share the terminal failure boundary.

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

### Commit pipeline: three coherent candidates

**First proof path (C0): durable WAL first, then synchronous authoritative
installation, then contiguous visibility/ACK.** This minimizes the amount
of shared uncommitted data the reader has to understand. It is a correctness
baseline, not the desired final latency contract: index/overflow work on the
post-sync path and head-of-line waiting for earlier installers can dominate
an otherwise fast NVMe fsync.

**Challenger (C1): construct/install invisible versions before or concurrently
with the durability barrier.** All reader/index paths must check transaction
status and handle an uncommitted page split, eviction, abort and checkpoint
without leaking a version or invalidating the previous root. Publishing a
commit decision remains dependent on durable original effects, and success
still waits for a readable contiguous committed prefix. RocksDB's
[WritePrepared design](https://github.com/facebook/rocksdb/wiki/WritePrepared-Transactions)
is evidence that preparation/visibility indirection is possible, *not* that
its transaction protocols apply without modification here.

**Challenger (C2): publish a bounded, transaction-complete read overlay over
the durable logical log and install page/index structures asynchronously.**
An acknowledged transaction must be immediately visible to *all* point,
range, secondary-index, catalog and constraint reads at its promised
snapshot. The overlay must participate in the same read/write and predicate
certification, account for memory and rebuild/GC, and block or throttle before
it becomes unbounded. A log offset alone is not an index; no success may
rely on a missing index entry or incomplete replay. This is an optional
experiment **after** C0 proves a persistent end-to-end contract, not a
required second runtime or an excuse for weaker indexes.

**Committed-prefix rule:** the CSN provides a single read/ack ordering for
this initial single-node store. Speculative install may run in parallel,
but do not acknowledge a later CSN while an earlier durable transaction
is logically absent from its read view. Log sequence, commit acceptance,
durability frontier, physical installation and read visibility are
*different* states. An error or timeout after a durable decision is
unresolved/success-after-recovery, not a clean abort.

**First remove incidental serialization:** `src/vnext/commit_append.rs` builds
`PreparedLogBatch` inside the CSN lane; `src/vnext/log_io.rs` excludes append during
sync with one device-operation mutex. Prepare variable-size bodies outside ordering;
keep the ordered portion to final validation, sequence assignment and atomic
acceptance/evidence publication. A serialized append is acceptable for the first
recovery proof, not a selected multi-writer performance architecture. Positional
reservation/append must first prove framing, abandoned reservations, torn tails,
barrier coverage and absence of acknowledged effects beyond an unrecoverable hole.

Measure own installation separately from waiting for an earlier installer's prefix.
Advance C1/C2 only for a demonstrated end-to-end cost, including their extra read,
abort, recovery and memory work; neither is required to prove certification.
Compare sync/coalescing policies on the supported filesystem/device and durability
class. The [SIGMOD 2025 autonomous-commit paper](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/latency.pdf)
challenges group-commit queueing in its NVMe setup, not the need for filesystem
barriers or the value of batching on every device. It is not a SeerDB measurement.

### Serializable control and contentious writers

The initial executable reference uses decision-ordered optimistic
certification with a finite, exact read/absence/range dependency set and
atomic publication of accepted-uninstalled writes. It is **not** selected
as the only viable high-contention protocol. Compare against SSI-style
read/write dependency tracking (including read-only/safe snapshots) and
fine-grained lock or wait/recheck policies for hot-key cases. PostgreSQL's
[SSI design](https://wiki.postgresql.org/wiki/Serializable) documents
read/predicate dependencies and false-positive abort trade-offs.
[Silo](https://dspace.mit.edu/entities/publication/17029f9a-6379-4673-9f9f-1c36d23feb1b)
demonstrates a high-throughput in-memory optimistic serializable approach,
but its logging, page faults and snapshot history assumptions differ.

Register observations at the storage read boundary, including absence and
partial-range continuation, rather than trusting a later installer to recreate
reads. The kernel owns logical intent waiting, wakeup and deadlock policy;
OmenDB owns statement snapshot refresh and row/predicate re-evaluation. Waiting
never silently refreshes a fixed-snapshot transaction. Write predecessors and
mandatory catalog/constraint observations also require validation.

Keep indexed CSN-ordered summaries of accepted writes, including uninstalled
ones. Preparation may inspect old summaries outside the sequencer; acceptance
rechecks the remaining tail and publishes evidence without a validation gap.
Prune against the oldest live snapshot still allowed to write and outstanding
validators, not unrelated read-only, CDC or backup leases. A read-only capability
cannot upgrade to a writer after its certification history has been reclaimed.
Old live transactions end at restart, so their read dependencies and certification
indexes need not be logged for recovery. Persist effects/outcomes and retained
history, not every read set merely to avoid a second in-memory publication step.
An immutable accepted-write summary is published with ordering; auxiliary indexes
may build outside the lane only if validation also covers every unindexed tail
entry. Persistent prepared transactions would require a separate recovery protocol.

Predicate spans describe logically evaluated index ranges, including absence
and schema/enforcement membership, not physical leaf boundaries. Conservative
coarsening/spill is allowed; measure false positives. Pressure can refuse or
explicitly expire abortable work, never discard evidence and certify success or
abort an accepted unresolved decision. Do not replay arbitrary client work as a
hidden response to pressure.

The first proof uses a homogeneous serializable writer domain. A read-only
snapshot can serialize at its prefix without validation only when that prefix
was produced by appropriately certified writers. Before enabling mixed weaker
writers, define the guarantee for interacting transactions and prove it; merely
certifying the SERIALIZABLE transactions does not make a mixed history globally
serializable. Constraint/catalog integrity remains mandatory in every supported
mode. Default isolation remains a separate measured product decision.

## Physical access and hot-path policy

- Initial ordered structure: mutable buffered B+/B-link tree with fine-grained
  guards, high fences/right siblings and generic current records carrying visibility
  metadata. Older versions use the history service below. OmenDB layers its row/index
  codecs over it; the independent KV facade uses the same tree.
- Variable-size records need bounded *physical* cell descriptors and logical
  overflow for oversized keys, separators, fences and values. Admission proves
  fully fenced actual leaf/internal/root split candidates and replay/history
  representability; an empty-leaf fit check is insufficient. Derive bounds from
  the actual codec, including fences, tags, slots, alignment and useful internal
  fanout. Finish parent/root obligations before readiness.
- Immutable overflow binds each chunk to its chain identity/incarnation, object,
  total length and position, with exact successor/termination validation. A valid
  same-object chunk from another chain must not satisfy a spliced reference.
  Resolve ambiguous logical key prefixes outside byte latches, then revalidate
  the guarded descriptor/version. Acquire chain liveness before releasing the
  descriptor guard; bound traversal, I/O and decode memory.
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

## MVCC, history, and checkpoint economics

Treat the **latest readable version, older snapshot history, committed log,
checkpoint coverage, and retained CDC/backup/replication history as one
lifetime system**. The reference is compact current records and a bounded recent
version cache, with older complete immutable before-images addressed by an H1
history directory keyed by `(object incarnation, encoded logical key, begin CSN)`.
Its logical-key identity/order matches the current tree; values carry exclusive
end CSN and a complete value or absence marker. Each retained key has a birth CSN
and a current head's commit identity/put-or-delete kind in its anchor or owned
version metadata. Birth means the first insertion for the retained anchor's lifetime,
not the latest reinsertion; it survives delete/reinsert and may reset only after the
old anchor can be reclaimed. Before birth the key is absent; afterwards the current,
recent and historical views provide complete interval coverage within the readable horizon,
including absence intervals across delete/reinsert. After checking current/recent
versions, seek the greatest historical begin CSN not exceeding the snapshot and
check its end/visibility. Missing required coverage or payload is corruption, not
an empty result. This selects a seekable path, not physical byte layout or a proven
performance winner. SQL key/RowId semantics remain in OmenDB; current/index/catalog
objects share these storage obligations.

Kernel MVCC owns anchor lifetime under readable-snapshot leases. Range reads
enumerate retained anchors and resolve the same version as a point read; they do
not mistake the newest head for visibility at an older CSN. Retain anchors for
deleted/moved keys while supported snapshots can need them. A history point seek
alone is not old range completeness; early anchor removal would require a complete
historical-key merge path. Charge retained anchors and invisible-key scan work.

The history directory indexes immutable version records. Its own physical
maintenance/GC joins WAL/checkpoint authority, not recursive user-MVCC updates
that generate history-of-history.

Accept the directory's write/space cost in the first reference and measure it
with checkpoint and relocation metadata. Reverse links with indexed immutable
segments (H0) are a challenger if they reduce that total cost without linear
old-version lookup. Start with full images; bounded deltas or inline recent
versions earn their reconstruction, GC and replay complexity through sparse-update
measurements. No separate history durability authority is implied.
WiredTiger's [history store](https://source.wiredtiger.com/develop/arch-hs.html)
and [checkpoint coordination](https://source.wiredtiger.com/develop/arch-checkpoint.html)
illustrate current/history separation and closure, not an OmenDB format prescription.

Compare recent hot reads, a one-hour pinned reader under churn, historical
random lookups, version lookup I/O, index and status memory, write
amplification, retained disk growth, GC debt, checkpoint interruption,
recovery time and constrained RAM. A bounded count of in-memory version
offsets is not sufficient if recovery reads an unbounded WAL or every
history lookup takes a shared file mutex.

Never collapse different retention units into a single numerical floor:
**reader snapshot CSNs**, **writer/validator certification-summary CSN bases**,
**LSN log replay**, history-version references and external lease epochs have
separate authority. An eviction may flush a
version to history only after its identity, log and checkpoint dependencies
are satisfied. A checkpoint's published manifest must identify a logically
closed, replayable state and all history needed by retained readers; old
manifest roots and log/history extents survive until every reader,
replica, consumer and backup lease is released.

**Concurrent structural admission:** checking whether one mutation fits
today's leaf is not proof it will install after other accepted transactions
split or grow that tree. Before any irrevocable commit, prove an affordable
protected completion plan for every admitted shape, including concurrent growth,
pins, structural/overflow allocation and scratch. Bounded reservations or a slow
path are mechanisms within that proof, not an unspecified fallback escape. An
unexpected post-decision device/allocation failure preserves the durable outcome
and may require fencing/recovery, never a clean abort. Only a separately selected
and fully qualified C2 overlay may continue serving complete logical effects
without finishing the page installation first.
Do not reserve an impractically large worst-case buffer for every ordinary
point write without measuring memory efficiency. Treat tree, immutable history,
page mapping and out-of-place checkpoints as one maintenance system: choosing a
B-tree does not eliminate rewrite/GC debt or establish lower amplification than
an LSM. Count steady-state total bytes, reserve occupancy and tail latency.

## WAL, checkpoint, recovery, and reclamation

The complete recoverable log plus a **structurally closed published checkpoint**
must reconstruct current values, canonical index/catalog effects, transaction
outcomes and retained history. A separate synchronized before-image file in the
current implementation is required until this complete-log replacement actually
passes history/crash qualification. Large external payloads can require distinct
durable dependencies; do not promise a universal one-fsync transaction.

The runtime owns one materialization-requirements/durability authority shared by
coordinator, buffer and page store, distinct from certification dependencies.
Attach requirements while the exact image remains writer-owned and pinned, before
it can be dirtied/evicted; split copies, parents, roots and extents inherit them.
Post-hoc attachment by logical page key is not safe. Open re-establishes the owned
WAL/history durability frontiers before writeback is enabled; a caller cannot
accidentally append on one log and authorize materialization against another.

- Versioned framed WAL identifies original effects, transaction boundaries,
  counts and digests, and store incarnation. Checksums detect accidental
  corruption, not malicious modification; cryptographic authentication is not
  implied. Support large transactions through bounded framing/spill or deliberate
  pre-append refusal; do not let the current one-segment implementation become a
  product limit by accident. Fragmented effects need one complete final decision
  and retention back to every frame it requires, even with interleaved writers.
  A checkpoint/consumer LSN cannot cut away the beginning of a retained decision.
- Checkpoint capture binds an exact visible CSN/decision LSN, root/catalog/
  object graph, B-link/fences, overflow/extent closure, allocation/page maps,
  status/history and all leases. Publish checksummed manifest *only after*
  dependent bytes are durable; preserve prior valid authority on failures.
  Persist/resume every relevant allocation namespace, not only page IDs: object,
  transaction, logical version, map/epoch and WAL identity. Resume CSNs above both
  the checkpoint prefix and validated suffix. Exhaustion refuses without wrapping.
  Every reachable current/history owner must have a recoverable outcome or safe
  frozen CSN before its decision can be reclaimed; freezing does not erase replay
  identity still needed by the retained suffix.
- A conservative quiesced capture is the first correctness proof. Design and
  qualify an **incremental/epoch capture** with bounded pause and temporary
  resource use for multi-TB databases before treating local checkpointing as
  production scalable.
- Recovery validates checkpoint and committed suffix, streams effects with
  bounded memory, refuses complete corruption/foreign references, resolves
  incomplete trailing frames only when provably safe, and replays complete
  transactions through the same logical reducer as live commits. Validate original
  count/digest/ordinals before reducing to final effects. Retain `(TxnId, final
  ordinal)` within each logical object/key: identical replay is allocation-free,
  conflicting content is corruption, and a newer page LSN is not proof an older
  independent effect was applied. Logical idempotence does not finish a split.
  Logical history identities survive relocation/segment GC and never alias a new
  before-image; validate owner/key/chain binding, not just decreasing offsets.
  Crash during recovery, two reopens and subsequent ordinary write must work.
  Repeated failed retries cannot accumulate unbounded orphan pages.
- Physical page map publication and duplicate validation must scale
  subquadratically and preferably incrementally with changed pages; avoid
  writing/validating a complete map on every small checkpoint or mutation.
  Reclamation requires proofs against prior checkpoint, oldest readable
  snapshot, CDC/backup, future replica and outstanding physical reader pins.
- Snapshot/bootstrap is an atomically retained handoff of source incarnation,
  catalog/layout, snapshot CSN, change restart LSN and history/delta leases.
  The current `{CSN, LSN}` exported pair is insufficient on its own.

### Genesis replay and selection of restart authority

S1 has one explicit authority mode: store-bound genesis plus the complete retained
logical log, including object lifecycle. Surviving working page/undo/map files are
not adopted as a newer database. Rebuild in disposable working epochs under store
exclusion; retries reclaim abandoned epochs without touching authoritative log or
retained checkpoints. Repeated opens must not append another permanent copy of
reconstructed history. Physical undo offsets cannot escape as restart-stable IDs.

S3 switches authority only through complete manifest publication. For the quiesced
C0 baseline, drain accepted decisions through visibility before choosing the cut;
no partially installed transaction belongs to its checkpoint prefix. A later
incremental scheme must prove coherent images/history despite concurrent mutation,
not substitute per-page maximum LSNs for that proof. Lease registration and retention
capture share authority. Capture uses bounded stable images rather than waiting
indefinitely for arbitrary reader pins.

An older checkpoint is usable only with the complete validated log/history suffix
needed to recover all required decisions. Do not interpret "prior-root fallback"
as permission to skip complete corruption or lose acknowledged work after log GC.
Outcome, history, allocation, object/overflow and prior-checkpoint closure must all
hold before deleting their recovery inputs.

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

