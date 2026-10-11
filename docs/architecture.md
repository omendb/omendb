# OmenDB architecture

**Status:** accepted DBNext target, not a description of a completed release.
The current server uses the existing ordered-KV SeerDB engine. The replacement
storage kernel is developed under `crates/seerdb/src/vnext` and is not yet a
restart-safe product backend. [ADR 0013](adr/0013-storage-kernel-and-access-methods.md)
owns the kernel boundary, [ADR 0014](adr/0014-vnext-installation-and-recovery.md)
owns installation/recovery, and the [vNext plan](plans/storage-kernel-vnext.md)
owns implementation order and unfinished gates.

OmenDB is a server-first relational database in Rust, optimized first for OLTP
on RAM and local NVMe. PostgreSQL compatibility belongs at deliberate external
boundaries: wire protocol, SQL behavior, drivers and tooling. It does not require
PostgreSQL's internal pages, WAL, executor or process-per-connection model.
The direct Rust API uses the same transaction semantics; it is not a separate
storage product.

## Current implementation versus target

| Concern | Current product path | Target |
| --- | --- | --- |
| Storage | `src/seer_direct.rs` over transactional ordered KV | Shared transaction/storage kernel with specialized access methods |
| Physical updates | Generation-COW engine | Buffered pages, logical MVCC, log-authoritative commit, asynchronous out-of-place materialization |
| Server concurrency | `Arc<RwLock<RelationalDatabase>>`, lock polling and `spawn_blocking` | Concurrent database handle and bounded execution/admission runtime |
| Rows and execution | Bootstrap row codec and materialized row results | Schema-driven guarded row views, OLTP micro-plans and typed batch pipelines |
| Planning | SQL classification, executable Describe probes and mixed catalog views remain | Binder-owned types, privileges, effects, dependencies and result schema from one transaction/catalog view |
| Operation failure | Some errors leave committable partial relational state | Private statement delta or failed transaction; no partial row/index/catalog commit |
| Analytics/search | Future integration | Snapshot-qualified access paths over the same transactional authority |

The existing engine remains a semantic and crash/fault reference until the
replacement passes correctness and representative performance gates. Known
legacy defects in the [gap register](gap-register.md) are not the specification;
independent PostgreSQL/SQLite semantics and accepted contracts resolve those
cases. There is no permanent backend matrix. Cutover deletes the superseded physical engine;
a temporary adapter is qualification machinery, not product extensibility.

## Target ownership

```text
PostgreSQL clients / direct Rust API
                  |
       session / auth / protocol / SQL
                  |
      binder / optimizer / typed plans
                  |
       micro-plans + batch execution
                  |
       relational catalog + semantics
                  |
     shared transaction/storage kernel
       /         |           |       \
 ordered      canonical   specialized  derived
 B-trees      records     access paths representations
       \         |           |       /
         MVCC / log / buffer / lifetime
                  |
       RAM / local NVMe / optional cold tiers
```

### OmenDB

OmenDB owns SQL types and NULL behavior, schemas, row-layout versions, primary
and secondary index meaning, constraints, authorization, DDL, planning and
execution. It interprets committed changes as relational changes and owns
metadata for analytical/search representations and future placement.

### SeerDB

SeerDB owns transaction identities and outcomes, snapshots, visibility,
conflicts/intents, durable logging, recovery, buffering, page translation,
materialization, physical allocation and retention. These services do not
acquire SQL, BM25, vector-distance or graph-query semantics.

`TreeId + ordered key bytes + opaque value bytes` describes the **current
facade**, not the universal physical boundary. Ordered B-trees are the first
access method. Canonical record storage and later specialized structures may
use different layouts while sharing kernel transaction, log and lifetime
services. Access methods start as concrete compiled types; no runtime plugin
ABI or speculative universal trait hierarchy is required.

The first physical comparison is clustered primary-B-tree rows versus row/heap
pages plus a primary index. Later vector, text, graph and analytical structures
must prove both useful specialized performance and transactional composability.
See [ADR 0013](adr/0013-storage-kernel-and-access-methods.md).

### Packages and sibling engines

OmenDB is `AGPL-3.0-only`. SeerDB and
[`durable-fs`](../crates/durable-fs/README.md) are independently versioned
`Apache-2.0` crates maintained in this workspace. Package independence must not
force an artificial internal optimization boundary. External `durable-fs`
consumers pin an OmenDB Git revision until a registry release is available.
Shared filesystem helpers do not establish shared transaction authority.

DBNext's integrated modalities share one catalog/log/transaction authority.
The eventual product relationship with standalone OmenDB Vector and OmenDB OLAP
is **open**: this design does not authorize their archival or wholesale merge.
Independent deployments have independent transaction outcomes; asynchronous
integration must expose that boundary rather than claim cross-engine atomicity.
See [ADR 0012](adr/0012-unified-modalities-and-rewrite-strategy.md).

## Transaction and persistence authority

Keep these identities distinct:

```text
TxnId                transaction identity, not commit order
CSN                  logical committed visibility order
LSN                  physical durable-log position
PageId / frame ID    storage/residency identities, not logical row identity
store incarnation    namespace for persisted identities and recovery state
```

One durable decision covers every authoritative mutation in a transaction:
canonical records, required primary/unique/constraint indexes and catalog state.
Physical page placement cannot create or revoke that outcome.

The vNext baseline is:

```text
private writes
  -> validate effects / acquire intents / preflight predecessors
  -> append complete transaction + decision in CSN order
  -> synchronize decision WAL
  -> synchronize required undo
  -> install every authoritative effect with exact page dependencies
  -> publish status and readiness
  -> wait for contiguous visibility coverage
  -> acknowledge success
```

A durable decision is irrevocable commit authority; readiness alone is not an
acknowledgment. A failed or canceled request after possible decision append is
not safely retryable as an ordinary abort. Recovery resolves the outcome; future
retry support needs explicit durable request identity or outcome lookup.

Dirty pages materialize out-of-place. A page image may be written only when WAL
and undo durability cover its exact dependencies. A maximum page LSN does not
prove every earlier logical effect is installed. Working spill pages are not
restart authority without a complete checkpoint containing roots, object and
allocation metadata, owner outcomes, retention state and a validated page map.
Recovery loads that checkpoint and applies the synchronized committed WAL suffix
before exposing the recovered visibility frontier.

One runtime lifecycle owns admission, checkpoint drain, terminal failure,
read/snapshot safety and pending-commit wakeup. The process-local
`RuntimeAdmission` primitive now blocks new commits during checkpoint drain and
holds admitted ordered commits through contiguous visibility completion. Its
recovery-required state refuses new read/commit admission and wakes drain waiters.

Full integration remains a cutover blocker: every read/snapshot/server entry point
and long-running operation must consult that gate, and structural/GC/allocation
mutators outside the ordered commit path must quiesce during graph capture.
Independent component fences or an optionally attached coordinator gate do not
establish a complete persistent runtime. See
[ADR 0014](adr/0014-vnext-installation-and-recovery.md).

## Derived state and snapshot correctness

Rebuildability does not permit stale or future SQL results. A representation
must identify its source incarnation, object/schema dependencies, publication
generation, complete applied frontier and retained snapshot validity.

An applied frontier `F` is not proof that every snapshot `X <= F` is readable.
A latest-only representation at 100 cannot answer snapshot 90 after overwriting
90's values. Use it only at a supported snapshot, reconstruct that snapshot from
retained versions/deltas, or fall back to authoritative data. Compaction advances
the oldest readable boundary; it cannot invalidate an admitted reader silently.
The common contract is in
[ADR 0013](adr/0013-storage-kernel-and-access-methods.md#3-storage-objects-have-explicit-authority-classes).

ANN may approximate ranking under an explicit recall contract, but not row
identity, deletion visibility or authorization. Search candidates must be
validated against the query's canonical snapshot and predicates. Deleted,
replaced or newly unauthorized rows cannot leak through a stale accelerator.

## Snapshots and committed changes

The target bootstrap boundary remains:

```text
snapshot CSN X + restart position Y + retained source identity/lease
  -> copy the pinned snapshot
  -> consume complete committed transactions after the cut
  -> durably apply effects and consumer progress together
  -> publish consumer coverage
  -> advance retention only after durable consumer acknowledgment
```

The pair `(X,Y)` alone is not a live snapshot handle, a retention lease or a
transport protocol. The current `TransactionDatabase::snapshot_export` reports
a durable position; it does not atomically pin a copyable snapshot and change
suffix. A gap-free exported snapshot/lease handoff remains a target contract.
Logical consumer progress and physical WAL retention/recovery floors must remain
distinct, even if a baseline happens to use the same number.

Consumers must tolerate replay after a crash, apply transaction boundaries
atomically, bind positions to a source incarnation, and fail explicitly if
required history is gone. Physical WAL framing is not the stable public CDC
format. The shared bootstrap/retention contract is in
[ADR 0013](adr/0013-storage-kernel-and-access-methods.md#11-snapshotbootstrap-is-a-retained-handoff-not-just-two-numbers).

## Runtime, planning and data layout

Logical client sessions are not OS workers. Network/protocol tasks submit bound
operations to a bounded runtime. Short OLTP work benefits from worker locality;
scans, joins, builds and maintenance expose bounded resumable tasks. Admission
accounts for memory, spill, CPU and I/O rather than a single abstract cost.
Durability, recovery and essential reclamation retain progress capacity even
under foreground or analytical saturation. See
[ADR 0007](adr/0007-runtime-execution-and-contention.md).

Parsing, binding, authorization, Describe, planning and execution share one
transaction/catalog view and semantic metadata. A relational operation owns its
private mutation delta: before durable commit authority, failure restores that
delta or leaves a failed transaction that must roll back. After the decision,
response delivery failure cannot abort the commit. Describe must not execute a
fabricated user query to infer its schema.
Prepared plans retain catalog-object dependencies rather than relying on one
whole-catalog invalidation marker. Constraint enforcement uses mutation-driven
probes, not routine whole-table foreign-key scans.

Point/range OLTP uses compact micro-plans and short-lived guarded row views.
Native scan/access-method interfaces expose typed batches and selection vectors;
row-at-a-time iterators are adapters, not the mandatory hot-path boundary.
Scans, joins and aggregates decode only needed columns, and analytical execution
should operate directly on queryable lightweight encodings where useful. The schema-driven row codec includes layout versions,
NULL metadata and typed payloads; additional families or large-value indirection
require measured benefit and explicit relocation/retention semantics. JIT, custom
latches and SIMD refinements follow profiles rather than define semantics.
See [ADR 0010](adr/0010-row-layout-and-column-families.md).

## Analytics, tiers and distribution

In-engine batch scans over authoritative rows are the first analytical path.
Optional columnar, search and graph representations are snapshot-qualified
acceleration, not second transaction histories. A single-copy hybrid row/column
store remains an experiment requiring mixed-workload and recovery evidence.
Remote workers are optional and do not silently become the independently
writable OLAP product. See [ADR 0011](adr/0011-htap-and-analytical-representations.md).

Local mode needs no cloud service or cluster scheduler. RAM plus NVMe is the
initial hot/durable profile. Object storage suits immutable checkpoints,
backups, archives and analytical chunks; it is not the default random-write OLTP
device. Regional HA adds a quorum log only after local recovery qualification;
future disaggregation changes placement, not transaction meaning. See
[ADR 0006](adr/0006-deployment-storage-and-durability.md).

Distribution adds stable range/placement-group identities, ownership epochs,
replica sets and cross-range coordination only where needed. Plans retain
symbolic routing; execution resolves current placement. Controllers reconcile
topology but do not grant database commit authority. An outer resource scheduler,
including Archon, may manage processes and resource allocations; its leases do
not replace database replication/leadership fencing. See
[ADR 0008](adr/0008-distribution-ranges-and-global-topology.md).

## Qualification and implementation order

The [vNext plan](plans/storage-kernel-vnext.md) is the sole detailed execution
sequence. In summary:

1. Complete runtime admission/failure and checkpoint/restart authority.
2. Qualify bounded resources, retention and crash/reopen behavior.
3. Measure the actual vNext path, including checkpoint pauses, serial WAL/undo
   barriers, visibility waits and larger-than-memory operation.
4. Add canonical rows and cross-object relational qualification; compare
   clustered versus heap placement.
5. Pass product semantic/crash oracles and agreed performance budgets, cut over,
   and delete the superseded engine.

Keep fault effects and scheduling explicit enough to support deterministic
exploration of concurrency and later distributed interleavings. Simulation must
exercise the same state machines, not a separate mock database. Qualification
includes two consecutive reopens at persistence boundaries and recovery memory
versus retained history. Current recovery structures still grow with history;
there is no bounded-memory large-history claim yet.

Measure the entire memory hierarchy, including LLC/TLB/page walks, bandwidth
and NUMA placement where applicable, not just buffer hit rate. Huge pages,
validated translation hints, physical row/version references and large-value
arenas remain conditional experiments with lifecycle and end-to-end evidence.

No default translation, page size, MVCC layout, autonomous commit policy or
analytical format is established by another system's benchmark. The
[alpha gates](alpha-release-gates.md) define release evidence; the
[benchmark documentation](benchmarks.md) distinguishes product regression smoke
from replacement-kernel qualification.
