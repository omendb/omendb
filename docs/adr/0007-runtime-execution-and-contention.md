# ADR 0007: Runtime, execution, and contention architecture

- **Status:** accepted target architecture; implementation staged
- **Scope:** OmenDB server runtime, SQL execution, scheduling, and transaction contention
- **Depends on:** [ADR 0001](0001-seerdb-transaction-contract.md) and
  [ADR 0006](0006-deployment-storage-and-durability.md)

## Context

The current server proves PostgreSQL-wire lifecycle, authentication,
cancellation, and multi-session correctness, but it still contains development
shapes that must not become the final execution architecture:

- `Arc<RwLock<RelationalDatabase>>` is an outer database-wide synchronization
  boundary;
- blocking statements are submitted through Tokio `spawn_blocking` rather than
  OmenDB's own fixed execution runtime;
- the resource governor is not yet the server's actual admission/scheduling
  authority;
- SQL/wire code still classifies some statements from source text and Describe
  may execute a probe transaction to infer a result shape;
- the scan executor is row-oriented and some morsel paths rescan previously
  consumed rows;
- snapshot isolation rejects hot-key writers instead of providing an adaptive
  wait path for workloads where retries are pathological.

The target should exploit modern many-core CPUs without requiring applications
to understand those implementation details.

## Decision

### 1. `Database` becomes a cloneable concurrent handle

The final server does not protect the entire relational database behind one
outer read/write lock. The target shape is conceptually:

```text
Database
  Arc<DatabaseInner>
      transaction engine
      immutable/versioned catalog root
      plan/catalog caches
      runtime/admission handles
```

Ordinary reads, transactions, DML, planning, metrics, and commit require shared
access through engine-owned synchronization. DDL coordinates through the schema
transaction/catalog protocol, not an exclusive Rust guard around unrelated
query execution.

A database-wide mutex/RwLock may exist inside narrowly scoped maintenance or
bootstrap operations, but it is not the concurrency protocol for user queries.

### 2. Logical sessions are independent from execution workers

A client session owns protocol and SQL state, not a dedicated operating-system
thread and not a reusable hidden backend process.

```text
network/reactor
     |
logical session
     |
prepared/bound plan
     |
admission + scheduler
     |
fixed execution workers
```

Session state is explicit and durable only when SQL semantics require it. This
avoids the backend-session virtualization/scrubbing complexity required when a
pooler multiplexes clients over independent PostgreSQL processes.

### 3. Fixed workers with affinity for short OLTP; stealable tasks for batch work

The server owns a bounded worker set, normally near the usable CPU count.
Short transactions are assigned a home worker using stable affinity so their
transaction state, hot metadata, and common pages remain cache-local.

Long scans, joins, aggregates, index builds, and maintenance operations may
split into bounded tasks/morsels that idle workers can steal. Cross-worker
message passing is explicit where ownership matters.

This is a hybrid rather than a dogmatic shard-per-core design: cache locality
is preferred, but transactions and ordered trees are not permanently split by
CPU identity.

NUMA topology is part of worker/data placement. Future CXL/disaggregated memory
is treated as another placement tier, not as a new SQL execution model.

### 4. Progress-critical work has protected capacity

The runtime distinguishes at least:

1. **durability/progress:** WAL/log completion, recovery, essential reclaim;
2. **foreground latency:** short OLTP and metadata required to serve it;
3. **elastic work:** scans, analytical pipelines, index builds, compaction,
   non-urgent schema maintenance.

Unused protected capacity may be borrowed, but sustained foreground work must
not starve WAL completion or reclamation required for continued progress.
Conversely, background work must not consume unbounded memory/I/O and destroy
OLTP tail latency.

The current scalar `cost` governor evolves into reservations over real
resources, including memory bytes, spill/storage budget, I/O operations or
bytes, and CPU work/quanta. Read-ahead/prefetch queues consume the same bounded
memory/I/O budgets rather than creating a hidden source of pressure. Future
distributed coordinators extend the same model to exchange buffers, remote
result queues and network bytes. Admission errors remain explicit.

Admission that can park a session is transaction-grain: acquire it before the
transaction takes a snapshot, intents or locks, and retain ownership until the
transaction ends. Later statements, nested execution and completion work of an
already-admitted transaction do not queue behind work waiting for its locks.
Idle or aborted explicit transactions still own resources; account for them and
bound their lifetime rather than releasing and reacquiring their admission slot.

This workload/resource admission is distinct from vNext `RuntimeAdmission`'s
checkpoint cut. The latter drains committing mutations through installation and
visibility; it is not an overload scheduler. Checkpoint closure must not prevent
an admitted installer, WAL completion or rollback from making the progress needed
by the drain. One runtime owns their composition, with separately observable
queueing, active-transaction and checkpoint states.

### 5. Bind once; wire protocol does not infer semantics from SQL text

Parsing produces an AST. Binding/semantic analysis produces a stable object
containing at least:

```text
BoundStatement
  parameter types
  result schema
  resolved object/column IDs and typed expressions/coercions
  catalog/layout/name/enforcement dependencies from the transaction view
  effects (read/write/schema/session/transaction control)
  required privileges
  typed logical plan
```

PostgreSQL Parse/Describe/Bind/Execute, authorization, admission, plan caching,
and result encoding consume this shared semantic representation.

`Describe` must never execute a user query with fabricated values to discover
its type. Transaction control, DDL classification, RETURNING behavior, and
row-producing behavior are AST/binder properties, not whitespace/token-prefix
heuristics. Authorization precedes both Describe and execution; Describe may not
infer protected data through expression errors. A prepared plan is not a cached
authorization grant. Transaction control is session/transaction lifecycle, not
schema administration; rollback remains available for cleanup.

Binding, access-path selection, row identity and decoding consume one retained
transaction/catalog view, including object lifecycle leases. Parameter values may
specialize a plan, but cannot be required to discover result types. Replace the
competing projection, RETURNING and Describe semantic paths with one typed binder;
a Describe-only type walker would preserve the underlying inconsistency.

Authorization uses **current committed grants at each Describe/execution boundary**,
not grants pinned to the data snapshot or cached plan. Retain the authorization
view for that statement. Revocation alone does not cancel already-authorized work
or trigger commit-time reauthorization. Later boundaries check current grants;
an authorization error in an explicit transaction still makes the block rollback-only,
including its earlier staged writes. This policy is not implicit-savepoint behavior
or automatic cancellation. Explicit cancellation remains phase-aware;
rollback/cleanup never requires schema-admin permission. A trusted direct API may
retain an explicit privileged capability rather than adopting wire authentication.
The current serving implementation does not yet qualify this complete policy.

### 6. Two physical execution paths share one typed logical plan

OmenDB keeps the existing architectural split but makes it concrete:

#### OLTP micro-plans

Prepared point/range lookups, short DML, FK/unique probes, and simple joins use
compact typed operations with minimal allocation and dispatch overhead. They do
not materialize generic `Vec<Row>`/`Vec<Value>` intermediates on hot paths.

#### Batch pipelines

Scans, joins, aggregates, sorts, windows, index builds, and analytical work use
columnar/typed batches:

```text
storage cursor -> typed batch -> filter -> project -> join/aggregate -> sink
```

A batch carries typed vectors, null/selection state, and short-lived references
when page guards permit them. Operators are cancellation/admission checkpoints.
The storage cursor advances from its current position; pagination/morsels never
rescan the prefix already consumed. Its contract is conceptually
`next_batch(row_limit, byte_limit, work_budget) -> Batch | Yield | End`, retaining
snapshot/catalog/overlay identity and strict logical continuation. A root re-descent
from the last key is valid; an inclusive restart or repeated prefix scan is not.
Budget physical slots, invisible/tombstoned records, undo hops, chain reads and
I/O—not only visible rows. Release byte latches before suspension/output, bound
retained pins, and copy surviving fields into budgeted batch storage.

For scan/filter/visibility-heavy paths, batching is also the preferred internal
storage/access-method interface, not merely an executor wrapper. Access methods
should expose enough independent work for SIMD, prefetch, compressed decoding,
and selection-vector production. Point lookups and short micro-plans stay direct;
row-at-a-time iterators may adapt a batch cursor but must not force the hot scan
path into one-item dispatch.

### 7. Execution has an explicit database IR with tiered optimization

The binder/planner lowers into a database-specific typed IR rather than making
Rust call structure or PostgreSQL executor nodes the long-term execution ABI.

Execution tiers may be:

1. direct specialized micro-plan / compact interpreter for very short work;
2. vectorized pipeline for medium/large work;
3. optional hot-plan JIT using low-overhead code generation.

LLVM is not required. Copy-and-patch or similarly cheap JIT techniques are
valid candidates because compilation must not dominate short queries. Any JIT
must support at least x86-64 and AArch64 or retain an efficient portable path.

Runtime-dispatched SIMD kernels handle architecture-specific vectorization.
The logical IR remains architecture-neutral. JIT/code generation is a later
optimization: it should not precede a measured batch path and stable data
representation merely because compilation technology is available.

### 8. Contention management is adaptive

Pure first-committer-wins OCC is efficient when conflicts are rare and useful
for distributed systems, but repeated application retries are the wrong normal
behavior for hot-row local OLTP.

SeerDB therefore targets an adaptive hybrid:

- uncontended reads/writes use the current low-overhead optimistic path;
- a logical key/range that becomes contended may acquire a lightweight queued
  write-intent/parking structure;
- `READ COMMITTED` writers can wait for the prior writer, refresh/recheck the
  statement-visible row, and proceed without forcing the application to retry;
- repeatable-read/snapshot transactions retain fixed-snapshot guarantees and
  may still fail after waiting if the predecessor invalidates their snapshot;
- serializable mode layers dependency certification on the same primitives;
- deadlock avoidance/detection is explicit when operations can wait on multiple
  logical resources.

The fast uncontended path must not pay a heavyweight lock-manager cost merely
because a wait path exists.

For distributed ranges, the same logical intent API may be backed by a
different coordination mechanism; SQL semantics do not change.

### 9. Constraints are mutation-driven, not database rescans

Normal DML records only the constraint work its mutations introduce:

- child insert/update -> point/prefix lookup in referenced unique index;
- parent update/delete -> lookup in the child-side FK index;
- unique key write -> bounded index prefix probe;
- cascades operate on matched child ranges, not full child-table scans.

Creating a new constraint may scan existing rows once, but validation and the
catalog publication occur in the **same schema transaction** with registered
range dependencies. There is no validate-then-publish TOCTOU seam. Authoritative
child-side FK lookup makes parent checks selective. Cascade expansion uses a
bounded worklist with explicit visited/effect ownership, not repeated recursive
expansion; supported action semantics and explicit resource refusal remain.

Constraint observations are mandatory certification dependencies even below
SERIALIZABLE. [ADR 0014](0014-vnext-installation-and-recovery.md#2-decision-ordered-optimistic-certification)
owns their decision-order contract; a validate callback before an unprotected
append is insufficient. Adding/removing an index or constraint changes a table's
enforcement membership dependency, so an old writer cannot silently omit new
maintenance obligations.

The target makes timing observable at its named boundary: nondeferrable unique
and row constraints enforce their required mutation checks; immediate FKs must
satisfy the constraint before a statement succeeds; explicitly deferred constraints
resolve before transaction decision. Referential actions keep their declared
semantics. The legacy typed `Immediate` and `DeferredToPublication` both currently
validate FKs at publication, while SQL calls them immediate. That is a contract
mismatch, not evidence of statement-time compatibility. Revise the experimental
typed timing interface deliberately when migrating; do not rename expectations
to bless legacy output or export deferred behavior as nondeferrable SQL.
[PostgreSQL constraint timing](https://www.postgresql.org/docs/18/sql-set-constraints.html)
is the independent comparison (checked 2026-10-11).

All DDL funnels through one schema-transaction protocol; bespoke per-command
publication paths are transitional.

A relational transaction owns failure state and each statement's complete private
delta: base rows, indexes, cascades, constraint work, catalog effects and resources.
Bind/authorize, execute against a fixed statement input view, validate immediate
constraints and admit results before merging the complete delta. DML scans cannot
consume their own new effects or update the same row repeatedly. Earlier successful
statement effects and supporting dependencies remain retained.

Any explicit-block bind/auth/execute/cancel/precommit result-admission error makes
the transaction rollback-only; direct and wire callers share that state. Private
deltas provide atomic operation scope, not an implicit savepoint feature or permission
to commit a failed block. No partial row/index/catalog state is committable. After
possible decision append, kernel-owned responsibility replaces ordinary rollback;
post-decision encoding failure/disconnect cannot convert the commit into an abort.
One internal schema transaction does not by itself add SQL DDL-in-block support.

### 10. Catalog state is immutable and versioned at object granularity

The current whole-catalog marker is a correctness-friendly bootstrap format,
not the final concurrency boundary.

The target catalog has versioned database/schema/table/index/constraint
objects plus a catalog root/epoch. Transactions and cached plans record the
specific object versions they depend on. Unrelated DDL need not invalidate
all writers or every prepared plan.

Dependencies include namespace/name absence and each table's enforcement membership,
not just object versions already found. Classify semantic dependencies requiring
refusal/rebind, physical-plan dependencies requiring replanning, and statistics
that do not invalidate semantics. Retain layouts and logically retired objects
until reader/recovery leases end; logical retirement is not physical reclamation.

Catalog data remains ordinary transactional SeerDB data; it does not become a
second metadata database with separate durability.

### 11. Deterministic simulation is a core correctness facility

Fault injection remains, but concurrency, timing, and distributed work need a
deterministic simulation boundary as the system grows.

Core transaction/distribution code must avoid uncontrolled wall-clock time,
randomness, thread scheduling assumptions, and direct network/device calls.
Runtime adapters provide those effects so tests can deterministically explore:

- worker interleavings;
- delayed/reordered I/O completions;
- crashes at every durability boundary;
- network partitions and replica loss;
- clock/timer events;
- GC/compaction racing readers/writers;
- failover and reshard transitions.

Production remains normal Rust/native I/O; simulation is an alternate runtime,
not a mock storage implementation with different semantics.

## Hardware principles

- Avoid shared hot cache lines and unnecessary cross-core ownership changes.
- Reuse transaction/query arenas to eliminate per-row heap churn.
- Prefer compact data and cache-friendly hash/index structures.
- Keep enough independent work in flight to overlap modern NVMe/network I/O
  without creating unbounded queues.
- Use performance and efficiency cores appropriately on heterogeneous CPUs
  when the platform exposes reliable topology; correctness never depends on
  core type.
- Apple Silicon and AArch64 are first-class execution targets, not slow
  fallback architectures.

## Acceptance gates

Before declaring this runtime architecture implemented:

- autocommit and explicit writes execute concurrently without a database-wide
  outer lock;
- bounded worker count replaces one blocking task per statement;
- WAL/reclaim progress is demonstrably not starved under sustained OLTP;
- admission never parks a lock-holding transaction behind its waiters; explicit
  transactions, pipelined/nested execution, cancellation and disconnect return
  resource ownership exactly once without preventing checkpoint drain;
- a hot-key benchmark compares retry-only OCC to adaptive waiting and reports
  throughput plus p99 latency;
- batch scans use a resumable cursor and bounded memory, and the storage/execution boundary can produce useful multi-row batches/selection vectors without mandatory per-row allocation or dispatch;
- Describe/binding performs zero user query execution and cannot infer protected
  data; restricted roles can enter/end transaction blocks without DDL privileges;
- failed relational operations cannot commit partial row/index/catalog deltas;
  explicit blocks enforce the same failure lifecycle for execution and result
  admission, while post-decision response errors preserve committed outcome;
- current-grant checks prevent prepared-plan authorization reuse; revocation alone
  does not cancel authorized work or trigger commit reauthorization, while a later
  authorization error makes an explicit block rollback-only;
- immediate/deferred constraint timing and conversion agree with the declared
  phase, while certification protects unique/FK/catalog predicates;
- old transaction/catalog views remain consistent across concurrent schema
  changes, including binding, access-path selection, identity and decoding; old
  writers cannot bypass newly added constraints or authoritative participants;
- vector/batch execution reports allocation, compiler/vectorization evidence, cache/CPU profiles, and where relevant memory-bandwidth/TLB behavior;
- deterministic-simulation tests cover representative transaction/GC/I/O
  interleavings.

## Research and system inputs

- Turso — database-specific VM/IR, async I/O, embedded/server operation, MVCC;
- pgrust — multithreaded runtime, vectorized fused execution, low-latency JIT,
  cache-aware structures, query scheduling;
- ScyllaDB/Seastar — shard-per-core locality and explicit resource scheduling;
- TigerBeetle — bounded resources, direct async I/O, deterministic state-machine
  discipline;
- FoundationDB — OCC/conflict tracking and minimal transactional KV layering;
- TiKV — pessimistic/optimistic transaction lessons over distributed KV;
- LeanStore/Umbra — cache-local synchronization and modern hardware execution;
- Turbopuffer — production evidence that per-item Rust abstraction can block vectorization and that batched kernels can materially change search latency;
- DuckDB — separate I/O/compute scheduling and memory-governed read-ahead, especially for remote/cold reads.

## Consequences

- The current pgwire server remains a correctness foundation, not the final
  scheduler.
- `spawn_blocking` and the outer relational RwLock are explicitly transitional.
- PostgreSQL compatibility stays at semantic/wire boundaries without forcing
  PostgreSQL's process or executor architecture.
- Hot-row workloads can eventually make progress without pathological retry
  storms while the uncontended path stays optimistic.
- Execution can evolve from interpretation to vectorization/JIT without
  replacing parser/binder/catalog semantics.
