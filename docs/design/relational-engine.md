# OmenDB: native relational engine

**Maintained target, 2026-10-27.** OmenDB is intended to replace common
PostgreSQL/MySQL workloads, not implement their process model or guarantee
complete protocol/dialect compatibility. [Architecture](../architecture.md);
[SeerDB transaction kernel](../../crates/seerdb/docs/architecture.md);
[SeerDB roadmap](../../crates/seerdb/docs/roadmap.md) and
[OmenDB roadmap](../roadmap.md).

## Scope and correctness

Server-first, concurrent SQL database with typed tables and expressions,
multi-row transactions, primary/secondary indexes, unique/FK/check constraints,
schema evolution, authorization, query plans, backup and operations. Start from
OLTP but ensure selective scans, joins, aggregates and maintained views are not
architecturally excluded. Embedded Rust clients use the same core and opt into
a small/lazy serverless runtime.

Native SQL/type/catalog semantics are authoritative; a PostgreSQL protocol
adapter is an optional bounded interoperability layer. Reuse mature parser,
wire, type/encoding and optimizer/operator libraries when compatible; do not
copy PostgreSQL's storage engine, extension ABI, process-per-connection,
catalog OIDs or source-text heuristics into the native engine.

## Relational source ownership

SQL schemas, `TableId`/`ColumnId` and SQL `Value`, nullability and
coercions, relational primary/secondary index key layouts, row codecs,
constraint obligations, catalog mutations, authorization, the query
scheduler and all client/server protocol adapters are **OmenDB concerns**.
OmenDB owns its integration adapter into SeerDB's generic ordered or
record-access mechanisms. The current `src/seer_direct.rs` stays here
during migration; a replacement should eliminate obsolete adapters,
not push SQL semantics into the reusable storage crate.

SeerDB supplies versioned storage objects, buffered ordered/index and
extent primitives, atomic transactions, generic dependency registration,
snapshot/change streams, WAL/recovery and storage resource accounting.
OmenDB chooses which objects participate and registers SQL-specific
dependencies and constraints. `crates/durable-fs` retains filesystem
durability helpers. Do not introduce a SeerDB dependency on this crate.

## Binder, plan and execution

One path: SQL parser → **non-executing typed binder** → retained catalog view
and dependency set → logical plan → costed physical plan → micro-plan or
typed vectorized pipeline → cursor/row/index interfaces.

Build the smallest shared bound representation with the first relational slice,
not a second executor after storage cutover. It carries resolved objects, typed
expressions/coercions, parameter/result signatures, privileges and effects;
Describe, EXPLAIN and execution consume that same binding. Current
`src/sql_query.rs::{PredicatePlan,ProjectionPlan}` and SQL AST evaluation are
transition machinery, not an existing general typed IR. Reuse their valid scalar
semantics and tests, not their scattered binding authority.

- Bound statements have parameter/result types, typed coercions, effects,
  resolved IDs, schema/namespace/constraint dependencies and privileges.
  `Describe` must neither execute a user query nor infer types from observed
  data. Cached plans are never cached authorization grants. Every executable
  object reference contributes privileges; unsupported/unclassified AST shapes
  fail closed rather than disappearing from a separate name collector.
  Transaction/session controls are binder effects, not schema administration
  inferred from SQL text.
- Optimizer includes cardinalities, predicate/index statistics, covering
  indexes, storage locality, scan/decode/aggregation costs, and later
  snapshot-qualified derived paths; compare plans through EXPLAIN and
  representative regression workloads. Don't build a large custom JIT before
  a stable typed IR and measured bottleneck.
- Point lookups/updates use allocation-light **micro-plans**. Scans/joins/
  aggregates use typed batches and selection vectors, selective decode and
  bounded morsel work. Both share NULL/three-valued logic, collation,
  casting, expressions and error semantics.
- Resumable cursors preserve snapshot, catalog and private overlay identity.
  An exclusive last-key continuation may redescend; do not rescan prefixes
  through OFFSET, duplicate boundary rows or hold page latches across
  yield/network suspension. Budgets cover invisible slots, version walks,
  overflow, I/O and output bytes as well as visible row counts.

### Prepared statements, portals and authorization

Freeze parameter types at preparation and the externally advertised result
signature (field count, names, types and encoding-relevant modifiers). Parameter
values may specialize physical plans, never redefine that signature. Replanning
or semantic rebinding must preserve it or fail before execution and require
reprepare. A portal retains its bound parameter interpretation, catalog/layout,
snapshot and private-overlay view for one execution; it cannot rebind midway
through yielded output. A cached statement does not pin a live data snapshot
forever. Bound portal state and its leases are accounted for and released on
completion, close, error or session loss. Validate parameter counts and formats
before decoding/execution, never zip away missing inputs or guess a fallback
parameter type. Dependency invalidation uses catalog identities/versions across
all DDL entry points, not token-prefix cache clearing.

Native roles and grants are versioned transactional catalog state; adapter-local
files and caches are not a second grant authority. Authentication can map an
external identity to a native principal without transferring ownership of grants.
Ordinary grants bind principal and object incarnations, not reusable names.
Resolve names when granting; rename preserves the object's grants, while
drop/recreate does not transfer them. Database/schema-wide administrative
capabilities are explicit, not an accidental match against a table named `*`.
Rebinding to another object always requires authorization of the new identity.
The optional wire adapter maps these native capabilities rather than owning a
second permission model. Missing/malformed authorization state fails closed;
bootstrap uses an explicit privileged capability, never the absence of grant rows.

Check **current committed grants** on the resolved objects before Describe
metadata or execution is exposed, and retain that authorization view for the
operation. Execution uses those same resolved identities, with no intervening
name re-resolution. The authorization view is independent of an older data/catalog
snapshot. Revocation alone does not cancel already-authorized work or reauthorize
commit; later execution
boundaries, including resumed client Execute/fetch requests, check again.
A later authorization error makes an explicit transaction rollback-only, including
prior staged writes. Rollback/cleanup remains available without DDL permission.
Trusted direct callers may hold an explicit privileged capability. Syntax/binding
errors must not become a route to executing protected expressions or reading data.

## Batch execution and representation-aware access

For scans, joins, aggregates, ranking and selective projections, operators
should exchange **bounded typed batches**, not one owned SQL row per call.
Carry logical row IDs/positions, schema and snapshot identity, NULL validity
and a selection vector/bitmap through the pipeline. Apply predicates and
eligibility before decoding wide or unrequested columns; fetch them only for
surviving rows or final top-k output. Native index scans should yield bounded
candidate batches with exclusive logical continuations instead of an ever-
growing OFFSET. Retain or copy values before page guards expire; never hold
a latch across executor/network suspension.

Reserve sort, hash-join, aggregate, candidate and result memory before growth;
spill through accounted bounded storage or refuse before exceeding admission.
Apply row/byte/work limits while producing batches, not after constructing an
unbounded `Vec` of results. Backpressure and suspended consumers retain accounted
leases, not page latches. Mid-stream failure has an explicit error/completion
boundary and the same rollback-only transaction policy; an earlier partial result
is not a successful statement or commit.

Where semantics allow, compare compute-on-encoded blocks (constant, dictionary,
RLE, bitpacked and other measured representations) to full decoding. Some
equality or grouping work may operate on encoded values; SQL NULL, collation,
cast, overflow, comparison, row ordering and retained-snapshot contracts
still govern results. Physical codecs belong to the access method or
analytical representation, not to generic SeerDB query semantics. Short OLTP
point/update micro-plans remain a distinct low-overhead path; do not impose
a fixed batch allocation for a single key.

Initially interpret compact micro-plans from the shared bound representation;
add batch lowering without a second type/error/privilege system. Evaluate reusable
[DataFusion planning components](https://datafusion.apache.org/library-user-guide/building-logical-plans.html)
and Arrow/operator libraries before writing replacements, but retain OmenDB's
transaction/catalog authority and qualify any adapter's semantic and conversion
costs. No library choice or JIT is a prerequisite for the first slice.
[Adaptive query execution](https://db.in.tum.de/~leis/papers/adaptiveexecution.pdf)
illustrates why compilation startup belongs in total latency, not outside it.

Compare scalar/fused and typed-batch paths on identical bound queries, including
planning, allocation, conversion and concurrent-writer cost. Borrow encoded forms
where useful: [DuckDB's vectors](https://duckdb.org/docs/2.0/internals/vector.html)
show flat/constant/dictionary representations without making its batch size or
execution model an OmenDB requirement. The [Turbopuffer design](https://turbopuffer.com/blog/fully-vectorized-search-engine)
is another experiment source, not an assumed OmenDB speedup.

## Row, index and catalog representation

**Initial reference, not a frozen format:** compact PK-clustered records,
secondary entries carrying the PK, and C0 commitment with the kernel's
[seekable history reference](../../crates/seerdb/docs/architecture.md#mvcc-history-and-checkpoint-economics).
This gives a direct base lookup and PK-range locality without a separate PK-to-row
mapping. Accept and measure the cost of copying wide PKs into indexes and rewriting
secondary references on PK changes. Tables without a PK use a runtime-allocated
immutable RowId as the clustering key. Physical page/slot identity is never RowId.

A PK change is an atomic old-key delete/new-key insert with index maintenance;
both keys retain their required snapshot history. It does not inherently break
MVCC or require a permanent RowId. A PK lookup key is not an immutable row handle.
The first layout challenger gives every row a stable RowId, makes the declared PK
a unique secondary index, and carries RowIds in other indexes, while keeping the
same commit/history services. It trades an extra non-covering PK probe and poorer
PK-range base locality for smaller references and less PK-change maintenance.
Neither layout wins by definition; do not hide its lookup cost by duplicating the
whole row into a covering PK index.

Use an OmenDB-owned schema-versioned row codec (NULL bitmap, typed fixed fields,
variable offsets and kernel-managed overflow for oversized payloads) over concrete
SeerDB ordered-record operations first. Owned KV results and guarded payload views
are access surfaces over that same representation, not two storage engines.
Measure borrowed/selected-field access against the owned-result control before
requiring a richer storage interface; SQL types never move into SeerDB. Avoid
per-row `Vec<Value>` construction and repeated key copies where the measured path
can eliminate them. Specialized cold-column placement and bounded update fragments
are later candidates, not prerequisites for the first row codec.

[FoundationDB Record Layer](https://foundationdb.github.io/fdb-record-layer/index.html)
and [DocDB packed rows](https://docs.yugabyte.com/stable/architecture/docdb/packed-rows/)
show that KV storage need not imply one allocation/record per SQL column or an
inefficient whole-engine boundary. DocDB's sparse-update fragments also introduce
read reconstruction and compaction costs. Compare those costs before adopting a
second representation. [InnoDB](https://dev.mysql.com/doc/refman/8.4/en/innodb-row-format.html)
is a PK-clustered reference, not evidence it beats RowId for our workload.

Primary/unique/required foreign-key indexes and catalog mutations are
**authoritative** and share the same SeerDB commit. Secondary entries carry
logical row identity and optional covering payload. Physical locator hints
need generation/reuse validation and logical fallback. Unique/FK checks must
be selective indexed probes, not whole-table rescans. Each FK has a parent-side
supporting unique index and a maintained child-side lookup on its FK columns;
compatible existing indexes may be shared with explicit lifecycle dependencies.
Dropping a required index either atomically replaces/rebinds its enforcement role
or refuses before catalog/physical mutation.
Bind a foreign key to its supporting unique index and explicit bijective ordered
child↔parent column mapping. The index may order the referenced column set
differently: translate probes into index order without changing the declared pairs. Use that same mapping
for creation validation, DML checks and referential actions. Comparison/equality,
index ordering and constraint matching have one bound type/collation authority,
not independent raw `Value` equality and byte serialization. Equal logical keys
(such as numerically equal decimals with different scales or float signed zeros)
need consistent index equality encoding. Declare supported NaN/collation behavior
and normalize/coerce before encoding, or refuse unsupported indexed operations.
Ordered keys preserve declared typed order, per-column direction, collation and
NULL placement, with unambiguous field boundaries and prefix/range bounds; raw
record serialization is not an ordered-key codec.
Changing canonical encoding requires explicit persisted-index rebuild/conversion
or refusal; never silently reinterpret existing keys. Conversion refuses conflicting
logical duplicates instead of discarding rows. Unique keys use NULLs-distinct semantics unless an explicit alternative is
implemented; MATCH SIMPLE FKs exempt a tuple containing NULL. Index coverage and
NULL lookup must still preserve exact query results through a supported path.

Catalog, schema version, layout and enforcement membership are versioned
objects. Transactions and plans hold consistent views and dependencies,
including names *not found*, range gaps and newly installed constraints.
DDL validation and publication form a **single schema transaction**, not
validate-in-one/commit-in-another. Retain namespace absence and each table's
index/constraint enforcement membership so an old writer cannot omit newly
activated maintenance. Retired objects/layouts remain decodable for admitted
readers and recovery. A rewrite/build becomes usable only after retained-snapshot
copy, complete change catch-up, final dependency validation and atomic catalog
publication; a scan followed by an unprotected metadata update is insufficient.

## Row and index workload used to select the physical layout

Use targeted cases, not a Cartesian product of every layout/commit/history option:
narrow stable-PK point reads/updates; wide or changing PKs with several indexes;
and non-covering PK/secondary ranges. Include a no-PK table and unique/FK checks.
Run warm and constrained-cache cases, then repeat the discriminating cases with
long snapshots and checkpoint/GC progress. Sparse wide-row updates trigger the
full-image-versus-fragment comparison only if amplification warrants it.

Measure complete transaction/query latency including retries, probes/base fetches,
CPU/allocations/RSS, and total WAL/index/history/page-map/checkpoint/GC bytes and debt.
Batched RowId fetches must restore required SQL ordering and account for buffering;
covering payload is charged as storage and update work. Match correctness,
durability, resource limits and offered load; run long enough to include recurring
maintenance. The [OmenDB roadmap](../roadmap.md#physical-layout-challenger-before-format-stabilization)
owns experiment order and selection. No paper result, arbitrary percentage threshold
or faster isolated lookup freezes the production layout.

## Transaction and SQL session behavior

The SeerDB kernel owns commit/isolation and failure authority. OmenDB owns
statement semantics and exact effects. One statement builds a private atomic
row/index/catalog/cascade delta; failed statements cannot leave committable
partial index or base-row changes. Freeze DML input at the statement's admitted
view, including earlier successful statements but excluding its own new delta;
updates/cascades cannot repeatedly consume newly written rows. Keep dependencies
from earlier statements through transaction completion. Validate supported
nondeferrable row/unique obligations at their mutation boundary, immediate FKs
before statement success, and explicitly deferred obligations before decision.
Constraint enforcement reads the resulting staged view at its declared check
boundary, not merely the frozen DML input.
Cascade/action semantics retain the declared column mapping and use an
admission-bounded worklist with visited/effect ownership; recursion depth alone is
not a work/memory bound. CHECK expressions are typed, immutable and row-local in
the first implementation: TRUE or UNKNOWN passes, FALSE violates the constraint,
and evaluation errors fail the statement ([independent SQL reference](https://www.postgresql.org/docs/18/ddl-constraints.html#DDL-CONSTRAINTS-CHECK-CONSTRAINTS)).
Adding a constraint validates and publishes in one schema transaction. Any later
NOT VALID/building state must be explicit in the catalog and enforce the declared
policy on new writes.
Private deltas are not implicit savepoints. Explicit SAVEPOINT/ROLLBACK TO/RELEASE
is a separate lifecycle feature; reject it honestly until its effects, dependencies
and resources have qualified rollback semantics.

Any bind/auth/execute/cancel or pre-decision result-admission error in an explicit
block makes the transaction rollback-only. Direct and wire APIs share that state;
COMMIT cannot publish earlier effects from a failed block. Internal schema
transactions do not silently add DDL-in-block support: until qualified, reject
unsupported combinations before mutation. Once decision append may have occurred,
completion belongs to SeerDB; post-decision encoding/delivery failure cannot
fabricate an abort. DML RETURNING/output admission that may refuse the operation
must finish before decision, and streaming output must not advertise commit success
before the kernel's visible completion.

Native batch APIs declare their atomic unit and validate all statement/parameter
shapes before work. The optional PostgreSQL adapter parses complete statement
lists, never splits on semicolons inside strings/comments. A simple Query message
uses [PostgreSQL's implicit transaction boundary](https://www.postgresql.org/docs/18/protocol-flow.html#PROTOCOL-FLOW-MULTI-STATEMENT)
for its statement list, except where explicit transaction controls establish
boundaries; it must not silently autocommit every fragment. Extended Execute/portal suspension preserves the same
transaction and failure owner, rather than becoming an independent commit.

SERIALIZABLE is the reference correctness mode but **not a locked product
default**. READ COMMITTED must use statement snapshots and wait/recheck
correctly; mixed isolation and constraint enforcement need independent
testing. Automatic retries of closures are safe only if external side effects
are controlled; arbitrary multi-statement network sessions cannot be
replayed by the server. Make retryability, ambiguous outcomes and
transaction IDs accessible to clients. Retry tokens and outcome resolution follow
[the kernel's durable outcome contract](../../crates/seerdb/docs/architecture.md#identity-transaction-ownership-and-retry-outcomes);
only explicitly retry-safe work may be replayed. Do not call a negative lookup
proof of abort while an earlier attempt can still append. Native request identity
must survive reconnect and remain distinct from a session or prepared-statement name.

Each session owns protocol state, not a dedicated OS thread. A cloneable
database handle replaces the current outer `Arc<RwLock<RelationalDatabase>>`;
synchronization belongs to transactions, catalog objects and page guards.
Bound workers and resource admission protect commit/WAL/GC progress while
enabling parallel independent reads and writes. Cancellation/deadlines
never release an unresolved decided transaction. OmenDB owns the aggregate process
budget for execution, results, sessions and caches; SeerDB accounts for its storage
share and protected progress resources through explicit grants, without depending
on SQL scheduling policy. Bound idle transaction/portal retention as well as active
workers. Do not wait for a new workload reservation while holding resources needed
by the work ahead in that queue. Cooperative cancellation is not a hard kill/RSS
guarantee for arbitrary blocking code; use isolation where that guarantee is required.

## Server operation and path to parity

A general-purpose database needs mature behavior beyond SQL throughput:
concurrent clients and connection admission, auth/TLS deployment,
observable query/transaction latency and errors, EXPLAIN/statistics,
backup/restore and CDC, safe startup/recovery, checks/repair boundaries,
resource/tenant isolation and online maintenance. Do not call the preview
production-ready while known auth or failed-statement integrity gaps persist.

First target a trustworthy SQL server and Rust API with clear supported
syntax, migrations and transactional semantics, not instant feature parity
with every PostgreSQL/MySQL extension. Against PostgreSQL/MySQL, test
the same isolation, durability, schemas, FK/unique/DDL behavior, concurrent
writers and real SQL plans; compare throughput and p50/p95/p99, CPU/RSS,
index write amplification, larger-than-memory scans, checkpoint pauses and
recovery. PostgreSQL protocol tests remain valuable **only** for explicitly
advertised overlap.

The current `src/sql*.rs` row-oriented result paths, execute-to-describe
logic, `src/morsel.rs` offset-rescans and Pgwire global relational RwLock
are transition code, not contracts to preserve. [Current gaps](../gap-register.md)
and [alpha gates](../alpha-release-gates.md) remain evidence; neither is
a second architectural owner.

