# OmenDB: native relational engine

**Maintained target, 2026-10-25.** OmenDB is intended to replace common
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

- Bound statements have parameter/result types, typed coercions, effects,
  resolved IDs, schema/namespace/constraint dependencies and privileges.
  `Describe` must neither execute a user query nor infer types from observed
  data. Cached plans are never cached authorization grants.
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

Where semantics allow, compare compute-on-encoded blocks (constant, dictionary,
RLE, bitpacked and other measured representations) to full decoding. Some
equality or grouping work may operate on encoded values; SQL NULL, collation,
cast, overflow, comparison, row ordering and retained-snapshot contracts
still govern results. Physical codecs belong to the access method or
analytical representation, not to generic SeerDB query semantics. Short OLTP
point/update micro-plans remain a distinct low-overhead path; do not impose
a fixed batch allocation for a single key.

Use the existing typed IR rather than inventing a second vectorized executor.
Compare scalar/fused and vectorized pipelines, existing Arrow/DataFusion
capabilities, batch sizes, record width, predicate selectivity, cache
residency, allocations/bytes decoded, branches, CPU and p99 under concurrent
transactions. The [Turbopuffer October 26, 2026 design](https://turbopuffer.com/blog/fully-vectorized-search-engine)
motivates an experiment, not an assumed OmenDB performance result.

## Row, index and catalog representation

Use schema-versioned compact rows (NULL bitmap, fixed typed fields, compact
variable-offset directory, inline payload and logical cold/large-value refs).
Borrow typed page-resident fields for short queries; only selected fields
are decoded/copied into output batches. Do not allocate generic `Vec<Value>`
for every stored row or duplicate primary-key identity without demonstrated
benefit. Clustered primary-key rows are the initial physical baseline, not a
settled universal SQL row layout. Independently test **primary-key-keyed
clustered rows** against a **stable internal RowId plus heap/row table**
and an optional hot-row/cold-column split. A table without an explicit
primary key still needs durable logical row identity. If secondary
indexes reference an immutable RowId, a PK update may avoid rewriting
unrelated index keys but adds a base-row lookup and storage overhead;
if secondary entries reference the PK directly, most reads save that
indirection but PK changes, wide keys and repeated key copies cost
more. Physical page/slot identity is never a permanent logical RowId.

Keep schema-aware record access and generic transaction/KV access
**equally reachable** from one SeerDB runtime until a real comparison.
[FoundationDB Record Layer](https://foundationdb.github.io/fdb-record-layer/index.html)
demonstrates native-style indexes and SQL above transactional KV, while
[InnoDB's clustered rows](https://dev.mysql.com/doc/refman/26.10/en/innodb-row-format.html)
are a production physical baseline. Direct typed access only earns its
complexity if it measurably reduces CPU/allocations, decode/copying,
index maintenance or I/O in **equal SQL semantics**. No permanent pair
of storage backends or generic table-specific code inside SeerDB.

Primary/unique/required foreign-key indexes and catalog mutations are
**authoritative** and share the same SeerDB commit. Secondary entries carry
logical row identity and optional covering payload. Physical locator hints
need generation/reuse validation and logical fallback. Unique/FK checks must
be selective indexed probes, not whole-table rescans.

Catalog, schema version, layout and enforcement membership are versioned
objects. Transactions and plans hold consistent views and dependencies,
including names *not found*, range gaps and newly installed constraints.
DDL validation and publication form a **single schema transaction**, not
validate-in-one/commit-in-another. Online schema rewrite can later use
retained snapshot plus complete transaction change catch-up.

## Row and index workload used to select the physical layout

The initial experiment includes narrow/large/wide variable-length rows,
composite and changing PKs, an implicit RowId table, 0/1/4 secondary
indexes, composite unique/FK checks, covered versus uncovered queries,
uniform/skewed insert/update/delete, range and point reads, joins, and
mixed writers. Compare database size/RSS, index amplification per
mutation, bytes read per projected field, snapshot-version growth,
page split rates, cold reads and p99. Verify each case against
independent SQL results and identical isolation/durability. OmenDB
owns row/index codecs; SeerDB owns the transaction, page and index
primitives. Keep the simplest representation that meets the workload
bar; optional physical structures can be introduced later without
violating the logical row/catalog contract.

## Transaction and SQL session behavior

The SeerDB kernel owns commit/isolation and failure authority. OmenDB owns
statement semantics and exact effects. One statement builds a private atomic
row/index/catalog/cascade delta; failed statements cannot leave committable
partial index or base-row changes. Explicit transactions obey a declared
rollback-only policy after bind/auth/execute/result-admission failures;
post-durable delivery failure cannot fabricate an abort.

SERIALIZABLE is the reference correctness mode but **not a locked product
default**. READ COMMITTED must use statement snapshots and wait/recheck
correctly; mixed isolation and constraint enforcement need independent
testing. Automatic retries of closures are safe only if external side effects
are controlled; arbitrary multi-statement network sessions cannot be
replayed by the server. Make retryability, ambiguous outcomes and
transaction IDs accessible to clients.

Each session owns protocol state, not a dedicated OS thread. A cloneable
database handle replaces the current outer `Arc<RwLock<RelationalDatabase>>`;
synchronization belongs to transactions, catalog objects and page guards.
Bound workers and resource admission protect commit/WAL/GC progress while
enabling parallel independent reads and writes. Cancellation/deadlines
never release an unresolved decided transaction.

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

