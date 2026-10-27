# OmenDB relational roadmap

**Sole maintained OmenDB product/integration sequence, 2026-10-25.**
This document owns the native SQL/server and relational product gates.
The separately maintained [SeerDB kernel roadmap](../crates/seerdb/docs/roadmap.md)
owns its transaction, buffer, page, WAL, history, checkpoint and reusable KV
implementation. [Architecture](architecture.md) and
[relational design](design/relational-engine.md) specify target behavior;
[specialized engines](design/extensions.md) and
[distribution](design/distribution.md) are later capabilities.
Old [ADRs](adr/README.md) and [vNext plan](plans/storage-kernel-vnext.md)
are historical, not competing backlogs.

The last code audit underlying these targets was performed against
`1590011` (2026-10-13). Current repository documentation is newer than
the implementation. This plan does not assert a passing integration, release
or performance gate.

## O1. Repair present SQL integrity and authorization

In the *current maintained serving path*, close **exposed authorization,
partial-commit and data integrity defects**, independently of SeerDB's
eventual physical replacement. Preserve independent regression oracles
and protect existing users, but do **not** spend an extended refactor
on legacy binder/page/cursor abstractions slated for replacement.
Prototype the clean SQL semantics and transactional row/index boundary
alongside SeerDB S1 rather than sequentially polishing the old engine. Use one
non-executing binder for parameter/result types and resolved catalog views;
enforce current object/principal-incarnation grants on Describe as well as Execute.
Rename/name reuse cannot transfer access, and missing grant state cannot reopen
bootstrap privileges. Freeze prepared parameter/result signatures across replanning;
reject incompatible rebind before execution. Every statement stages a
private atomic row/index/catalog delta; errors in explicit transactions
follow a coherent rollback-only lifecycle. FK creation/timing,
constraint-index column mappings and timing, schema publication, cursor continuation,
parameter-batch shape, parser-driven wire statement-list transaction boundaries,
and dump/restore behavior must have exact semantics. Repair float index equality
with an explicit legacy-index rebuild/refusal policy; normalization must not
silently make existing keys unreachable or choose among duplicate logical keys.

**Gate:** independent restricted-role, PostgreSQL-overlap and native SQL
oracles reproduce protected outcomes on real direct and wire entry points.
No partial committable row/index changes, unauthorized Describe execution,
rename/recreate grant transfer, prepared signature drift, wrongly paired composite
FKs, equal values admitted by a unique index, catalog mixed-generation reads or
duplicated scan boundaries. Do not
treat incorrect legacy output as the reference contract.

## O2. Define and integrate native relational access

Implement **OmenDB-owned** schema-versioned compact rows, stable row
identity for both primary-key and heap-style SQL tables, relational
secondary/unique/FK index codecs and maintenance,
transactional catalog/layout snapshots and an access path directly using
SeerDB's compiled object/record/ordered primitives. No mandatory
serialize-all-rows-into-generic-KV round trip. A real SQL transaction must
update canonical row, secondary/constraint indexes and catalog under
**one SeerDB decision**; application rollback and DDL dependencies use
the same owner.

Begin an **integrated relational vertical slice in parallel with SeerDB S1**:
one schema, one PK + one unique secondary index, multi-row updates,
delete, rollback, concurrent writers and close/reopen through the public
native API. Initially rely on complete-log replay, then use S2/S3 to
qualify full serializable predicates, older snapshots, true checkpoint
and history retention. Prototype OmenDB row codecs and access boundaries
without waiting for every optional KV optimization. No release gate
closes until [SeerDB stages S1–S3](../crates/seerdb/docs/roadmap.md)
and OmenDB semantic/security gates agree on one real runtime.
Do not create permanent adapters for multiple old/new engines merely
to preserve a developer-preview interface. Preserve useful independent
SQL semantic and crash probes through migration.

**Gate:** transactionally correct inserts, updates, deletes, constraint
violations and schema changes work on the recovered replacement runtime.
Long readers and older catalog versions retain their declared view.
Offline preview data conversion preserves supported logical semantics
or refuses explicitly.

## Physical-layout challenger before format stabilization

Compare, on that same SeerDB transaction engine and on equivalent schema
and consistency, PK-clustered rows + secondary PK references, stable RowId/
heap-like base rows + secondary RowId references, and a transactional
ordered-KV facade versus direct typed row access. Quantify write/index/
history amplification, PK changes, point/range and covering-index reads,
cold payload projection, concurrent DDL and p99. Keep one selected physical
path in production once measured; do not build an abstract backend matrix
or rely on an incumbent's benchmark as OmenDB's evidence.

## O3. Replace the executor and remove global contention

One parser → non-executing binder → typed IR/planner → specialized OLTP
micro-plan or selection-vector/typed-batch pipeline. Share NULL, collation,
coercion, privilege, statement error and parameter semantics. Replace
`Vec<Vec<Value>>`/row-by-row hot paths with borrowed typed record
views, then preserve bounded typed batches and selection masks end-to-end
through filter, join, aggregate and projection, with late materialization.
Compare fused scalar and batch paths at realistic selectivity, width and
cache states; prove SQL NULL/collation/snapshot equivalence and measure
copy/decode bytes, allocations, p99 and batch-size crossover.
Reserve/account sort, hash-join, aggregate and result memory as it grows; spill
within bounded storage or refuse, rather than applying only a post-materialization
payload check. Bound scan work including invisible versions and I/O. Cursor yields must
not retain locks, repeat OFFSET prefix scans or duplicate boundaries.

Replace `Arc<RwLock<RelationalDatabase>>` with a concurrent database
handle, versioned catalog and explicit transaction admission. Short OLTP
and maintenance-critical commit/reclaim work retain protected worker/IO
capacity under scans. Support cancellation, timeout, disconnect and
ambiguous durable decisions without data loss or unsafe automatic retries. Retain
client retry identity and explicit committed/refused/unresolved resolution through
reconnect and the declared outcome horizon; session names are not durable tokens.
Account for idle transaction/portal leases and coordinate query/storage budgets
without queueing a resource-holder behind work that needs those same resources.
The native SQL server is primary; optional PostgreSQL-wire adapters share
the same engine without determining its semantics.

**Gate:** independent clients execute real concurrent SQL with no
database-wide execution/write serialization, starvation, memory-growth
or unbounded long-scan work. Explain plans correspond to physical
execution; row/index/constraint effects and snapshot isolation remain
correct at every yield and commit boundary.

## O4. Choose product defaults and qualify a server release

Compare OmenDB against PostgreSQL/MySQL at overlapping indexed SQL and
durability/isolation semantics, and SQLite for relevant embedded
trade-offs. Use device-backed Linux NVMe, concurrency 1/4/16+, mixed
hot and cold rows, larger-than-memory scans, long snapshots,
schema/index churn and ongoing checkpoint/GC. Measure p50/p95/p99,
throughput, aborts/retries, allocation/CPU/RSS, write amplification,
checkpoint pauses, recovery time, error/operational behavior and
real client latency. Declare budgets before measurement, not after.

SERIALIZABLE with complete dependencies is the reference correctness
isolation, but the default must be selected from measured retry
ergonomics versus READ COMMITTED. Do not describe SNAPSHOT/REPEATABLE
READ as automatically serializable. Decide SQL dialect, driver/protocol
and ergonomic priorities based on supported native use, not full
PostgreSQL extension or system-catalog compatibility.

**Gate:** [alpha-release criteria](alpha-release-gates.md) met at the
actual code revision, including failures/recovery, auth, bounded
resource use, usability and a defined performance bar. Never equate
a legacy-engine microbenchmark, a documentation change or an optional
Pgwire differential with native product qualification.

## O5. Extend the engine and prepare distribution deliberately

After the relational serving path earns its reliability and cost,
integrate text/BM25, vector/ANN and selected analytical representations
where transactional snapshot correctness and a measured query workload
justify them. Keep independent Vector/OLAP products independently
deployable for separately ingested data; share algorithms/operators
with one maintained source owner, not a second WAL inside SQL.
Cross-engine projections require atomic snapshot/CDC leases, complete
transaction change application, precise visibility/freshness and GC.

Coordinate later SeerDB S5 secondary object storage and regional HA
with OmenDB's [distribution plan](design/distribution.md): one-node
fast path first, then fenced replicated ranges, placement groups,
online migration, query routing and eventually explicitly correct
cross-range transactions. Neki-class scale is a multi-node workload
goal, **not** a single-node throughput claim.

**Gate:** no tax for unused extensions, exact/declared approximate
and stale behavior, bounded rebuild and maintenance debt, safe failover
and online movement with topology fencing. A design document does
not authorize a new release, private product merge or public source
publication.

## Current blockers and information ownership

The [SQL gap register](gap-register.md) tracks implemented product
defects. SeerDB's [kernel roadmap](../crates/seerdb/docs/roadmap.md)
owns WAL, history, buffer, page feasibility, checkpoint and recovery
gates. The historical [benchmarks](benchmarks.md), [compatibility
matrix](pgwire-compatibility.md) and [research/ADRs](adr/README.md)
remain bounded evidence rather than a second release checklist.
Reuse or delete legacy adapters, tests and formats only after
the appropriate independent regression protections survive cutover.

