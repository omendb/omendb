# OmenDB and SeerDB implementation roadmap

**Sole maintained execution sequence, 2026-10-25.**
Targets live in [architecture](architecture.md) and [design contracts](design/).
The [ADR archive](adr/README.md) records historical rationale; private research
belongs in the owner's knowledge repository. This is a delivery sequence, not
a claim that any milestone passed. Source baseline for the last code audit:
`1590011` (2026-10-13); subsequent changes through 2026-10-25 are documentation.

## Delivery principles

- One server-first native relational database, one SeerDB transaction/log/catalog
  authority. OmenDB and the separately usable KV facade share the *same*
  runtime. PostgreSQL compatibility is optional; correctness and usability
  of declared native SQL are mandatory.
- A heavy refactor/rewrite is allowed. Reuse already proven logic, not obsolete
  interfaces. Implement a **vertical end-to-end relational transaction**
  before adding speculative traits, second engines or isolated benchmarks.
- Keep the current generation-COW path as a **fallible** semantic/crash reference
  until qualified cutover. When new behavior is accepted, delete obsolete
  engines/adapters/tests/flags; no permanent vNext backend matrix.
- Qualify under real Linux NVMe and a documented durability class. Have
  independent semantic oracles, failure injection, process crashes, double
  reopens, subsequent mutations, held old snapshots and bounded resource
  conditions. Performance claims require workloads, hardware, isolation,
  sync, sample distributions and old/new raw results.

## Stages and release gates

### A. Transaction authority + relational correctness

**Build:** one persistent SeerDB runtime owning store incarnation, transaction
outcomes, certification, WAL, MVCC/history, buffer/page/extent services, resource
and lifecycle admission, checkpoint/recovery and GC. Eliminate persistent
optional-component assembly. A single transaction can change a typed base row,
a unique/secondary index and catalog state under one outcome.

**In parallel, fix serving integrity:** non-executing typed binder; auth on
Describe and Execute; statement-private atomic deltas; rollback-only explicit
blocks; coherent versioned catalog/constraint enforcement; exact FK timing;
cursor continuation; parameter batch validation and dump/restore parity.

**Gate:** real direct and network transactions, concurrent sessions,
rollbacks/constraints and failed/cancelled writes cannot expose partial
row/index/catalog effects. No query-global `RwLock<RelationalDatabase>`.
Independent oracles and restricted-role security checks pass.

### B. Commit, versions and index/page representations

**Build:** decision-ordered serializable dependency certification against
accepted/uninstalled writers; safe hot-key policy; complete recoverable WAL
record/history model with pre-decision admission; bounded actual fenced B-link
splits and logical overflow; schema-driven clustered rows and mandatory indexes;
bounded private overlays and snapshot-stable streaming cursors.

**Gate:** fixed snapshots and point/absence/range/constraint/catalog conflicts
are correct under pressure. Every admitted durable effect can install and
replay; large keys/records, deletes, long readers and indexes survive faults.
The old two-barrier undo protocol remains until complete-log recoverability
passes independent tests.

### C. Persistent checkpoint, streaming recovery and lifetime

**Build:** complete, validated checkpoint reference closure and retained WAL
suffix; exact snapshot/history/CDC/backup leases; bounded streaming recovery,
version/status cleanup, space reclamation, abandoned retry-space policy;
short page-image capture before async writeback, scalable page maps and
incremental/epoch checkpoint once size/pause evidence requires it.

**Gate:** crash after decision before page/history sync, during manifest/GC
and repeatedly during recovery; two independent reopens plus subsequent
mutations; no acknowledged loss, foreign-reference adoption or unbounded
memory/space leakage. Test under real checkpoint, large dataset, long readers
and ENOSPC. Preview conversion preserves supported *logical* state or refuses.

### D. Native SQL engine and production-shaped concurrency

**Build:** one typed binder/catalog/IR and optimizer, allocation-light OLTP
micro-plans plus selective typed batch pipeline, an indexed cursor with bounded
physical work and exclusive continuation, cloneable database handle and
bounded/lazy worker/admission pools. Current PG-wire adapter remains optional;
it does not own semantic typing or transaction policy.

**Gate:** direct/server workload with multiple writers, SQL joins/indexes,
DDL concurrency, cancellation, client disconnects, auth and statement failures;
no known production-path data integrity gap, cursor duplication or
offset-rescan. Open transactions have declared retry/ambiguous commit outcomes.
Choose default isolation using real contention/retry/p99 evidence,
not preference alone.

### E. Matched performance and first release

**Baseline:** same machines/isolation/durable ACK, schema, key/row sizes and
client concurrency: PostgreSQL/MySQL for SQL; SQLite for appropriate
embedded/useful crossover; RocksDB, Fjall and SurrealKV for comparable KV.
Profile own index/OCC/commit lane, WAL barriers, cache/translation,
LLC/TLB misses, row allocation, write/space amplification, GC/checkpoint
p99, recovery time and sustained history pressure. Include hot and cold,
uniform and skewed, 1/4/16+ writers, long snapshots and bigger-than-RAM data.

**Decisions after measurement:** safe optimistic page reads, BBTree/SIMD
layouts and batched probes, page size, clustered vs heap/PAX, copy/placement,
autonomous NVMe commit, speculative *invisible* installation, JIT,
io_uring/FDP/ZNS and LSM alternatives. Never assume they win from
isolated paper/microbench numbers.

**Gate:** choose budget thresholds *before* running; require matched
whole-product correctness, durability, tail latency and maintenance/recovery
results, not only a microbenchmark lead. Release readiness uses
[alpha gates](alpha-release-gates.md); no release/tag is authorized
by this roadmap.

### F. Specialized engines, storage tiers and distributed serving

**After core gates:** optional S3-compatible backup/restore, immutable
checkpoint/log archive and cold extents with full lifetime/recovery checks;
snapshot-qualified native text/vector/columnar engines and independent
ingestion projections with complete retained CDC; separately qualified
regional HA; then placement groups, routing, online movement, distributed
SQL and atomic cross-partition transactions.

**Gate:** each feature proves exact/declared approximate semantics, no
material tax when unused, isolation/freshness under updates, failure
containment, p99 and total cost. Neki-class scale means measured
shard-local growth, HA and zero-gap resharding at explicit topology and
durability, not replication of a 512-shard read-only QPS headline.

## Currently known blockers (source audit, not test rerun)

- `vnext` is **not** one restart-safe runtime; certification lacks full
  serializable point/gap/catalog coverage.
- Current page admission proves only an empty leaf, not future fully fenced
  split/internal/root feasibility. The current WAL batch has a fixed
  one-segment representability restriction.
- Undo is separately synchronized; current commit append still encodes
  within its sequencing mutex; the physical log/history replacement
  and checkpoint closure remain unqualified.
- Page writeback retains a read byte lock across I/O; history reads
  share a file mutex; WAL replay materializes records in memory;
  page-map duplicate detection is quadratic.
- Existing SQL Describe/auth/statement integrity, catalog/constraint,
  cursor and conversion issues remain registered in
  [gap register](gap-register.md).

The source repository owns implementation/release checks, the private
coordination tracker owns genuine cross-session task state if used, and
research/experiment measurements belong with their dated evidence.
No milestone closes by editing documentation.

