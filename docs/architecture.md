# OmenDB architecture

**Status:** accepted direction; implementation is incomplete and the current
alpha line is not a release of this architecture. Physical formats, scheduling
policies, commit mechanisms, analytical representations, and distribution
protocols remain replaceable until measurement plus recovery qualification
establishes them.

OmenDB is a **server-first PostgreSQL-class relational database written in
Rust**, optimized first for demanding OLTP while allowing real-time analytical
work on the same authoritative state. PostgreSQL compatibility belongs at
deliberate external boundaries (wire protocol, SQL behavior, drivers, and
tooling); OmenDB does not copy PostgreSQL's internal page layout, executor, WAL,
or process-per-connection model.

The direct Rust API remains a useful embedded and testing surface. It must use
the same transaction and storage semantics as the server, not become a second
database product.

## Product shape

```text
PostgreSQL clients / future native clients
                    |
             OmenDB server
                    |
       session, auth, protocol, SQL
                    |
      binder / optimizer / typed IR
                    |
     micro-plans + batch execution
                    |
       transaction and catalog layer
                    |
                 SeerDB
                    |
 logical MVCC + ordered buffered B-trees
                    |
 durable log + async physical materialization
                    |
 RAM / NVMe / analytical / archive tiers
```

The first serious deployment target remains a single excellent node with strong
operational tooling. Regional HA follows from the same log/snapshot contracts.
Distributed SQL is not a prerequisite for the first server architecture and
must not impose a permanent coordination tax on local mode.

OmenDB no longer assumes that useful OLAP requires a separate physical database.
The authoritative transaction/log state can feed in-engine compressed columnar
or hybrid hot-row/cold-column representations for zero/low-lag analytics. The
same atomic `snapshot CSN + restart LSN` contract can also bootstrap optional
remote analytical workers or a future scale-out `omen-olap` deployment when
workload isolation or analytical scale justifies it. Analytical physical layout
is therefore a policy/cost choice, not a second source of truth. See
[ADR 0011](adr/0011-htap-and-analytical-representations.md).

## Workspace and ownership

The repository is a Cargo workspace whose root package is OmenDB:

```text
omendb/
├── Cargo.toml       # root package and workspace
├── src/             # OmenDB server and relational engine
├── crates/
│   └── seerdb/      # independent generic storage crate
├── docs/
├── benchmarks/
└── tests/
```

OmenDB remains `AGPL-3.0-only`. SeerDB remains an independently versioned and
publishable `Apache-2.0` crate. The repository is the single writable source
for both projects; SeerDB's former standalone repository is not a second
implementation source.

The current development dependency is a workspace path dependency. Registry
releases remain independent: publish and qualify the Apache-2.0 `durable-fs`
crate from `crates/durable-fs`, then SeerDB, then OmenDB against that SeerDB
version. External consumers pin `durable-fs` to an OmenDB Git revision until a
registry release is available. Neither package version is inherited from the
workspace.

## OmenDB and SeerDB boundary

SeerDB is a generic OLTP-oriented transactional ordered-KV engine. Its logical
model is deliberately small:

```text
TreeId + unsigned-lexicographically ordered key bytes + opaque value bytes
```

SeerDB owns:

- ordered-tree access and resumable cursors;
- transactional tree lifecycle and atomic multi-tree mutation;
- MVCC visibility, transaction status, logical write intents/conflicts, and
  future adaptive contention waiting;
- page/frame residency, dirty tracking, buffer translation, blob/large-value
  storage, physical mappings, and physical garbage collection;
- durable log ordering, checkpoint/recovery, durability transports, and storage
  pressure;
- generic committed changes and snapshot/restart positions.

OmenDB owns:

- SQL and PostgreSQL-facing behavior;
- catalogs, schema, row codecs, NULL/type semantics, row-layout versions, and
  optional column-family placement;
- primary and secondary index meaning and covering payloads;
- constraints, DDL, optimizer, typed IR, and execution;
- relational CDC interpretation, analytical representation metadata, and future
  distribution/placement metadata.

OmenDB encodes relational keys and row-family records into SeerDB's opaque byte
boundary. SeerDB must not acquire SQL schema IDs, NULL bitmaps, column
directories, or index semantics. OmenDB must not bypass SeerDB's
transaction/MVCC machinery.

A generic storage-plugin matrix is not the product architecture. OmenDB calls
SeerDB through a capability-rich Rust API. Different deployment profiles may
change durability transport, cache tiering, or physical materialization, but
they do not become separate relational backends. The transaction/crash
invariants are recorded in
[ADR 0001](adr/0001-seerdb-transaction-contract.md).

## Transaction and durability identities

These identities are distinct:

```text
TxnId = transaction identity
CSN   = logical committed visibility order
LSN   = durable log position
```

The transaction API must provide:

- multi-writer MVCC transactions with explicit isolation semantics;
- point reads, conditional insert/put/delete, and ordered cursors;
- atomic writes across multiple trees;
- point and range dependency events for serializable certification;
- lightweight logical write-intent/wait primitives for contended keys while
  keeping the uncontended optimistic path cheap;
- short-lived borrowed record access with explicit page/frame-guard lifetimes;
- a durability result containing CSN and durable LSN;
- storage, retention, and recovery-pressure visibility.

A physical WAL/log is for durable recovery and physical/log replication. It is
not the public long-term CDC format. A generic committed change stream preserves
transaction boundaries and TreeId/key operations without exposing page-layout
details.

Snapshot export atomically returns:

```text
snapshot CSN X + restart LSN Y
```

A consumer copies snapshot X and then consumes committed changes after Y with
no gap. This is the contract for backups, CDC, analytical projection/bootstrap,
and future live range movement.

The durable commit **decision** is the visibility authority. Physical page or
analytical materialization is not allowed to create or revoke logical commit
state.

## Runtime and server direction

The final server owns logical sessions and a bounded execution runtime. A client
session is not an OS thread and is not a hidden reusable backend process.
Protocol/network work hands bound operations to fixed workers.

Short OLTP work uses stable home-worker affinity where possible for cache
locality. Long scans, joins, index builds, and maintenance produce bounded tasks
that may be stolen by idle workers. NUMA placement and future CXL-like memory
are runtime policies rather than SQL semantics.

Progress-critical work has protected capacity:

```text
durability / recovery / essential reclaim   guaranteed progress
foreground OLTP                              latency priority
scans / index builds / analytical work       elastic and preemptible
```

Unused reserve can be borrowed, but foreground or analytical load cannot
indefinitely starve WAL/log completion or reclamation required for continued
service. Admission evolves from one abstract scalar cost to concrete memory,
spill/storage, I/O, and CPU reservations.

The current pgwire `Arc<RwLock<RelationalDatabase>>`, 1 ms lock polling, and
Tokio `spawn_blocking` per operation are correctness scaffolding, not the target
runtime. See [ADR 0007](adr/0007-runtime-execution-and-contention.md).

## Planning and execution

Parsing, binding, authorization, Describe, planning, and execution share one
semantic representation. The binder produces a typed statement object carrying
parameter types, result schema, effects, catalog-object versions, and a logical
plan. Wire code must not infer DDL/RETURNING/row production from token prefixes,
and Describe must not execute a fabricated user query merely to discover its
type.

The relational engine shares one typed planner/type system across two physical
execution paths:

- **OLTP micro-plans** for prepared point/range lookups, constraint probes,
  simple DML, and short transactions. These use compact specialized operations,
  borrowed row views, and avoid generic `Vec<Row>`/`Vec<Value>` hot paths.
- **Batch pipelines** for scans, joins, aggregates, sorts, windows, index
  builds, and analytical queries. These consume resumable storage cursors and
  use typed vectors, selection representations, bounded work, and cancellation.

A database-specific typed IR permits several execution tiers: direct micro-plan
execution, vectorized pipelines, and optional very-low-overhead JIT for hot
plans. JIT is an optimization rather than a semantic boundary and must retain
efficient x86-64 and AArch64 paths.

## Relational row direction

The current tag-per-`Value` row codec is a bootstrap format. Stored rows become
schema-driven compact records with explicit row-layout versions:

```text
layout version + flags + null bitmap + offset directory + typed payload
```

Fixed-width values decode without allocation; variable-width data can be
borrowed from guarded storage. One compact row family is the default to minimize
KV/MVCC overhead. Wide/cold/large columns may be placed into additional families
or large-value segments where measurement shows a benefit. Batch execution
decodes only projected/predicate columns into typed vectors.

Primary-key values already encoded in the ordered key should not be duplicated
in every row value unless a deliberate covering/locality choice justifies it.
See [ADR 0010](adr/0010-row-layout-and-column-families.md).

## Local storage direction

SeerDB keeps ordered B-trees as the default local primary/index structure, but
**whole-tree/per-generation copy-on-write is not the long-term transaction
mechanism**.

The target local architecture is:

```text
logical key/value MVCC + transaction status
        |
shared buffered B-tree records
        |
optimistic page/node guards and fine-grained structural updates
        |
DRAM buffer residency + low-overhead PageId translation
        |
dirty pages
        |
out-of-place NVMe page/segment writes
        |
durable page mapping + periodic checkpoint
```

A buffered page can contain a record owned by an active transaction without
making that record visible to other snapshots. Visibility comes from MVCC/status
resolution. Multiple logical updates may therefore coalesce in one dirty page
before that page is physically rewritten.

Dirty pages flush **out-of-place** so the SSD-facing benefits of the current
design remain: sequential/batched placement, compression/packing, lifetime
separation, garbage collection, and future ZNS/FDP integration. The durable log
must contain enough transaction data to reconstruct committed logical state if a
process dies after commit durability but before page flush. Checkpoints bound
replay time rather than defining every commit.

The current 4 KiB node, translation mechanism, blob threshold, and buffer policy
are alpha choices. Page/node sizes, variable-record layout, translation,
pointer-swizzling/virtual-memory techniques, buffered vs direct I/O, and Linux
`io_uring` are all benchmark-gated before format stability. See
[ADR 0009](adr/0009-buffered-btree-and-materialization.md).

## HTAP and analytical representations

OmenDB is OLTP-first but does not require an ETL-fed second database for every
analytical workload. The initial safe architecture treats analytical state as a
**derived, rebuildable representation** of the authoritative transactional
snapshot/log. Candidates include workload-selected column stores, compressed
cold chunks, materialized expressions, vector/search projections, and skipping
metadata.

Each representation names the CSN/catalog frontier it covers. The planner uses
it only when it is valid for the requested snapshot or when a bounded delta
merge can bridge to that snapshot. Missing/corrupt derived state can be rebuilt
instead of making the primary unavailable.

A later single-copy hybrid row/column store is explicitly allowed if mixed-
workload benchmarks show that it beats the simpler row-source + columnar-
projection architecture. CedarDB/Colibri demonstrates that hot row data and
cold compressed column chunks can coexist efficiently on modern SSD/object
storage; OmenDB will measure rather than assume the same answer for its MVCC and
storage design.

Heavy analytics can also execute on remote workers/`omen-olap` bootstrapped by
the same snapshot/change stream, preserving strict OLTP resource isolation when
needed. See [ADR 0011](adr/0011-htap-and-analytical-representations.md).

## Deployment and durability profiles

One transaction/storage semantics supports several first-class physical
profiles rather than pretending one I/O policy is optimal everywhere.

| Profile | Commit durability | Hot tier | Durable/materialized tier | Object storage |
| --- | --- | --- | --- | --- |
| embedded/local | local durable log | process RAM / OS cache | local file/SSD | optional backup |
| single-node server | local NVMe log | managed DRAM cache | local NVMe | backup/archive |
| regional HA | quorum replicated log | per-node RAM | local/page-service NVMe | checkpoint/archive |
| disaggregated | replicated log service | compute/cache RAM | cache/page-service NVMe | authoritative checkpoint/archive |
| global | per-range replicated log + distributed transaction protocol | regional caches | regional storage | archive/bootstrap |

Local NVMe must benchmark current group commit against autonomous/parallel commit
and adaptive batching. Group commit remains a qualified fallback, not a permanent
hardware law. HA/disaggregated profiles use a replicated log as the short-term
commit authority while page/materialization services may lag and rebuild from
checkpoint + log.

Object storage is a designed immutable tier for checkpoints, archived log,
backups, cold version history, large immutable artifacts, analytical chunks,
and replica bootstrap. It is **not** the default fine-grained random-write OLTP
device. A future cost-first object-native physical materializer remains possible
when separately measured; it must not force an LSM layout on local NVMe mode.

See [ADR 0006](adr/0006-deployment-storage-and-durability.md).

## Contention and isolation direction

Pure retry-only first-committer-wins OCC is not the final answer for hot-row
OLTP. The target is adaptive:

- uncontended operations stay on the low-overhead optimistic path;
- repeatedly contended logical keys/ranges can acquire lightweight queued write
  intents/parking state;
- READ COMMITTED writers may wait for the prior writer, refresh/recheck the
  statement-visible row, and continue instead of forcing application retries;
- repeatable-read/snapshot transactions retain fixed-snapshot semantics and may
  still fail when a predecessor invalidates that snapshot;
- serializable mode layers dependency certification above the same primitives.

Constraint enforcement is mutation-driven: child writes probe referenced unique
keys; parent changes probe child-side FK indexes. Routine commit must not rescan
whole child tables and referenced indexes for every foreign key. Constraint
creation may scan once, but validation and catalog publication occur in the same
schema transaction.

## Catalog and schema direction

The current whole-catalog marker is a simple correctness format, not the final
DDL invalidation unit. The target catalog stores versioned schema/table/index/
constraint objects behind a catalog root/epoch. Transactions and cached plans
record the object versions they actually depend on, so unrelated DDL need not
invalidate every writer or prepared plan.

Catalog bytes remain ordinary transactional SeerDB data; OmenDB does not create
a second metadata database with independent durability semantics.

## Distribution and global direction

Distribution is a layer over the ordered logical keyspace, not a requirement for
local mode. The movement/replication unit is a logical range with an ownership
epoch and replica set. Relational **placement groups** intentionally co-locate
related tables/indexes on the same range boundaries and distribution key.

The native optimizer inserts routing/exchange operators and sends typed OmenDB
plan fragments to OmenDB nodes. It does not ship SQL to a second PostgreSQL
planner. Single-range transactions use the normal local fast path; only
cross-range transactions allocate a distributed transaction protocol.

Live split/move reuses the snapshot/change contract:

```text
snapshot CSN X + restart LSN Y
        -> copy X
        -> replay changes after Y
        -> catch up
        -> fence old owner / advance epoch
        -> publish topology
        -> reclaim old placement when safe
```

Regional HA is the default distributed durability primitive. Global placement is
locality-first; multi-region active-active writes are not treated as free. See
[ADR 0008](adr/0008-distribution-ranges-and-global-topology.md).

## Deterministic correctness

Fault injection remains necessary but is insufficient as concurrency and
distribution grow. Core transaction/storage/distribution code must make time,
randomness, device/network completion, and scheduling effects explicit enough
that an alternate deterministic runtime can explore difficult interleavings.

Simulation covers transaction/GC races, delayed/reordered I/O, crashes at every
durability boundary, network partitions, replica loss, failover, and range
movement. Production remains normal native Rust I/O; simulation is another
runtime for the same state machines, not a separate mock database.

## Current status and roadmap

The current tree has a direct Rust relational API, durable SeerDB integration,
SQL/catalog/index/constraint tests, fault and recovery coverage, and a
feature-gated PostgreSQL wire server. `src/pgwire_server.rs` exposes a persistent
`RunningServer` and `omendbd`: one process owns one opened database, bounds
admitted connection tasks, derives trust/SCRAM policy from the durable auth
catalog, reports lifecycle counters, and closes on explicit shutdown. Wire
cancellation routes `CancelRequest` to cooperative checkpoints and a daemon-level
SIGKILL/reopen test covers process loss.

The direct SeerDB path (`src/seer_direct.rs`) remains the single production
backend per [ADR 0005](adr/0005-delete-storage-kernel-seam.md). There is no
storage-kernel/backend matrix.

The dependency-ordered roadmap is now:

1. **Semantic contracts — substantially complete.** Keep TxnId/CSN/LSN,
   transaction status, logical MVCC/undo, snapshot/change positioning, tree
   lifecycle, crash-state invariants, and the OmenDB↔SeerDB boundary.
2. **Replace generation-COW storage with buffered/log-authoritative storage.**
   Build the ADR 0009 vertical slice: shared buffered B-tree pages, fine-grained
   page concurrency, dirty tracking, out-of-place materialization, checkpoint +
   log recovery, then qualify it against the existing implementation before
   deletion.
3. **Modernize row/data paths.** Add the ADR 0010 compact schema-driven codec,
   borrowed row views, selective typed batch decode, and only then benchmark
   multiple column families/large-value placement.
4. **Remove server-wide concurrency scaffolding.** Make the database handle
   cloneable/concurrent, unify schema transactions, eliminate routine FK scans,
   and replace `spawn_blocking` with the bounded worker/admission runtime from
   ADR 0007.
5. **Make planning semantic.** Introduce binder-owned result/parameter/effect
   metadata, typed IR, micro-plans and resumable typed batch pipelines; remove
   text-prefix statement classification and Describe execution probes.
6. **Add rebuildable analytical acceleration.** Use the batch path and committed
   frontier to prototype workload-selected columnar/cold representations under
   ADR 0011; compare against a single-copy hybrid layout before making either a
   stable physical-format commitment.
7. **Measure modern hardware policies.** Benchmark autonomous vs group/adaptive
   commit, page/node layouts, translation, allocation arenas, buffered/direct
   async I/O, hot-key waiting, RAM-heavy and larger-than-memory workloads on
   x86-64 and AArch64.
8. **Regional durability/replication.** Add quorum log replication and replica
   materialization/bootstrap using the same local transaction semantics.
9. **Distribution only after the local engine earns it.** Add range ownership,
   placement groups, online split/move and cross-range transactions under ADR
   0008; preserve the single-range/local fast path.

The implementation tracker is GitHub issue #1, **Implement the post-2026
architecture vertical slice**. Every architectural replacement requires
before/after performance evidence plus the relevant crash/fault matrix; no
component is retained merely because it already exists.

A test-only SeerDB reference model continues to serve as a semantic oracle. The
alpha release gates define evidence requirements; the current
`0.1.0-alpha.*` line remains unreleased until the server-first storage and
server criteria are met.
