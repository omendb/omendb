# ADR 0011: HTAP and analytical representations

- **Status:** accepted target architecture; representation strategy remains benchmark-gated
- **Scope:** analytical scans, hot/cold physical layout, columnar acceleration, and optional scale-out OLAP
- **Depends on:** [ADR 0006](0006-deployment-storage-and-durability.md),
  [ADR 0007](0007-runtime-execution-and-contention.md),
  [ADR 0009](0009-buffered-btree-and-materialization.md), and
  [ADR 0010](0010-row-layout-and-column-families.md)

## Context

The earlier architecture assumed that OLTP and OLAP would use separate physical
engines connected through snapshot export plus a committed-change stream. That
is a clean isolation boundary, but making it mandatory has costs:

- fresh analytics require a second copy and change-application pipeline;
- applications must operate two systems even when one node has enough compute
  and storage bandwidth for both workloads;
- resource isolation becomes an operational problem rather than something the
  database runtime can schedule directly;
- real-time dashboards, vector/AI enrichment, and agent-generated analytical SQL
  increasingly mix short transactions with scans over current state.

Modern systems show that the trade-off is no longer binary. CedarDB/Umbra's
Colibri work demonstrates a single transactional system with hot row-oriented
data and cold compressed column chunks beyond main memory. AlloyDB demonstrates
a different point in the design space: one transactional source of truth plus a
workload-managed columnar representation used by a separate vectorized execution
path. Both avoid making ETL into another database a prerequisite for useful
analytics.

OmenDB should preserve the ability to scale analytics out independently, but it
should not force every deployment to do so.

## Decision

### 1. One authoritative transaction/log state

OmenDB has one authoritative transaction outcome, MVCC history, catalog, and
committed-change order. Analytical representations do not define independent
transaction truth.

A query that requires the latest committed snapshot can therefore execute
against row or analytical representations only when the representation proves it
covers that snapshot. Otherwise the planner falls back to the authoritative row
path or combines a stable analytical base with newer transactional deltas.

### 2. In-engine analytics is a first-class execution mode

The typed batch executor from ADR 0007 is part of OmenDB itself. It can scan the
ordinary row store immediately; analytical acceleration may provide more
scan-friendly representations without changing SQL semantics.

The runtime schedules OLTP and analytical work under explicit resource classes.
Large scans have bounded memory, I/O and CPU reservations and cannot consume the
progress reserve required by transactions, WAL/log durability, recovery, or
reclamation.

This permits HTAP on one machine while preserving predictable OLTP tail latency.

### 3. Analytical physical representations are derived and rebuildable first

The initial analytical acceleration is a derived representation that can be
rebuilt from a consistent snapshot plus the committed-change stream. It is never
the only copy required to recover the database.

Useful representations include:

- compressed column chunks for stable/cold row ranges;
- a workload-selected in-memory/SSD column store for frequently scanned columns;
- materialized expressions used repeatedly by scans;
- vector/search-specific projections;
- zone/min-max/bloom metadata for skipping row or column chunks.

Because the representation is derived, its format can evolve faster than the
transactional row format and corrupt/missing analytical state can be discarded
and rebuilt rather than making the primary database unavailable.

### 4. Hot and cold data may converge toward a single hybrid store

A derived columnar cache is the lower-risk first implementation, but the
architecture explicitly permits a Colibri-like hybrid store if measurements
show it is superior:

```text
logical row / RowId
       |
ordered primary/index structures
       |
   +---+-------------------------+
   |                             |
hot mutable rows            cold stable ranges
row-oriented pages          compressed column chunks
   |                             |
   +-------------+---------------+
                 |
           one MVCC/log state
```

Cold conversion is online and snapshot-aware. A row/range becomes eligible only
when its versions and update temperature permit conversion without harming the
transactional hot path. A later update can materialize a hot row/delta without
rewriting an entire cold chunk synchronously.

The exact choice between "row source + columnar projection" and "single-copy
hybrid row/column storage" remains benchmark-gated. OmenDB does not promise one
before comparing mixed-workload performance, write amplification, cache
footprint, recovery complexity, and operational behavior.

### 5. Analytical freshness is explicit

Every analytical representation carries a covered logical frontier (CSN and,
where needed, schema/catalog version). The planner may use it when:

- the query snapshot is at or below the frontier; or
- a delta-merge path can apply committed changes from the frontier to the query
  snapshot within a configured cost bound.

No query silently reads stale analytical state because it happens to be faster.

### 6. Automatic workload adaptation is a policy, not a correctness mechanism

The database may sample query/column access, update frequency, selectivity,
compression, and cache pressure to recommend or automatically populate
analytical representations.

Auto-columnarization or hot/cold conversion must be:

- bounded background work;
- interruptible and restartable;
- observable in diagnostics/EXPLAIN;
- safe to disable;
- never required for transactional correctness.

A manual policy/override remains available for operators who need deterministic
layout.

### 7. Storage tiers map naturally onto hot/cold analytics

Deployment profiles from ADR 0006 can place analytical representations
according to access pattern:

- DRAM: hottest dictionaries, metadata, frequently scanned columns;
- local NVMe: large column chunks and scan cache;
- page/cache service NVMe: disaggregated analytical cache;
- object storage: immutable cold chunks/checkpoints where request size and
  bandwidth amortize latency.

Object storage is much more suitable for large immutable analytical chunks than
for OLTP random page updates. This is one of the few places where it may be part
of the normal query path rather than only backup/archive.

### 8. Scale-out analytics remains supported through the same snapshot/change contract

A future `omen-olap` or remote analytical worker pool can bootstrap from:

```text
snapshot CSN X + restart LSN Y
```

and apply committed changes after Y. It therefore shares transaction truth with
in-engine analytical projections rather than introducing a second replication
contract.

The optimizer may choose local HTAP execution or remote analytical execution
based on size, freshness, locality, queue pressure and cost. Deployments that
need strict OLTP isolation can keep heavy analytics off the primary server;
smaller deployments do not have to operate another database.

### 9. Indexing and analytics cooperate instead of duplicating everything

The primary B-tree and secondary indexes remain optimized for point/range OLTP.
Analytical representations add scan-oriented statistics/chunks only when useful.
The planner considers both access families and can combine them.

Do not maintain a columnar copy of every column by default. Workload-driven
selection and cold-range conversion should avoid doubling write amplification
for tables that never run analytical scans.

### 10. The benchmark is mixed workload, not isolated TPC-H alone

A representation is accepted only if it improves useful mixed workloads without
silently destroying transaction tail latency. Qualification includes:

- CH-benCHmark / TPC-C + TPC-H style mixtures;
- current-state dashboards after writes;
- pure OLTP baseline to measure analytical-maintenance tax;
- pure analytical baseline to measure scan quality;
- hot/cold skew and larger-than-memory data;
- update-heavy tables that should resist columnarization;
- read-mostly tables that should convert aggressively;
- local NVMe and object-backed cold chunks where implemented.

Report both analytical throughput/latency and OLTP p50/p95/p99, plus CPU,
memory, bytes written, cache occupancy, conversion work and freshness lag.

## Non-goals

- making a second OLAP database mandatory;
- making every OLTP table columnar;
- weakening transaction isolation for faster analytics;
- loading a full columnar shadow copy into RAM by default;
- coupling the public CDC format to one analytical physical representation.

## Research inputs

- CedarDB/Umbra Colibri, *Two Birds With One Stone: Designing a Hybrid Cloud
  Storage Engine for HTAP* (VLDB 2024) — hot row data + cold compressed column
  chunks with a B+-tree/row-id bridge and modern SSD/object-storage design;
- AlloyDB columnar engine — workload-managed columnar representation and
  vectorized planner/executor beside a PostgreSQL-compatible transactional
  engine;
- HyPer/Umbra lineage — MVCC snapshots, data-centric/code-generated execution,
  and morsel-driven parallelism for mixed workloads.

## Consequences

- The previous statement that OLTP and OLAP **must** use separate physical
  engines is superseded.
- OmenDB remains OLTP-first, but useful real-time analytics can run directly on
  current transactional state.
- The snapshot/change stream still matters: it powers derived analytical
  representations and optional scale-out analytics rather than forcing the
  latter.
- A future hybrid row/column physical store is allowed, but only after it beats
  the simpler derived-projection design on measured mixed workloads and passes
  recovery/fault qualification.
