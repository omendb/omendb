# ADR 0011: HTAP and analytical representations

- **Status:** accepted target; reviewed 2026-10-13; physical-format promotion is measured
- **Scope:** native operational analytics, columnar representations and optional workers
- **Related owners:** ADR 0007 execution; ADR 0010 rows; ADR 0013 snapshot/lifetime;
  [ADR 0012](0012-unified-modalities-and-rewrite-strategy.md) products/repositories

## Context

Mandatory ETL into another engine imposes extra copies, lag and operations even
when a single node could answer useful analytical queries. Conversely, integrating
all analytics into the primary does not make CPU, memory, bandwidth and history
pressure disappear. OmenDB supports local operational analytics and optional isolated
execution rather than selecting one deployment for every workload.

[Colibri](https://www.vldb.org/pvldb/vol17/p3290-schmidt.pdf) is a serious counterexample
to the assumption that HTAP requires duplicate row and column databases. Its hot/cold
hybrid design motivates an alternative to measure, not adoption of its physical layout
by reputation. The initial OmenDB acceleration remains derived and rebuildable.

## Decision

### 1. One authoritative transaction/log state

Within OmenDB, analytical execution resolves the same requested snapshot and catalog
as relational access. A representation can lag only behind explicit coverage/delta/
fallback logic. It cannot invent a second transaction history. Independently ingested
OLAP-companion data has a separate authority, as decided in ADR 0012.

### 2. In-engine analytics is a first-class execution mode

Build typed, resumable batches over authoritative rows first. Projection, predicates,
joins and aggregates need bounded work and selective decode, not mandatory conversion
through row-at-a-time adapters or a second SQL binder. Maintain one semantic plan and
type/NULL/cast/collation policy across micro-plan and batch execution.

Use physical statistics and cost estimates for scans, indexes, representation coverage,
selectivity, bytes read and materialization. JIT and custom SIMD follow profiles.
Arrow is an interchange/batch boundary; it need not be the only internal encoded form.
Do not embed a sibling database session merely to claim a native analytical executor.

### 3. Analytical physical representations are derived and rebuildable first

Start with authoritative rows plus optional selected column chunks, expressions and
skipping metadata. Do not copy every column automatically. Derived state can rebuild
from a retained snapshot/change suffix, allowing its layout to evolve independently
of primary recovery. Missing/corrupt derived data is never silently trusted or allowed
to change canonical results.

### 4. Queryable encoding is distinct from outer compression

Keep useful dictionary, bit-packed, delta, run-length or string encodings executable
where possible; decompress/materialize only when the operator requires it. Outer zstd
or other general compression is a separate tier/cost policy. Metadata and dictionaries
are evictable and accounted for, not assumed permanently resident.

Evaluate existing encoded-array/scan technology before inventing a format.
[Vortex's array/compute model](https://docs.vortex.dev/concepts/index.html) and
[scan interface](https://docs.vortex.dev/concepts/scanning.html) are concrete reuse
candidates. A library or format does not provide OmenDB snapshots, transactions or
resource safety. Parquet remains valuable for interchange and immutable cold data;
no vendor format ratio establishes the internal storage choice.

### 5. Hot and cold data may converge toward a single hybrid store

Compare the row-plus-derived-column baseline with a single-copy hot-row/cold-column
layout before stable physical-format commitment. Test compact row-ID/PAX/block layouts
as well as clustered rows; do not assume primary-key clustering is universally best.
Cold updates must not synchronously rewrite an entire large column chunk. Visibility,
relocation, history and recovery determine whether hot deltas or rematerialization
are safe.

Promote a hybrid only when its total storage/memory, point/range/update behavior,
scan cost, write amplification and recovery are better within declared OLTP budgets.
Duplicate representations cost maintenance and space; single-copy hybrids cost more
complex versioning, updates and conversion. Neither is a universal winner.

### 6. Analytical freshness is explicit

Use [ADR 0013's common contract](0013-storage-kernel-and-access-methods.md#3-storage-objects-have-explicit-authority-classes):
source incarnation, schema dependencies, reader-stable generation, contiguous coverage,
actual retained readability and complete deltas. All inputs to a join/aggregate must
resolve the same requested snapshot. The minimum of unrelated latest-only frontiers
cannot reconstruct it. Wait, fall back or return an explicit unsupported snapshot;
never silently serve stale/future data.

### 7. Automatic workload adaptation is a policy, not a correctness mechanism

Column selection, conversion and maintenance are bounded, observable, restartable
background work, safe to disable with operator overrides. Statistics inform promotion
and eviction; correctness never depends on an auto-tuner choosing well.

### 8. Storage tiers map naturally onto hot/cold analytics

Column/vector/posting extents may use larger I/O and cache units than mutable B-tree
pages. Share incarnation, publication, retention and aggregate resource accounting,
not one allocation size or eviction policy. Keep hot routing/dictionaries separate
from bulky cold payload when beneficial. Object storage belongs on sufficiently large
immutable reads/checkpoints with measured request cost and local caching, not default
fine-grained OLTP mutation.

Reserve memory, CPU, spill and I/O for each query/build/maintenance class while
protecting log, installation, recovery and essential reclamation. Include retention
pressure from long snapshots. Measured memory reservations are not necessarily process
RSS, and cooperative cancellation does not safely kill arbitrary blocking operators.
Use isolated workers where a hard fault/resource boundary is required.

### 9. Scale-out analytics remains supported through the same snapshot/change contract

A remote OmenDB worker bootstraps with the [retained handoff](0013-storage-kernel-and-access-methods.md#11-snapshotbootstrap-is-a-retained-handoff-not-just-two-numbers)
and applies whole transactions before advertising coverage. It can wait for a requested
snapshot, use an exact supported view or explicitly expose staleness. Source commit
success does not imply remote completion. Distributed plans must preserve semantics,
catalog identity, cancellation and bounded coordinator resources.

The standalone OLAP companion remains a separate product/repository for independently
ingested analytical data and isolation. A companion consuming OmenDB CDC is not thereby
a native remote worker or participant in its SQL transactions. Its mutable projection
contract must handle updates, deletes, schema, durable progress and rebootstrap;
append-only ingestion is insufficient. ADR 0012 owns this resolved product boundary.

### 10. Indexing and analytics cooperate instead of duplicating everything

The optimizer chooses among ordered, scan and specialized paths using costs and exact
coverage. Expose selection vectors and useful statistics, not opaque per-item adapters.
A table with no analytical workload must not pay a mandatory shadow-copy or background
maintenance tax.

### 11. The benchmark is mixed workload, not isolated TPC-H alone

Compare pure OLTP, pure analytics and mixed TPC-C/TPC-H or CH-benCHmark-shaped work;
current dashboards after writes; skew/update-heavy and read-mostly tables; long readers;
larger-than-memory data; ongoing conversion/compaction; and isolated remote execution.
Report both workload latencies/throughput, OLTP p50/p95/p99, memory/RSS, bandwidth,
bytes read/written, freshness/retention lag, checkpoint pauses and recovery costs.
Match durability, semantics and cache state before ranking engines or layouts.

## Consequences

Useful analytics does not require another database, while heavy or independently
ingested analytics need not inherit OLTP storage economics. The native batch path and
optional derived acceleration come first; a better measured single-copy layout can
replace them without changing SQL/transaction authority. No HTAP performance or strict
isolation guarantee follows merely from this design or a dependency's API.
