# OmenDB architecture

**Status:** accepted target, reviewed 2026-10-13; not an implemented release.
The serving backend still uses the generation-COW SeerDB engine. The replacement
under `crates/seerdb/src/vnext` is not yet a restart-safe product backend.
The [vNext plan](plans/storage-kernel-vnext.md) owns implementation order and
open gates; this document is the architectural overview, not another backlog.

## Product and deployment decision

Build an integrated, server-first relational database for RAM and local NVMe,
with an embedded Rust surface using the same semantics. Support native vector
and text access paths and useful operational analytics over its transactional
data. Heavy analytics can move to optional workers or a separate OLAP companion.
Neither a cloud service nor a second database is required for local operation.

The boundary is **one transaction authority for shared authoritative data**, not
one physical representation, process, or repository for every workload.
Independently ingested analytical datasets may have their own durable authority;
they are not silently part of an OmenDB SQL transaction. Repository and product
ownership are decided in [ADR 0012](adr/0012-unified-modalities-and-rewrite-strategy.md).

PostgreSQL compatibility concerns wire protocol, SQL behavior, drivers and tools.
It does not require PostgreSQL's pages, WAL, executor or process model. The
supported surface remains bounded by the [compatibility matrix](pgwire-compatibility.md)
and [gap register](gap-register.md).

## Ground-up structure

```text
PostgreSQL clients / direct Rust API
                  |
       sessions / authorization / protocol
                  |
       one binder / catalog / typed plans
                  |
     OLTP micro-plans + resumable batch execution
                  |
        relational meaning and constraints
                  |
        SeerDB transaction/storage kernel
           /          |               \
    ordered trees   canonical rows   optional access paths
           \          |               /
         shared commit / MVCC / lifetime authority
                  |
     mutable buffered pages + immutable extents
                  |
            RAM / NVMe / optional cold tiers
```

### Ownership

OmenDB owns SQL types and NULL behavior, schemas/layout versions, constraints,
authorization, catalog/name resolution, planning and execution. SeerDB owns
transaction identities/outcomes, snapshots, conflicts/intents, recoverable logging,
visibility, resource admission, buffering, materialization and physical lifetime.
The kernel does not acquire SQL, BM25 or vector-distance semantics.

Ordered KV remains a useful API over the ordered access method. It is neither
a second engine nor the universal physical interface. A transactional KV layer
can implement relational consistency; the reason to expose a lower internal
boundary is control over layout, batching and shared lifecycle, not a claim that
KV systems cannot support transactions or selective access.

Likewise, do not replace a universal KV boundary with a universal small-page
boundary. Ordered nodes need guarded mutable frames; column chunks, vector
payloads and postings may need immutable extents, range reads and different cache
units. They share ownership and budgets, not mandatory page sizes or codecs.
[ADR 0013](adr/0013-storage-kernel-and-access-methods.md) owns these contracts.

## Local storage and commit

The local default is a buffered B-link/B+tree family: update hot state in RAM,
resolve visibility through logical MVCC, and coalesce dirty-page writes before
out-of-place SSD materialization. Per-transaction whole-tree/generation copying
is not the target. Node layouts, translation hints, direct I/O, compression,
large-value separation and FDP/ZNS are measured policies, not prerequisites.
[ADR 0009](adr/0009-buffered-btree-and-materialization.md) owns ordered layout.

The first row path uses compact schema-driven records and typed guarded access.
Clustered primary-tree records are the baseline to compare with row/heap pages
plus a primary index. Prove row/index/catalog atomicity and useful row access;
do not build a second physical tree implementation merely to demonstrate modularity.
[ADR 0010](adr/0010-row-layout-and-column-families.md) owns row semantics.

The target ordinary commit is:

```text
private writes + dependencies + protected completion resources
  -> prepare outside the decision lane
  -> certify / order / append complete recoverable decision
  -> establish log durability
  -> install authoritative effects and finish structural work
  -> publish a contiguous visible prefix
  -> acknowledge

page/history materialization and checkpoints follow independently
```

The current coordinator instead synchronizes the WAL and then required undo
before installation. Those barriers remain mandatory for its existing format.
The target removes a separate synchronous history flush only after the log,
retained checkpoints and logical version identities can reconstruct all required
state. This is a protocol/format change, not deletion of an `fsync`. Large
external payloads may legitimately require additional dependency barriers.
[ADR 0014](adr/0014-vnext-installation-and-recovery.md) owns both the baseline and
the replacement's proof obligations.

Use bounded adaptive batching without imposing a fixed wait on an uncontended
writer. Parallelize preparation and independent installation. Keep encoding,
page reads and durability waits out of the short certification/ordering lane.
Autonomous NVMe logging is an experiment, not an assumption that one log authority
must become one physical writer forever. Measure lane occupancy, barriers and
ready-to-visible head-of-line waits separately.

## Isolation, lifetime and recovery

Keep `TxnId`, logical CSN, durable LSN, store incarnation and physical page/frame
identities distinct. Fixed snapshots and write intents alone are not serializable.
Initial serializable writers use decision-ordered optimistic certification of
point, absence, range, catalog and constraint observations, including earlier
accepted effects not yet installed. Hot-key waiting and other isolation modes
must preserve their declared semantics rather than silently retry a stale snapshot.

One runtime owns transaction/history leases, admission and commit responsibility.
An unresolved attempt after possible decision append must fence before unwinding
releases ownership. Cancellation abandons response delivery, not an irrevocable
commit. Reserve bounded completion capacity before that boundary, including under
query, history, cache and allocation pressure.

A complete checkpoint plus retained committed log suffix defines restart state.
Working spill pages and a maximum page LSN do not prove complete logical replay.
Checkpoint publication preserves prior valid authority; recovery retries have a
bounded working-allocation lifecycle. Snapshot, CDC, backup and prior-checkpoint
retention must participate in reclamation. A snapshot/LSN pair without an atomically
registered lease is not a safe bootstrap API. See ADRs 0013 and 0014.

## Execution and specialized representations

Use one typed binder for Describe and execution; Describe evaluates no user query.
Retain catalog/layout/enforcement dependencies, enforce current grants at each
statement boundary, and keep statement deltas private. Explicit-block failure
makes the transaction rollback-only; catalog, index and constraint work cannot
escape that lifecycle. [ADR 0007](adr/0007-runtime-execution-and-contention.md)
owns these semantics and the bounded worker/admission runtime.

Short OLTP operations use direct micro-plans. Scans, joins and searches use typed,
resumable batches, selection vectors, selective decoding and costed access paths.
Planning statistics must cover ordinary columns and, where used, filter/vector
correlation, representation coverage and maintenance cost. Arrow/Parquet are
interchange boundaries, not mandatory decoding or row-storage formats. JIT and
custom kernels must improve measured workloads rather than define the architecture.

Operational analytics starts on authoritative rows, with optional snapshot-valid
columnar acceleration. Benchmark this against a single-copy hot-row/cold-column
layout before committing the latter as a stable format. Never silently combine
unrelated latest-only representations as one snapshot. ANN may approximate ranking,
not row identity, deletion, predicates or authorization. Canonical validation,
complete delta coverage and refill/exact fallback remain required.
[ADR 0011](adr/0011-htap-and-analytical-representations.md) owns analytics;
ADR 0012 owns modality/product boundaries.

All execution and maintenance share explicit memory, CPU, I/O and history budgets.
Different caches may use different policies; their totals have one admission owner.
Progress-critical logging, installation, recovery and reclamation keep protected
capacity. Telemetry or cooperative cancellation alone is not a hard process-wide
resource/termination guarantee; isolated workers are appropriate where needed.

## Deployment, qualification and non-goals

Local mode has no network coordination tax. Regional HA adds a replicated durable
log and fenced ownership before general distribution. Object storage is appropriate
for immutable archives, checkpoints and sufficiently large cold reads, not ordinary
fine-grained OLTP updates. Replication, failover and control-plane availability
need their own evidence; fast logging proves none of them by itself.
[ADR 0006](adr/0006-deployment-storage-and-durability.md) owns deployment profiles;
[ADR 0008](adr/0008-distribution-ranges-and-global-topology.md) owns later distribution.

The architecture selects mechanisms, not a state-of-the-art performance claim.
Representative benchmarks must match durability, isolation, hardware, cache state,
transaction size and correctness, and include churn, long snapshots, checkpoints,
larger-than-memory data and recovery. The [plan](plans/storage-kernel-vnext.md)
selects experiments before cutover; [alpha gates](alpha-release-gates.md) govern
release. No doc update closes a code or qualification gate.

Retain useful existing oracles, but resolve known legacy defects against independent
semantics. Cutover removes the generation-COW engine, transitional adapters and
obsolete docs rather than maintaining a runtime backend matrix. Repository history
is retained; private research and sibling code are not published by implication.
