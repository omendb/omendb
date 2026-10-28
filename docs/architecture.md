# OmenDB architecture

**Product direction, 2026-10-28—not an implementation or performance claim.**
The [README](../README.md), [known limitations](gap-register.md),
[wire compatibility matrix](pgwire-compatibility.md) and [operator runbook](runbook.md)
describe the development preview. This page explains the intended product and
engine boundaries, not an implementation schedule.

## Product

OmenDB is a native, server-first, general-purpose relational database in Rust.
It aims to compete with PostgreSQL and MySQL in relational functionality,
correctness, operability and performance, and eventually support horizontal
scale, high availability and online movement. Embedded Rust clients share the
engine but are not its primary performance target.

OmenDB owns its SQL, type and catalog semantics. PostgreSQL wire/dialect support
is an interoperability surface, compatible only where explicitly tested; it does
not require PostgreSQL's physical formats, process model, catalog IDs or extension
ABI. A native engine does not require a proprietary wire protocol.

The first qualification profile is local durable indexed OLTP with concurrent
reads, writes, selective scans/joins/aggregates and history/checkpoint/GC work.
Larger-than-RAM data, skew, long snapshots and maintenance interference belong in
that profile. No competitive performance or resource budget has been qualified.

## Engine boundaries

| Owner | Responsibility |
| --- | --- |
| **OmenDB** | SQL/types/NULL semantics, schemas/catalog, constraints, relational row/index codecs, binding, planning, execution, authorization, process budgets, query scheduling and protocols |
| **SeerDB** | Reusable transactions, MVCC/visibility, conflict evidence, durable decisions, ordered access, buffers/pages/extents, checkpoint, recovery, retention and storage admission |
| **Specialized access engines** | Physical text/postings, vector/ANN or analytical representations when an integrated workload justifies them |
| **Standalone Vector/OLAP products** | Independently ingested datasets and their own qualified durability; code sharing does not confer SQL atomicity |
| **durable-fs** | Filesystem durability utilities, not database transaction decisions |

The dependency direction is **OmenDB → SeerDB → durable-fs**. SeerDB can serve an
independent Rust ordered-KV consumer without SQL or server dependencies. Its
reusability must not force every row, posting or column group through one opaque
KV representation. See the [SeerDB architecture](../crates/seerdb/docs/architecture.md).

## Intended serving path

```text
SQL / Rust API / optional PostgreSQL-wire clients
                         |
             sessions and authorization
                         |
              typed binding and catalog
                         |
                  costed query plans
                   /            \
          OLTP micro-plans   bounded batch pipelines
                   \            /
               rows, indexes, constraints
                         |
             SeerDB transaction runtime
          MVCC / conflicts / WAL / retention
                         |
               ordered access / buffers
                         |
          local NVMe / checkpoint / history / GC
                         |
           optional immutable object-storage tier
```

Canonical rows, required indexes and catalog share one durable outcome within a
local transaction domain. OmenDB does not maintain a second SQL WAL or independent
transaction-status authority. Later distributed transactions must coordinate local
domains explicitly; unsupported cross-domain work must be refused before partial
effects. Independent latest reads do not constitute one distributed SQL snapshot.

Derived representations need exact source/schema identity and retained snapshot
coverage, complete changes, or an explicit freshness/approximation contract.
Approximate ranking does not permit approximate authorization, deletion or predicate
eligibility. Separate encoding and cache units do not imply separate SQL durability.

## Reliability, resources and storage

A durable decision is irreversible after cancellation or disconnect. Success also
requires complete authoritative effects to be readable through the contiguous
committed prefix; an unresolved outcome cannot silently become an abort.
Recovery uses a structurally complete checkpoint plus required committed log and
history, not a surviving spill or maximum page LSN.

Execution, results, sessions, caches and storage maintenance share bounded process
resources with protected completion capacity. Cancellation is not proof that a
worker stopped, and cooperative checks are not a security or hard RSS boundary.
SERIALIZABLE is the reference correctness mode; the default SQL isolation remains
an open product choice, including READ COMMITTED and retry ergonomics.

Local RAM/NVMe is the initial authoritative serving tier. S3-compatible storage is
initially optional for immutable checkpoints, backup, archived log and cold data.
Remote-primary storage, replication and sharding require their own durability,
latency, ownership and recovery protocols, without imposing network authority on
the local fast path.

Storage layout, row identity, compression and execution mechanisms remain measured
choices—not claims that a B-tree, LSM or particular runtime wins universally.
Whole-engine qualification must include matched semantics/durability, offered load,
retries, memory, total device/log/history traffic, recurring maintenance and recovery.

The serving preview still uses generation-COW SeerDB; the experimental replacement
is not a qualified persistent product runtime. Do not serve untrusted clients or
irreplaceable data while the documented integrity and authorization gaps remain.
