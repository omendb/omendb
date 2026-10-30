# OmenDB architecture

**Product direction, 2026-10-30—not an implementation or performance claim.**
The [README](../README.md), [known limitations](gap-register.md),
[wire compatibility matrix](pgwire-compatibility.md) and [operator runbook](runbook.md)
describe the development preview. This page explains the intended product and
engine boundaries, not an implementation schedule.

## Product

OmenDB is a native, server-first, general-purpose relational database in Rust.
The goal is an OLTP-led engine with strong mixed-query behavior and clear
operational UX: concurrent transactions, useful scans/reports and predictable
maintenance, resource limits and recovery. PostgreSQL and MySQL are correctness,
functionality and performance references, not architectures to clone. Eventual
horizontal scale, high availability and online movement remain goals. Embedded
Rust clients share the engine but are not its primary performance target.

OmenDB owns its SQL, type and catalog semantics. PostgreSQL wire/dialect support
is an interoperability surface, compatible only where explicitly tested; it does
not require PostgreSQL's physical formats, process model, catalog IDs or extension
ABI. A native engine does not require a proprietary wire protocol.

Local/block and authoritative object-backed storage are first-class design targets.
The first executable qualification profile may use local durable indexed OLTP with
concurrent reads, writes, selective scans/joins/aggregates and history/checkpoint/GC
work. Larger-than-RAM data, skew, long snapshots and maintenance interference belong
in that profile. Storage placement does not select a niche or change SQL semantics.
No competitive performance or resource budget has been qualified.

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
          MVCC / conflicts / outcomes / retention
                   /                 \
        durable decision journal   ordered access / buffers
                   |                         |
        local / replicated /       materialization repository
          sealed object log        checkpoint / history / GC
                                             |
                                  local/block or object storage
                                  + disposable RAM/disk caches
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

Separate the **durable decision journal**, **materialization/checkpoint repository**
and **disposable caches** within one transaction authority. Target profiles are
local/block with no mandatory network, object-backed materialization with a local
or replicated low-latency journal, and potentially sealed/batched object-only
journaling with its explicit latency/cost trade-off. A local disk holding the only
acknowledged journal suffix is not a cache. Host-independent recovery needs all
required journal/history inputs to survive that failure, not just older data in a
bucket.

Object stores are not filesystems: immutable identity, validated range reads,
publication, writer fencing, ambiguous I/O and safe reclamation need their own
protocols. Page uploads can leave ACK only when the durable journal/dependencies
close recovery and complete effects are readable. Object durability alone supplies
neither HA nor distributed SQL ownership/global snapshots. Support and qualify
specific provider/API profiles without weakening local durability or pretending
all backends have identical latency, cost or failure guarantees.

Storage layout, row identity, compression and execution mechanisms remain measured
choices—not claims that a B-tree, LSM or particular runtime wins universally.
Whole-engine qualification must include matched semantics/durability, offered load,
retries, memory, total device/log/history traffic, recurring maintenance and recovery.

The serving preview still uses generation-COW SeerDB; the experimental replacement
is not a qualified persistent product runtime. Do not serve untrusted clients or
irreplaceable data while the documented integrity and authorization gaps remain.
