# SeerDB architecture

**Target boundaries, 2026-10-30—not a qualification claim.**
The [crate README](../README.md) describes the current generation-based ordered-KV
API. [Qualification](qualification.md) documents the available development
harnesses and their limits. The experimental replacement is not yet a qualified
persistent multi-writer runtime.

## Reusable kernel

SeerDB supplies durable concurrent transaction and storage capabilities to OmenDB
and independent Rust consumers. An ordered-KV client does not import SQL, catalog
or server code. OmenDB remains the primary integration/performance workload;
reusability is not a requirement to encode every specialized representation as
opaque KV or to preserve two transaction implementations.

The dependency is **OmenDB → SeerDB → durable-fs**. Put a capability in SeerDB when
its contract makes sense without OmenDB types or a relational schema.

| SeerDB owns | Consumer owns |
| --- | --- |
| Store/object identity, transactions, snapshots, outcomes, visibility, intents and generic dependency mechanisms | SQL statement semantics, schema-derived row identity, constraints and dependency registration |
| WAL, checkpoint, recovery, retention, GC and storage-resource accounting | Catalog/layout versions, DDL and relational maintenance obligations |
| Ordered trees/KV, buffers, pages/extents, allocation, overflow, mapping and I/O | SQL row/index codecs, covering/unique/FK meanings and relational adapters |
| Storage cursors/batches, reusable fault and durability machinery | Binder, optimizer, query operators/budgets/scheduling, authorization and protocols |
| Compression/index primitives with actual independent consumers | BM25/ANN/scoring, SQL equality/collation and specialized query semantics |

`durable-fs` owns filesystem durability helpers, not transaction decisions.

## One runtime and authority

One persistent runtime owns store exclusion, transaction identity/outcomes,
conflict evidence, ordering, WAL, contiguous visibility, snapshots/retention,
buffers, object lifetime, recovery and storage admission. Ordered KV and richer
compiled access paths share that authority and failure lifecycle.

Authoritative mutation consists of enrolled versioned storage effects with
concrete dependency, recovery and lifetime semantics. Recovery does not invoke
SQL or application callbacks. Mutable-page access is not an unlogged bypass.

Store/object incarnations, transaction IDs, CSNs, LSNs, frame/page incarnations,
logical versions and physical images are distinct identities. Physical locators
may accelerate access but cannot establish record identity or snapshot visibility.
Foreign or incomplete artifacts do not become authority because a checksum passes.

A successful commit requires complete authoritative effects in the contiguous
readable committed prefix. Durable decisions survive cancellation and ambiguous
acknowledgements. Checkpoint plus required committed log/history is restart
authority; a spill or maximum page LSN is not sufficient. Reclamation must preserve
all retained readers, outcomes and recovery inputs.

## Physical access and resources

The initial executable reference uses local RAM/NVMe, buffered ordered access,
compact current records and seekable retained history. Local/block and authoritative
object-backed storage are first-class targets of the same kernel. Row identity,
commit scheduling and physical layout alternatives remain conditional on whole-engine
evidence. No runtime plugin ABI or mandatory second engine is required; local/block
needs no network coordinator.

Storage cursors preserve logical continuation and snapshot identity while bounding
traversal, decoding, I/O and memory. Guards do not span executor suspension or slow
writeback. Logical overflow preserves representability and identity, not unlimited
transaction admission.

OmenDB owns the aggregate process budget. SeerDB accounts for its share and protects
WAL/installer/reclaim progress through explicit grants, without importing SQL
scheduling policy. Resources for possibly decided work remain owned until completion
or terminal fencing. Cooperative cancellation alone cannot release an active
worker's reservations.

Immutable analytical/vector extents may use different physical layouts where useful.
Separately ingested products keep independent authorities; shared algorithms do not
make their writes part of an OmenDB transaction.

## Storage authority and deployment profiles

Separate the **decision journal** (ordering, durable outcomes and replay),
**materialization repository** (immutable images/history/extents and closed checkpoint
roots), and **disposable caches**. These are responsibilities inside one runtime,
not independently committing databases. `durable-fs` remains a filesystem helper,
not an object-store append/fsync abstraction.

Target profiles include local/block without mandatory network, object-backed
materialization with a local or replicated low-latency journal, and sealed/batched
object-only journaling as an explicit latency/cost candidate. A journal containing
the only acknowledged suffix is authoritative, even if older images are in a
bucket. Profile guarantees state process/device/host/AZ/region failure assumptions;
object durability is not automatic database HA. Object-only storage does not
inherently prohibit concurrent or serializable transactions.

ACK requires the complete readable committed prefix, not necessarily final page
materialization. Deferring uploads requires complete journal/dependency recovery
closure, including supported retained cuts and outcomes; the current undo sync
cannot simply be removed. A bounded complete read overlay or invisible installation
may change physical scheduling, never permit missing index/catalog effects.

Remote profiles need immutable incarnation-qualified identities, integrity checks,
version-bound range reads, closed publication and fencing of journal/root ownership.
In-flight ambiguous requests and old destructive I/O remain owned through safe
resolution. GC joins retention registration, publication and physical-reader
quiescence; TTL alone is not proof, and retired identities cannot be reused while
old deletes could affect them. Packing, request/transfer cost, cold-cache recovery
and upload debt belong in resource accounting and qualification.

These are target contracts. The preview and experimental replacement do not yet
implement qualified object-backed profiles.
