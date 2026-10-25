# ADR 0015: Native server-first relational database

**Decision:** accepted direction, 2026-10-25. **Qualification:** not proven.
**Living contracts:** [architecture](../architecture.md),
[SeerDB](../../crates/seerdb/docs/architecture.md),
[relational](../design/relational-engine.md),
[extensions](../design/extensions.md),
[distribution](../design/distribution.md).
**Execution:** [roadmap](../roadmap.md).

## Context

The earlier SeerDB/OmenDB design had accumulated multiple "accepted target"
documents, a second generation-COW engine and speculative access-method
boundaries while the integrated persistent runtime remained incomplete.
The user's goal is a **native relational database comparable with PostgreSQL,
MySQL and SQLite for relevant workloads**, with future Neki-class
horizontal scaling, not a PostgreSQL reimplementation.
The server is the priority; embedded mode reuses the same engine.

SeerDB should remain generally useful where this does not compromise
OmenDB's workload, potentially through a separate ordered-KV facade.
Specialized text/vector/analytical engines may use different physical
representations, and standalone products can own independent authority
for independently ingested data. S3-compatible object storage is a
secondary tier, not the local transaction authority.

## Decision and rejected constraints

1. Maintain **one** OmenDB relational/SeerDB transaction ownership model,
   not a permanent KV-backend matrix or two separate commit/recovery engines.
2. Choose initially a mutable buffered ordered/clustered-row access path,
   with logical MVCC and an authoritative recoverable transaction log;
   compare heap/PAX/LSM, speculative invisible installation, page layouts
   and optimistic readers on real end-to-end workloads before fixing formats.
3. Treat PostgreSQL wire, SQL dialect and system-catalog compatibility as
   **optional adapters**, never requirements on the storage/core architecture.
4. Preserve SeerDB's reusable kernel and ordered-KV entry point while
   avoiding mandatory byte serialization or a single page/extent unit for
   unrelated access methods.
5. Native correctness, distributed placement, replication/HA and
   online resharding have **separate gates**. Neki's 118.5M QPS was a
   fleet-wide read-only/shard-local benchmark, not a single-node target.
6. SERIALIZABLE is the reference correctness implementation; READ COMMITTED
   is a distinct mode. **Public default isolation remains a measured
   product decision**, not accepted merely because this ADR exists.

## Why not the alternatives?

A universal ordered-KV boundary permits legitimate relational layering
(FoundationDB shows that), but constrains OmenDB's typed rows, batch execution
and specialist physical structures. An always-on pluggable-backend matrix adds
complexity with no user-visible benefit until a second engine wins a real
workload. Generation-COW publication incurs substantial write amplification
for the targeted mutable OLTP path. Copying Neki's stock-PostgreSQL router/
sidecars/OID adaptation would retain constraints a native engine need not
have. A monolithic single-page-size storage layout needlessly penalizes
cold vector, posting and columnar extents.

These are *design trade-offs*, not measured claims of global optimality.
Historical detail remains in this file's prior Git revision and the
preceding ADRs. Technical contracts now have one maintained home.

