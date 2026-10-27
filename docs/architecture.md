# OmenDB architecture

**Design target, 2026-10-25. Not an implementation or performance claim.**
This is the maintained product and engine map. Detailed contracts live in
[SeerDB](../crates/seerdb/docs/architecture.md), [relational execution](design/relational-engine.md),
[specialized engines](design/extensions.md) and [distribution](design/distribution.md).
The [SeerDB roadmap](../crates/seerdb/docs/roadmap.md) owns kernel work; the
[OmenDB roadmap](roadmap.md) owns product/SQL integration and release gates.
[ADRs](adr/README.md) preserve historical decisions and alternatives, not parallel specs.

## Product

OmenDB is a **native, server-first, general-purpose relational database**. It aims
to compete with PostgreSQL and MySQL in relational functionality, correctness,
operability and performance, and later with Neki/Vitess-style systems in *horizontal*
scale, availability and online reconfiguration. The native SQL/type/catalog contract
is OmenDB's; a PostgreSQL wire/dialect adapter is optional and only as compatible
as explicitly tested. Do not design the core around PostgreSQL physical formats,
processes or catalog IDs. Embedded Rust usage shares the engine, with a smaller
deployment footprint, but is not the primary performance target. "Native" describes
OmenDB's engine and semantics, not a requirement to invent a proprietary wire
protocol; the first server can use its documented PostgreSQL-wire subset.

Initial authoritative writes and hot pages live on local RAM/NVMe. S3-compatible
object storage is a secondary, optional tier for backups, immutable checkpoints,
archived WAL and cold extents. Remote-primary/disaggregated storage needs a separate
durability, latency and recovery decision. No cloud control plane is required for
local serving.

## Engine boundaries

| Owner | Owns | Does not own |
| --- | --- | --- |
| **OmenDB** | SQL/types/NULL semantics, catalog and constraints, binder, planner, execution, authorization, client/server protocols | Its own independent WAL or transaction status database |
| **SeerDB** (workspace `crates/seerdb`) | Transactions, visibility/MVCC, conflicts/certification, one durable decision log, buffer/page/extent services, checkpoint, recovery, retention and admission | SQL types, query plans, BM25/ANN ranking or a universal byte-KV physical contract |
| **Specialized access engines** | Physical indexing/computation for ordered rows, text/postings, vector/ANN, columnar analytics and later modalities | Independent commit authority over canonical rows inside one OmenDB transaction |
| **Standalone Vector / OLAP products** | Their own datasets, APIs and qualified persistence when independently ingested/deployed | Automatic atomicity with an OmenDB SQL transaction |
| **durable-fs** | Generic filesystem durability utilities | Database commit or snapshot semantics |

The dependency direction is **OmenDB → SeerDB**. OmenDB-specific schemas,
typed record/secondary-index encodings, SQL constraints and catalog behavior
remain in the OmenDB crate, including its SeerDB relational adapter. General
storage algorithms, transactional capabilities, WAL/MVCC/recovery,
buffer/page/extent management and reusable storage I/O belong to SeerDB
when a non-SQL consumer can use them. A generic filesystem durability
helper belongs in `durable-fs`, not in the SQL server. See the
[explicit code-placement policy](../crates/seerdb/docs/architecture.md#source-ownership-and-dependency-direction).

SeerDB should remain usable by other Rust projects through a transactional ordered-KV
facade and useful kernel services, if that does not constrain OmenDB. Its internal
contract is typed object/page/extent/transaction capabilities, **not** a requirement
to wrap every row, posting or column group as bytes in one universal KV tree.
Its independently useful interface and packaging must share the same runtime and
recovery code as OmenDB. No permanent backend switch or duplicate storage engines.

## End-to-end shape

~~~text
native SQL/API clients    optional PostgreSQL protocol adapter
          \                      /
            sessions / security
                     |
          typed binder + catalog
                     |
           optimizer / plan IR
              /            \
       OLTP micro-plans   typed batch pipelines
              \            /
         relational rows + constraints
                     |
          SeerDB transaction runtime
     MVCC / certification / WAL / retention
                     |
         authoritative access methods
         /                         \
  clustered ordered rows       indexes / catalog
                     |
           buffers + extents
                     |
      local NVMe WAL + checkpoint + GC
                     |
   optional object-tier archive / backup

  later: native text / ANN / columnar access engines
  later: per-shard replication, routing, online resharding
~~~

Canonical rows and required primary/unique/FK indexes, plus catalog metadata,
share **one durable transaction outcome within a local transaction domain**.
Later cross-partition transactions must coordinate multiple such domains through
the explicit distributed protocol; one logical SQL outcome does not require one
physical WAL for the entire distributed database. Unsupported multi-domain work
is refused before effects; a multi-domain statement also needs one proved consistent
read cut and catalog/topology view, not independent latest reads. Derived structures
are admitted only with explicit source identity, schema dependency, retained snapshot coverage and
complete change intervals or a correct fallback/staleness contract. Their separate
cache and encoding units do not entail separate SQL durability.

## Reliability and performance bar

A durable commit is irreversible even after cancellation/disconnection; an
acknowledgement additionally requires visibility through the contiguous published
commit frontier. No unresolved decision can silently become an abort. Snapshot,
CDC, backup, GC, and checkpoints share exact identity/retention authority.
Recovery from a complete checkpoint and committed log must restore all required
authoritative state and retained history; a spilled page or max page LSN alone
is not restart authority.

Measure **whole-engine** indexed OLTP, mixed read/write/scan loads, contention,
larger-than-memory workloads, long snapshots, checkpoints, failures and recovery,
not just in-memory KV operations or synthetic throughput. Compare SQL semantics
with PostgreSQL/MySQL/SQLite, standalone KV semantics with RocksDB/Fjall/SurrealKV,
and later shard-local throughput, HA and resharding with Neki/Vitess-style systems
under matched conditions. Treat vendor results as hypotheses, not OmenDB results.

The currently serving code still uses a generation-COW SeerDB path; `vnext` is a
component-level replacement, **not** a qualified persistent product runtime.
The [gap register](gap-register.md), [release gates](alpha-release-gates.md) and
[Pgwire compatibility matrix](pgwire-compatibility.md) describe current limitations,
not targets imposed on the native engine. Do not serve untrusted/irreplaceable
data on the preview while known integrity and authorization gaps remain.

