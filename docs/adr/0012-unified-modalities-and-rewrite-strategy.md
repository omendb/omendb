# ADR 0012: Unified modalities and rewrite strategy

- **Status:** accepted target architecture; implementation staged
- **Scope:** vector, text search, graph, document, time-series, KV boundaries, and how the current implementation is replaced
- **Depends on:** ADR 0006 through ADR 0011

## Context

The current OmenDB implementation has proved useful semantics, crash behavior,
PostgreSQL-facing compatibility, and benchmark infrastructure, but large parts of
its storage/runtime/execution implementation are no longer the desired target.
At the same time, separate OmenDB OLAP and vector repositories explored useful
specialized engines with their own WALs, manifests, segment stores, and query
runtimes.

Keeping those as independent databases would recreate exactly the state
synchronization problem OmenDB can avoid: one application fact would acquire
separate relational, vector, text, graph, and analytical authorities.

Modern systems demonstrate two useful facts at once:

1. applications benefit from composing relational, graph, vector, text,
   document, temporal, and analytical predicates inside one transaction/query;
2. those capabilities do not require every modality to own an independent
   transactional storage engine.

OmenDB therefore adopts a **single-authority multimodal relational architecture**.
Specialized physical representations are access paths or derived representations
above one transaction/log/catalog truth.

## Decision

### 1. One canonical transaction and catalog authority

Tables/rows and their schema are the canonical application-data authority.
Every mutation receives one OmenDB/SeerDB transaction outcome and one committed
ordering. A vector index, inverted text index, graph adjacency projection,
columnar chunk, materialized view, bitmap, zone map, or other accelerator may
never define a contradictory transaction history.

Derived state records the CSN/catalog frontier it covers. The optimizer may use
it only when it covers the query snapshot or when a proven delta path can close
the gap. Otherwise execution falls back to a canonical exact path.

### 2. Vector search is a type plus access methods, not a sibling database

OmenDB will support vector-valued columns as part of ordinary transactional rows.
Initial useful shapes are dense `f32`, half-precision, and later sparse or
multi-vector values when workloads justify them.

The planner chooses among vector access paths just as it chooses B-tree access:

- exact SIMD scan as the correctness oracle and selective-filter path;
- HNSW as the first in-memory/general ANN candidate;
- filtered traversal that cooperates with scalar/payload predicates rather than
  blindly post-filtering one global graph;
- quantized traversal plus canonical rerank when memory wins justify it;
- NVMe-oriented DiskANN/PAG-family or segment-partitioned graph layouts when the
  dataset exceeds RAM;
- object-storage-native ANN only as a separately measured tier, never by forcing
  an in-memory graph onto object storage.

Canonical full-precision vectors remain available for exact scoring/reranking
unless a declared storage policy explicitly trades that capability away.
Approximate indexes are rebuildable derived state and must be continuously
qualified against exact recall.

### 3. Full-text/BM25 search is another native access path

Text remains ordinary typed row data. A search index is an inverted/postings
representation maintained from committed row changes. BM25/faceted search and
vector search share the optimizer so hybrid queries can combine:

- structured SQL predicates;
- lexical ranking;
- vector similarity;
- reranking/fusion;
- joins and graph predicates.

Hybrid ranking belongs in typed query plans rather than an application-side join
between unrelated search services.

### 4. Property graphs are catalog/query semantics over relational data

OmenDB does not start with a separate graph storage engine. A property graph
catalog object maps vertex and edge tables (normally PK/FK related relational
tables) into graph semantics, following the direction of SQL/PGQ rather than
inventing a second graph-only data authority.

The executor gains graph operators for pattern matching, recursive traversal,
shortest/path queries, and graph-aware joins. When traversal workloads justify
it, OmenDB may build snapshot-versioned adjacency/CSR-like projections or other
graph indexes from the same committed rows.

A graph projection is rebuildable and has an explicit coverage frontier. Simple
traversals can always fall back to ordinary indexes/recursive execution.

### 5. Document data is a relational type, not a document-store fork

A PostgreSQL-compatible JSON/JSONB-style type and schema-aware expressions cover
the document use case inside tables. Path/value inverted indexes, existence
indexes, expression indexes, and statistics are access methods over that value.
Schema-less or partially typed tables may be supported as a relational catalog
policy; they do not require another storage engine.

### 6. Time-series and geospatial are specialized physical policies

Time-series workloads use ordinary transactions plus time-aware partitioning,
ordering, zone/BRIN-like metadata, retention, compression, and incremental
aggregates. Geospatial support similarly adds types, operators, and spatial
indexes when justified.

Neither gets an independent WAL/MVCC authority.

### 7. Incrementally maintained views are a first-class derived modality

The committed change stream can feed continuously maintained materialized views
inside OmenDB. This is useful for operational analytics, agent/context queries,
continuous aggregates, denormalized serving views, and graph/search features
that should remain fresh without rescanning base tables.

Incremental maintenance runs under the same resource/admission system as other
background work and publishes its covered CSN. It must never block log/recovery
progress or silently serve a stale view as current.

### 8. KV remains SeerDB's native product boundary

SeerDB is already the ordered transactional KV substrate. OmenDB should not add
a second Redis/FoundationDB-style product API that bypasses its catalog and SQL
semantics merely to claim another model.

Applications that genuinely want generic ordered KV can use SeerDB directly.
OmenDB can still expose efficient byte-key/byte-value relational tables and the
same transaction engine internally.

## Repository and rewrite strategy

### Keep the repository; replace the implementation

Do **not** create a clean OmenDB repository. The existing repository contains the
executable specification we want to preserve:

- transaction/fault/crash recovery matrices;
- PostgreSQL differential tests and SQLSTATE behavior;
- pgbench/TPC-B and profiling harnesses;
- row/index/catalog property tests;
- server lifecycle, cancellation and admission tests;
- historical benchmarks identifying prior false hypotheses.

The new engine is developed against those tests and new architecture-specific
gates. Internal APIs, formats, modules, and algorithms have no compatibility
protection merely because the old implementation used them.

A temporary `next` implementation/module/branch is acceptable to keep main
green, but it is not a permanent engine matrix. Once the replacement satisfies
semantic, fault, and performance gates, it becomes `seerdb` and the legacy path
is deleted.

### OmenDB OLAP repository

`omendb-olap` is not merged wholesale. Its independent WAL, SQLite manifest,
namespace transaction authority, and LSM/Parquet database lifecycle conflict
with ADR 0011's one-authority HTAP direction.

Before archival, salvage:

- Arrow/Parquet interoperability tests;
- streaming/spill/resource-budget lessons;
- immutable segment/manifest and compaction fault tests that generalize to
  derived analytical representations;
- benchmark datasets and CH/analytical harness ideas.

Fresh HTAP implementation lives in the OmenDB monorepo. It may become an
internal crate (for example an analytical representation/executor crate) if that
creates a real compile-time ownership boundary, but it shares the OmenDB catalog,
transaction log, snapshot frontier, runtime, and optimizer.

### OmenDB Vector repository

`omendb-vector` is also not merged as a second database. Its independent WAL,
record store, segment manifest, and transaction model are replaced by OmenDB's
canonical rows/log.

Salvage and port only components that remain useful as algorithms/evidence:

- exact-search oracle and recall harnesses;
- HNSW/quantization implementations if they remain competitive after review;
- filtered-search routing and selectivity tests;
- BM25/filter/hybrid-ranking test corpus;
- ANN datasets, OOD tests, recall/latency benchmark methodology;
- segment-native ANN research where it fits the new derived-index model.

The resulting vector/text implementation becomes OmenDB access methods and
planner operators over transactional tables.

## Crate boundaries

Do not split by marketing modality. Split only where ownership, build cost or
unsafe/performance kernels justify it. A likely long-term shape is:

```text
omendb                 SQL/catalog/planner/server/product API
seerdb                  transaction/MVCC/log/ordered storage
omendb-exec (optional)  typed scalar + batch execution runtime
omendb-search (optional) vector + inverted-index access methods/kernels
omendb-analytic (optional) derived columnar representations/maintenance
```

These are workspace implementation boundaries, not independent databases. They
share one logical catalog/transaction authority through narrow interfaces.

## Acceptance principle

A modality is integrated only when it can satisfy all three:

1. **transactional composability:** ordinary SQL data and the modality can be
   queried/mutated with one coherent snapshot/transaction;
2. **specialized performance:** a dedicated index/representation is competitive
   with serious specialist systems on its intended workload;
3. **no mandatory tax:** databases that never use the modality do not pay a
   material storage, write, memory, or coordination cost for it.

This is the rule for vector, text, graph, columnar, streaming views, geospatial,
and future modalities.