# ADR 0012: Unified modalities and rewrite strategy

- **Status:** accepted DBNext integration architecture; implementation staged; standalone-product consolidation left open on 2026-10-11
- **Scope:** vector, text search, graph, document, time-series, KV boundaries, and how the current implementation is replaced
- **Depends on:** ADR 0006 through ADR 0011

## Context

The current OmenDB implementation has proved useful semantics, crash behavior,
PostgreSQL-facing compatibility, and benchmark infrastructure, but large parts of
its storage/runtime/execution implementation are no longer the desired target.
At the same time, separate OmenDB OLAP and vector repositories explored useful
specialized engines with their own WALs, manifests, segment stores, and query
runtimes.

Using independent database authorities *inside a DBNext SQL transaction* would
recreate the state synchronization problem OmenDB can avoid. DBNext therefore
integrates modalities over its own transaction authority. This does not rule out
independent products with independent datasets or explicit asynchronous
projection contracts.

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

Derived state follows
[ADR 0013's snapshot-validity contract](0013-storage-kernel-and-access-methods.md#3-storage-objects-have-explicit-authority-classes).
A complete applied frontier alone is insufficient for an older snapshot after
values have been overwritten. Use a retained exact view or a complete delta
path; otherwise fall back to a canonical exact path.

### 2. Vector search is a type plus access methods, not a sibling database

OmenDB will support vector-valued columns as part of ordinary transactional rows.
Initial useful shapes are dense `f32`, half-precision, and later sparse or
multi-vector values when workloads justify them.

The planner chooses among vector access paths just as it chooses B-tree access:

- exact SIMD/batch scan as the correctness oracle and selective-filter path;
- a workload-appropriate derived ANN structure rather than one algorithm being
  architectural law; HNSW remains a strong resident-memory candidate;
- compact/quantized routing or candidate representations that fit faster memory
  tiers, with canonical exact values fetched for final scoring/reranking;
- filtered traversal that cooperates with scalar/payload predicates instead of
  blindly post-filtering one global index;
- NVMe-oriented graph/cluster/segment layouts when the dataset exceeds RAM;
- object-storage-native ANN only as a separately measured deployment tier, never
  by forcing an in-memory graph onto object storage.

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

The inverted index chooses physical granularity for text/filter behavior rather
than inheriting the B-tree, row, or vector partitioning scheme. Benchmark
cardinality-adaptive posting representations, independently sized posting
blocks, compressed SIMD-friendly IDs/bitmaps, and exact match output as
selection vectors that downstream operators can consume directly. Mutable and
immutable segments may coexist, but current-snapshot correctness still requires
frontier coverage or an exact delta/fallback path.

A compact generation-safe physical row/version reference is worth testing for
postings because it can remove an ID-to-row lookup and improve page locality.
It is an optimization only: logical row identity remains distinct, and no
physical reference becomes a format contract until relocation, slot/page reuse,
MVCC retention, recovery and GC prove its lifetime semantics.

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
the document use case inside tables. The long-term physical form should be a
typed binary representation rather than permanently opaque text, with path
statistics/pushdown and optional shredding of common fields when workloads
justify it. Path/value inverted indexes, existence indexes, expression indexes,
and statistics are access methods over that value. Schema-less or partially
typed tables may be supported as a relational catalog policy; they do not
require another storage engine.

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


### 8. Ordered KV is an optional facade, not the physical product boundary

ADR 0013 revises the earlier assumption that SeerDB's generic ordered-KV API is
the internal narrow waist for every OmenDB structure. Ordered KV remains useful
as the first access method and may remain a standalone compatibility/product
facade if there is real demand.

OmenDB must not add a second Redis/FoundationDB-style API merely to claim
another model, and it must not force rows, inverted indexes, vector structures,
graph projections, JSON indexes, or analytical chunks through an unnatural
ordered-byte-map representation. They share the kernel's transaction, log,
buffer, visibility, and lifetime services while choosing physical layouts suited
to their workload.

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

### Standalone-product decision remains open

The 2026-10-11 review leaves consolidation versus independent Vector/OLAP
products unresolved. This supersedes the earlier assumption of sibling archival;
no repository retirement, wholesale merge or cross-product release commitment
follows from this ADR. Keep the currently implemented product boundaries explicit
while evaluating them.

Two coherent outcomes remain:

- **Integrated DBNext modalities:** use OmenDB canonical rows, catalog and
  transaction/log authority. Reuse useful algorithms and evidence, not a sibling
  database's independent commit protocol.
- **Independent products:** each owns its durable mutations and lifecycle.
  External projections need source identity, a retained bootstrap/change
  contract, durable consumer progress and explicit freshness. Independent writes
  are not part of a shared DBNext SQL transaction.

Compare these outcomes using actual workload needs, operational cost, useful
specialist performance and maintenance capacity. Shared branding, Rust or
filesystem helpers do not settle the product choice.

### OmenDB OLAP repository

The current standalone core uses a namespace WAL, metadata-log publication,
Arrow tails and immutable Parquet segments. It is not the former SQLite-manifest
implementation, and it is not automatically a DBNext remote analytical worker.

Useful inputs to integrated HTAP include Arrow/Parquet interoperability,
streaming/spill/resource-budget lessons, segment/compaction fault cases and
mixed-workload harnesses. If reused, the integrated implementation lives in the
OmenDB workspace and shares its catalog, transaction log, snapshot validity,
runtime and optimizer. Independently writable OLAP retains its own authority
unless a separate product decision changes that contract.

### OmenDB Vector repository

The standalone retrieval engine currently owns its record store, WAL and segment
publication. That authority cannot be imported as a second commit protocol inside
DBNext.

Useful inputs include exact-search/recall oracles, competitive HNSW/quantization
kernels, filtered-search/selectivity cases, BM25/hybrid-ranking corpora, OOD and
recall/latency methodology, and segment-native ANN research. Integrated
vector/text implementations become OmenDB access methods and planner operators
over transactional tables. The independent retrieval product's future remains
open rather than implicitly replaced by that integration.

## Crate boundaries

Do not split by marketing modality. Split only where ownership, build cost or
unsafe/performance kernels justify it. A likely long-term shape is:

```text
omendb                 SQL/catalog/planner/server/product API
seerdb                  transaction/MVCC/log/buffer/lifetime kernel + ordered facade
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
