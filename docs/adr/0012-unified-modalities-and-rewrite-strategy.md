# ADR 0012: Unified modalities and rewrite strategy

- **Status:** accepted target; product and repository boundary decided 2026-10-13;
  integration and migration are not implemented by this decision
- **Scope:** integrated modalities, standalone products, repository ownership and reuse
- **Depends on:** ADRs 0006 through 0011; [ADR 0013](0013-storage-kernel-and-access-methods.md)
  owns the common storage/snapshot boundary

## Decision

Choose an **asymmetric hybrid**: integrated OmenDB for transactional relational,
vector/text and operational analytical queries; a separate general-purpose OLAP
companion for independent ingestion, bulk analytics and workload isolation.
Optional OmenDB analytical workers are also possible, but they are a projection
of OmenDB authority, not automatically the independently writable OLAP product.

This resolves the earlier open consolidation question. It does not mandate
merging all repositories or archiving working products. Product APIs, physical
representations, deployment processes and source repositories are separate choices.

### 1. One canonical transaction and catalog authority

Rows and catalog state that participate in one OmenDB transaction share one
SeerDB decision, visibility and recovery protocol. Canonical records and required
indexes are authoritative; an accelerator is derived only if losing it cannot
change committed semantics and an exact path or explicit approximation contract
remains available. Rebuildability alone is not sufficient.

Apply [ADR 0013's snapshot-validity contract](0013-storage-kernel-and-access-methods.md#3-storage-objects-have-explicit-authority-classes)
to every representation. A maximum/applied frontier is not historical readability.
Keep source identity, schema dependencies, exact snapshot coverage, generations
and retention together. Background lag must not become silently stale SQL.

### 2. Vector search is a type plus access methods, not a sibling database

Store canonical vectors as typed values in ordinary records; unrelated records
must not require a vector. Preserve the declared exact value/precision unless
an explicit lossy storage policy says otherwise. Quantized routing codes and ANN
topology are derived and never substitute for canonical identity or visibility.

Start with exact batched scoring and a resident-memory ANN candidate such as HNSW.
Choose filtered traversal, IVF/clustered layouts, SSD-oriented graphs or quantization
from selectivity, filter/vector correlation, update rate, recall, memory and I/O
measurements. An algorithm winning unfiltered in-memory SIFT is not a universal
backend. NVMe and object-backed search need their own placement/request economics.

Turbopuffer's [September 30, 2026 v3 redesign](https://turbopuffer.com/blog/rip-vector-database)
is concrete external evidence for this separation: its ANN-address-as-primary
layout amplified document/index rewrites and forced unrelated scans/aggregates
to use vector-cluster-sized blocks. Its proposed record-primary design makes
ANN a secondary index; performance qualification was still in progress when
published. OmenDB should likewise keep stable canonical row identity distinct
from relocatable ANN addresses and let posting, vector and analytical block
sizes follow their own workloads. This is not a mandate to copy Turbopuffer's
object-storage or search layouts.

Resolve candidates against the query's canonical snapshot and trusted predicates,
suppress replacements/tombstones, then refill or use a declared exact fallback
when needed. ANN approximates ranking, not deletion, eligibility or authorization.
Metadata predicates in the current standalone library are not SQL three-valued
logic or an authorization system.

Embedded, server and object-backed retrieval remain valid deployment goals. Reuse
scoring, indexing and ranking components; do not embed the current standalone
`Store`, WAL or checkpoint lifecycle inside a SQL transaction.

### 3. Full-text/BM25 search is another native access path

Text stays typed canonical data. Postings, scalar filters, vector candidates,
ranking/fusion and joins compose in typed plans. Posting/bitmap granularity follows
term distribution and selectivity, not the vector partition or B-tree page size.
Expose batch selection results and measure compressed IDs, block skipping and
physical-reference hints only with safe movement/reuse/version lifetimes.

### 4. Property graphs are catalog/query semantics over relational data

Model vertices and edges through tables and graph catalog/query semantics rather
than another independently committing store. Ordinary indexes provide an exact
fallback; snapshot-qualified adjacency or CSR-like projections are optional.
Graph traversal and specialized operators follow demonstrated workloads.

### 5. Document data is a relational type, not a document-store fork

Typed binary JSON/document values, path expressions, statistics and optional
shredding/inverted indexes share the catalog and transaction engine. Schema-less
policies do not require a second data authority.

### 6. Time-series and geospatial are specialized physical policies

Use appropriate types, partitioning, ordering, retention, compression and indexes
under the same logical semantics. No independent WAL is introduced merely for a
modality. These are extension directions, not prerequisites for the first release.

### 7. Incrementally maintained views are a first-class derived modality

Maintain views from complete transactions with explicit snapshot validity and
retention. Publication, rebuild and compaction are bounded background work.
Incremental maintenance does not imply a complete streaming/event-time language;
that requires separately specified time, correction and watermark semantics.

### 8. Ordered KV is an optional facade, not the physical product boundary

Keep a usable ordered transactional KV surface over SeerDB's first access method.
Do not build a second KV engine or a Redis/FoundationDB compatibility layer merely
to claim another model. Generic transactional KV is a valid alternative architecture;
OmenDB chooses a lower internal boundary to control specialized layout, execution
and shared lifetime, not because a KV layer cannot express relational correctness.

## Repository and rewrite strategy

### Keep the repository; replace the implementation

`omendb/omendb` is the canonical implementation workspace for the relational
server, SeerDB, durable-fs, and integrated access methods. Continue the replacement
on `storage-kernel-vnext`; do not create a fresh root repository or use a historical
coordination repository as the new product tree.

Keep useful differential, lifecycle, crash and workload evidence. Tests of known
legacy defects are not a specification. Once qualified, replace and delete the
legacy physical engine and transitional adapters rather than retain a selectable
backend. Do not merge an unqualified storage branch to publish a design change.

### OmenDB Vector repository

The current standalone repository remains the source for its existing API and
experiments until replacement consumers are qualified. Its long-term direction
is reusable retrieval components and standalone/embedded/server facades over
shared components, not a second OmenDB transaction authority.

Port a concrete useful algorithm or operator with its exact/recall/mutation oracle,
then give it one source owner in the OmenDB workspace when it serves integrated
OmenDB. Standalone callers can depend on that component. Do not copy an entire
engine, create speculative shared crates, or maintain two diverging algorithm
copies. Retire superseded implementations only after API/lifecycle migration and
qualification; repository archival is a separate action.

An independently operated projection is permitted when isolation or storage tiers
justify it, but exposes its own source identity, freshness and recovery boundary.
Shared branding does not grant cross-process SQL atomicity.

### OmenDB OLAP repository

Keep the standalone OLAP companion in its existing repository. Its independent
columnar ingestion/publication and DataFusion-based execution are useful for
analytical datasets and do not need the OLTP B-tree as a mandatory segment store.
The long-term scope remains general OLAP; the append-oriented companion contract
is an initial supported surface, not a permanent limit on analytics.

Integrated OmenDB analytics instead shares OmenDB catalog, transactions and typed
execution, as specified in [ADR 0011](0011-htap-and-analytical-representations.md).
Reuse compatible encoded-array, Arrow/Parquet, pruning, compaction and resource
techniques through concrete boundaries. Do not import a second SQL binder or
independent manifest/WAL into an OmenDB transaction. Share packages only after an
actual common implementation exists; a common language or filesystem helper is
not sufficient justification.

An OLAP companion consuming OmenDB changes needs an atomically retained bootstrap,
transactional update/delete/schema application, durable consumer progress and
explicit query freshness. Append-only ingestion or an insert-only CDC fixture
is not that contract. Independently written OLAP tables remain independent data.

### Package and information ownership

SeerDB and durable-fs remain Apache-2.0 crates in the OmenDB workspace. OmenDB and
its AGPL components must not become dependencies of the generic kernel. Preserve
existing license boundaries; do not infer a relicense from code movement.

Use small concrete modules first. Extract workspace crates for real ownership,
reuse or build-cost benefits, not one crate per marketing modality. Feature-disabled
search/analytics must not allocate indexes, create background workers or add a
material write-path cost. Separate executables may use the same components.

Historical standalone SeerDB is not a second writable implementation. Private
research/coordination repositories are not product source or competing roadmaps.
Preserve private visibility: no private OLAP source, experiments or notes move to
a public repository without an explicit publication decision. This architecture
change neither publishes packages nor relocates private source.

## Acceptance principle

Integrated functionality must prove transactional composability, useful specialist
performance on its intended workload and no material unused-feature tax. Remote
or standalone deployment must additionally expose its consistency and recovery
boundary. Benchmark local integration against isolation rather than assuming
one process always wins or a network hop is always necessary.

The [vNext plan](../plans/storage-kernel-vnext.md) owns implementation order.
Current standalone contracts remain in their own repositories until migrated;
this ADR is not evidence that a port, release or consolidation has happened.
