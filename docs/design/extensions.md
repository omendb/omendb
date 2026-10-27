# Specialized engines and transactional projections

**Maintained interface direction, 2026-10-25; not an implemented integration.**
OmenDB's [product architecture](../architecture.md) and [relational design](relational-engine.md)
own SQL semantics. [SeerDB](../../crates/seerdb/docs/architecture.md) owns
shared transaction/storage authority. [Distribution](distribution.md) is a
different concern; [OmenDB's roadmap](../roadmap.md) owns delivery.


## Multi-engine architecture

OmenDB should eventually support dedicated physical **engines** for relational
records/ordered indexes, text/inverted indexes and BM25, vector/ANN, columnar
analytics, and later graph/time-series/geospatial access when warranted.

Each engine may choose its page/extent layout, index algorithm, encoding,
compression, prefetch and cache units. It consumes OmenDB typed semantics
where needed and SeerDB transaction/object/snapshot/retention services when
participating in the same authoritative data. The shared layer is **commit
and visibility authority**, not a mandatory B-tree, byte KV facade, fixed
page size or polymorphic backend-plugin ABI.

Required primary/constraint indexes are synchronous participants.
Rebuildable search/column indexes are derived only with:
source incarnation and schema dependencies; reader-stable generation;
exact snapshots actually retained; complete mutation/delete/schema
deltas and lease closure; or an explicit fallback/staleness/approximation.
An index at frontier F cannot automatically serve *every* snapshot ≤ F.
ANN may approximate ranking, not identity, eligibility or authorization.
Synchronous search maintenance, bounded exact delta overlays and
asynchronous derived indexes are choices to benchmark under churn and
selective filtering, not architectural dogma.

Existing [OmenDB Vector](https://github.com/omendb/omendb-vector) and
the private independently ingested OLAP engine remain separate products
with their own qualified WAL/catalog when necessary. Shared algorithms
should have one source owner when real consumers need them. Do not embed
an independently committing Store inside a SQL transaction, force OLAP
through SeerDB's row B-tree or publish/merge private code merely to
make the product diagram tidy. A projection across engines/processes
advertises its own durability, lag and exactness.

## Batched composition across specialized engines

Text/posting, vector/ANN, columnar, filter, rank, top-k, aggregation and
projection access paths should compose through an **OmenDB-owned typed
batch/selection interface** when they share canonical relational data.
Keep logical candidate identity, score, validity, snapshot, index generation,
deletion and eligibility intact between operators. Evaluate selective
predicates before wide payload materialization. Query-dependent thresholds
may prune ranked candidates only with safe upper bounds on the *complete*
score expression and correct top-k refill.

This does not mandate one physical index, batch size, generic plugin ABI or
pipeline for cheap OLTP point queries. Block-max WAND versus vectorized
MAXSCORE and HNSW versus cluster-scanning ANN are locality/pruning
alternatives to test under realistic filtered, correlated, long-query and
storage-backed workloads. SeerDB owns reusable storage batches; BM25,
ANN, SQL, fusion and authorization stay with their respective engine
owners. Standalone Vector/OLAP may reuse algorithms without sharing
OmenDB's commit authority.

## CDC and projection contract

Atomically retain source incarnation, compatible schema, snapshot CSN,
restart log position and snapshot/change leases *before copying*.
Deliver committed transactions with complete effects, deletes, schema
and object lifecycle; apply consumer effects **and progress** atomically
and idempotently. Consumer advancement releases source history only
when safe. A remote projection without coverage cannot silently serve
the exact current SQL snapshot; wait, fall back, rebootstrap or return
declared staleness. Reuse this mechanism for online index rebuild and
schema rewrite where it matches actual semantics.

Capture a representation's generation, exact snapshot coverage and required
history/delta leases atomically. Rebuild/compaction replacement cannot invalidate
an admitted view. Joins and fused searches need one supported snapshot across all
inputs; taking the minimum of unrelated latest-only frontiers does not create it.
Canonical validation removes wrong candidates but cannot alone supply omitted
updates/deletes or missing coverage. Refill, complete delta application or exact
fallback must satisfy the declared query contract; ANN recall/ranking approximation
is separate from predicate, deletion and authorization correctness.

Keep logical consumer progress separate from physical recovery/retention floors.
Transport may split a large transaction, but consumers expose it as complete only
after atomic application of all its effects and progress, with bounded staging/spill.
Logical changes carry the schema/value information their consumers require; raw
page redo is not a public CDC format. Foreign source incarnation, expired leases or
lost suffix coverage requires explicit rebootstrap/error, not best-effort resume.

## Qualification and ownership

The first SQL server must work correctly without these extensions. Add one
physical engine or operator only when a concrete product workload requires
it and its authoritative/derived state can meet exact snapshot correctness,
maintenance cost and recovery under churn. A derived index that cannot
serve its requested snapshot must wait, apply a complete delta, use a
documented exact alternative or reject with an explicit freshness result.
Do not accept silent stale SQL or a full-table fallback as economically
adequate without a measured workload.

Standalone Vector and OLAP retain independent development and release
boundaries for independently ingested datasets; port shared algorithms into
one maintained source owner when real integrated consumers need them.
No repository merge, code publication or independent-WAL integration is
authorized by this design. Defer generalized backend/plugin hierarchies.

