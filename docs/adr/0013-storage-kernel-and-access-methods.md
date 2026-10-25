# ADR 0013: Storage kernel and access-method boundary

- **Status:** historical design record; maintained target now in [living specification](../design/storage-kernel.md). This document retains rationale, not an independent current contract.
- **Scope:** shared transaction/storage services, physical access methods and lifetime
- **Revises:** the universal ordered-KV boundary in earlier ADRs; reviewed 2026-10-13
- **Related owners:** [ADR 0014](0014-vnext-installation-and-recovery.md) for commit/recovery,
  [ADR 0009](0009-buffered-btree-and-materialization.md) for ordered pages,
  [ADR 0010](0010-row-layout-and-column-families.md) for rows,
  [ADR 0011](0011-htap-and-analytical-representations.md) for analytics and
  [ADR 0012](0012-unified-modalities-and-rewrite-strategy.md) for product/repository boundaries

## Context

A transactional ordered KV layer can maintain relational data and indexes atomically.
[FoundationDB's layer model](https://apple.github.io/foundationdb/layer-concept.html)
is a credible alternative, not an obsolete design. OmenDB instead needs direct
control of RAM/NVMe locality, guarded typed records, batch execution and specialized
representations. Choose the shared kernel because it exposes those mechanisms
without duplicating transaction authority, not because byte APIs imply weak semantics.

## Decision

### 1. SeerDB becomes the shared transaction/storage kernel

SeerDB owns transaction identities/outcomes, snapshots, visibility, conflicts and
intents, log ordering/durability/recovery, buffer residency, storage-object identity,
physical materialization, resource admission and retention. SQL types, constraints,
query plans, BM25 and vector-distance meaning remain outside the kernel.

One runtime constructs these services and owns their failure/lifetime boundary.
Separate independently fenced component bundles are not a persistent database.

### 2. Ordered KV becomes one access method/facade, not the universal physical model

The ordered B-tree is the first access method. The public ordered-KV facade uses
it without requiring every row, posting or column chunk to be an opaque KV value.
Access methods are concrete compiled code, not a runtime plugin ABI or backend matrix.
Share primitives where useful; static dispatch and inlining remain available.

Do not replace the KV restriction with a universal page restriction. Mutable tree
nodes/rows use guarded frames; large immutable vector, posting and column extents
can use range reads, prefetch, different cache units and encoding-aware scans.
Both participate in store identity, checksums, publication, retention and resource
accounting. One transaction authority does not mean one page size, cache policy,
file layout or mandatory index algorithm.

### 3. Storage objects have explicit authority classes

**Authoritative objects** participate in durable transaction recovery before
acknowledgement: canonical rows, required primary/unique/constraint indexes and
catalog state. **Derived objects** can lag/rebuild only when the declared query
semantics remain available through valid coverage, complete deltas, an exact
fallback or an explicit approximation/staleness contract.

A published representation identifies:

- source incarnation and object/schema dependencies;
- a reader-stable generation and contiguous applied CSN frontier;
- the snapshots actually retained and readable;
- complete transactional delta intervals needed to reach a requested snapshot.

A latest-only representation at `F` does not answer every `X <= F`. A base at `B`
can answer `X > B` only with complete updates, deletes and lifecycle changes through
`X`. The minimum of unrelated latest-only frontiers does not manufacture a common
snapshot. Capture descriptors and history leases atomically; compaction cannot
invalidate an admitted view. Otherwise fall back or return an explicit unsupported
snapshot/staleness outcome.

ANN approximates ranking, not identity, predicates, deletion or authorization.
Canonical validation and necessary refill remain required. Rebuildability does not
make a constraint index safely asynchronous. Exercise mutations, old readers, lag,
rebuild replacement, compaction and schema changes, not just insert-only latest reads.
[Materialize's frontier distinction](https://materialize.com/docs/sql/explain-timestamp/)
is useful independent evidence, not a required timestamp implementation.

### 4. Access methods share transaction and page primitives

Provide transaction/object capabilities, guarded page access and immutable-extent
access, logical dependency/intents, versioned recovery effects, dirty tracking,
publication/verification/GC and explicit I/O/resource accounting. Access intent
may include random/sequential access, expected lifetime and prefetchability without
leaking device policy into SQL semantics.

Expose point operations and native resumable batches/selection vectors. Bound rows,
bytes and physical work, including invisible versions, overflow traversal and I/O.
Do not force every batch through per-record dispatch or full-value decode. Guards
cannot span output suspension; continuations preserve logical position and snapshot.
Build the smallest concrete interfaces needed by actual callers, not a speculative
universal trait hierarchy.

### 5. The first two physical structures are deliberately narrow

Qualify an ordered-tree path and a canonical-record path with secondary/constraint
indexes and catalog effects under one transaction. Compare compact clustered
primary-tree records with row/heap pages plus a primary index. The row participant
must expose real typed access and lifetime/recovery behavior, not merely rename
an opaque serializer. It may reuse the ordered node implementation; inventing a
second physical engine to prove the boundary is not a requirement.

Add analytical and search structures only after the shared kernel earns their
cost. Their eventual requirements inform interfaces, not speculative infrastructure.

### 6. MVCC is a kernel service but version layout may be access-method aware

Transaction state, logical visibility and retention are common; physical version
metadata need not have one representation everywhere. Prefer compact current
metadata and resident recent history with bounded spill/checkpoint support over
mandatory synchronous per-key history I/O. The complete recoverable-log design in
ADR 0014 must be qualified before removing the current undo barrier.

Snapshot, certification-summary, CDC, backup and checkpoint floors are distinct
retention consumers. Budget their byte/age pressure; refuse or explicitly expire
abortable work rather than silently discard correctness evidence. Never pressure-
abort an already durable decision. Logical limits follow supported semantics and
progress budgets, not incidental slot widths or an unversioned codec cap.

### 7. Recovery is one transaction outcome across all authoritative access methods

Each durable decision covers every authoritative participant. Versioned concrete
recovery records provide enough information to reconstruct their effects; unknown
kinds fail closed. No nested independently committing store or arbitrary callback
log is introduced. Derived state validates its identity/coverage and replays or
rebuilds. Whether bytes are a page or an extent does not change this authority.

### 8. Buffer translation is an explicit measured seam

Start with the sharded translation baseline and correct single-flight Loading
ownership. An exact frame incarnation prevents reuse errors but does not itself
prevent an old delayed load from being published after a newer write/eviction.
Loading, mutation, writeback and eviction must share page freshness authority.

Compare validated hints/predictive translation, swizzling or virtual mapping only
on the actual workload. Preserve one authoritative lookup/lifetime path behind
any fast hint. Measure cycles, LLC/TLB misses, page walks, bandwidth and NUMA as
well as hit rate. Do not permanently encode a translation trick into page formats
without benefit. Mutable-page, immutable-extent, history and query caches share a
global budget even when replacement policies differ.

[B-Trees Are Back](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrees-are-back.pdf),
[Predictive Translation](https://db.in.tum.de/people/sites/zinsmeister/papers/predictive-translation.pdf)
and [How to Write to SSDs](https://www.vldb.org/pvldb/vol19/p1469-lee.pdf)
justify layout/translation/placement experiments, not performance claims for SeerDB.

### 9. Physical row/version references are optional optimization hints

A locator such as `(PageId, SlotId, generation)` is not logical row identity.
Any fast reference needs movement, split/reorganization, reuse/ABA, snapshot,
checkpoint and GC rules, with a stable-handle contract or logical fallback.
Postings/secondary indexes may benefit, but no format reference is added merely
in anticipation of them.

### 10. SeerDB's independent package status is secondary to OmenDB architecture

Keep SeerDB independently versioned and Apache-2.0 in the OmenDB workspace.
Its useful KV API is a facade over the same core, not a separate roadmap or
physical implementation. OmenDB-specific AGPL dependencies must remain above the
kernel. Package independence does not freeze an alpha internal interface.
ADR 0012 owns repository and standalone-product decisions.

### 11. Snapshot/bootstrap is a retained handoff, not just two numbers

Atomically capture source incarnation, snapshot CSN, compatible catalog/layouts,
consumer start position and leases for snapshot plus change suffix. The current
`TransactionDatabase::snapshot_export` position pair is not this retained handoff.
Registration after copying begins can already have lost required history.

Keep logical consumer progress separate from physical recovery/retention floors.
A logical change stream preserves whole transaction boundaries, stable effects,
updates/deletes and enough schema/value information for its declared consumers;
physical page redo is not the public CDC format.

A consumer applies effects and progress atomically and idempotently before advancing
source retention. Splitting transport batches cannot publish half a source transaction
as complete. A destination unable to provide that atomic unit exposes a weaker
contract. Independently written destination tables do not inherit source atomicity.

Pins expose pressure and explicit release/advance/invalidation. Lost history or a
foreign incarnation requires rebootstrap/error. Retained readability and source
identity also govern optional remote search/analytical workers. PostgreSQL's
[logical decoding](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html)
and [replication-slot positions](https://www.postgresql.org/docs/18/view-pg-replication-slots.html)
are reference semantics, not a requirement to copy its slot implementation.

## Migration plan

The [vNext plan](../roadmap.md) is the sole execution sequence.
Keep the legacy engine as a fallible semantic/fault reference until qualified
cutover, then delete it and transitional adapters. Offline preview conversion
preserves supported logical data or refuses it; no stable physical upgrade or
permanent compatibility backend is implied.

## Acceptance gates

Require atomic cross-object/participant outcomes, exact supported snapshots,
bounded progress under cache/history/allocation pressure, recovery through two
reopens plus subsequent use, and matched hot/larger-than-memory performance.
Measure throughput and p50/p95/p99 under single and concurrent committers, allocations,
translation/latch/certification/frontier waits, host/device write amplification,
checkpoint pauses, recovery memory/time and unused-feature cost. Batch and optional
reference fast paths must demonstrate their actual benefit and lifetime safety.
No research citation or existing component test closes an integration gate.
