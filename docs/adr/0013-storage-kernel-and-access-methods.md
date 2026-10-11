# ADR 0013: Storage kernel and access-method boundary

- **Status:** accepted target architecture; ordered kernel baseline implemented on `storage-kernel-vnext`, persistent recovery and relational cutover pending
- **Scope:** SeerDB/OmenDB physical boundary, transaction/storage kernel, access methods, recovery participation, and multimodal derived state
- **Depends on:** [ADR 0001](0001-seerdb-transaction-contract.md), [ADR 0003](0003-seerdb-commit-recovery-state-machine.md), [ADR 0006](0006-deployment-storage-and-durability.md), [ADR 0009](0009-buffered-btree-and-materialization.md), [ADR 0011](0011-htap-and-analytical-representations.md), and [ADR 0012](0012-unified-modalities-and-rewrite-strategy.md)
- **Revises:** the earlier assumption that `TreeId + ordered key bytes + opaque value bytes` is the only OmenDB↔SeerDB narrow waist, including target-boundary language in ADRs 0005, 0006, 0010 and 0012

## Context

The existing SeerDB implementation proved important correctness properties: explicit `TxnId`/CSN/LSN identities, crash-safe transaction decisions, ordered access, snapshots, version retention, change positions, and fault qualification. Those semantics remain valuable.

Its current product abstraction, however, was designed as a generic transactional ordered-KV engine. OmenDB maps rows, catalogs, scalar secondary indexes, constraints, and every other durable relational structure into ordered byte trees. That boundary resembles FoundationDB's successful layer model and is excellent when the primary goal is a general distributed KV substrate.

OmenDB's target is different. It must optimize one integrated database for:

- low-overhead OLTP on RAM + NVMe;
- compact row storage and scalar secondary indexes;
- vector, text, graph, JSON and future specialized access paths;
- derived columnar/HTAP representations;
- local, HA and later distributed deployment profiles;
- one transaction/log authority across all authoritative state.

Modern high-performance systems such as Umbra/CedarDB integrate buffer management, MVCC, page/index structures, and physical representations more tightly than an opaque value boundary permits. A generic KV facade remains useful, but it must not force every storage structure to pretend it is an ordered byte map.

This ADR therefore keeps the semantic core that SeerDB established while lowering the internal narrow waist.

## Decision

### 1. SeerDB becomes the shared transaction/storage kernel

`seerdb` remains the working crate/name during the rewrite, but its architectural role changes. It is no longer defined as "the generic ordered-KV database below OmenDB." It becomes the shared kernel that owns transaction state, durability, buffer/page management, recovery and physical resource lifecycle.

The kernel owns at least:

```text
transaction manager
  TxnId / CSN / LSN
  snapshots and isolation state
  transaction status
  conflict/dependency tracking
  adaptive write-intent wait state

log + recovery
  ordered transaction records
  durable commit decisions
  durability scheduler/transport
  recovery replay
  checkpoint frontier
  generic committed-change framing

buffer + storage
  PageId / frame guards
  page-to-frame translation
  dirty tracking
  eviction/admission
  NUMA / memory-tier placement
  async reads/writes
  out-of-place NVMe placement
  page-map/checkpoint publication
  physical GC and retention

version / lifetime services
  logical version identities where needed
  snapshot retention
  WAL/log retention
  storage-object lifecycle
```

None of these layers acquire SQL table, column, NULL, vector, BM25, graph, or query-planner semantics.

### 2. Ordered KV becomes one access method/facade, not the universal physical model

The kernel supports storage objects implemented by compile-time access methods. The first authoritative access method remains an ordered B-tree. The existing `TransactionDatabase`/ordered-KV surface may survive as a compatibility and standalone facade implemented on that access method.

Conceptually:

```text
                       OmenDB
                         |
              transaction/storage kernel
             /        |        |        \
       ordered      row/     inverted   derived
       B-tree       record     index     structures
          |           |          |          |
          +-----------+----------+----------+
                         |
                buffer / log / MVCC
```

The kernel does **not** grow a runtime plugin ABI or a RocksDB-style backend matrix. Initial access methods are compiled with the database so hot paths can use static dispatch, concrete layouts and inlining. Modularity exists to permit distinct physical structures to share one transaction/durability substrate, not to support arbitrary third-party binary plugins.

### 3. Storage objects have explicit authority classes

A storage object declares whether it is **authoritative** or **derived**.

**Authoritative objects** participate in the transaction's durable recovery record before acknowledgement. Examples include canonical row families, primary indexes required to locate rows, unique/constraint indexes, catalog objects and other state whose loss would change committed SQL semantics.

**Derived objects** may lag, rebuild or catch up from the committed-change stream. Examples may include ANN graphs, some BM25 acceleration structures, columnar analytical chunks, graph adjacency projections, zone maps and optional materialized search structures.

Their publication descriptor must establish:

- source store incarnation and storage-object/schema dependencies;
- one immutable or otherwise reader-stable representation generation;
- the contiguous applied CSN frontier, not merely the largest observed CSN;
- which snapshots remain readable after overwrite or compaction;
- any complete delta interval needed to reach the query snapshot.

An applied frontier `F` alone does not permit reads at every `X <= F`. A
latest-only representation supports its exact published snapshot, not arbitrary
older snapshots. A versioned representation may declare a retained interval
`[oldest_readable, F]` only if it can resolve each snapshot in that interval
without exposing later changes. A base at `B` may serve a later `X` only with
complete transaction-aware deltas `(B, X]`, including updates and deletes.
Otherwise use authoritative data or an explicitly requested stale-read contract.

Capture the representation descriptor and required delta/history leases as one
reader-stable view. Validate source identity and schema compatibility before
use. Compaction/rebuild/GC may not invalidate that captured view. Canonical
snapshot validation must still enforce row identity, deletions and authorization;
ANN approximation is a ranking contract, not permission to weaken those checks.

A feature is not automatically derived merely because rebuilding is possible.
If current-snapshot SQL semantics or a declared constraint requires the structure
synchronously, make it authoritative or provide an exact delta/fallback path.
The qualification oracle must exercise update/delete, old snapshots, lag,
compaction, rebuild replacement and incompatible schema—not just insert-only
latest-snapshot reads.

Materialize's [read/write frontier distinction](https://materialize.com/docs/sql/explain-timestamp/)
is independent evidence for separating retained readability from completeness;
its timestamp representation is not an OmenDB implementation requirement.

### 4. Access methods share transaction and page primitives

An access method receives transaction context and storage-object identity and can:

- pin/allocate pages through frame guards;
- perform optimistic or exclusive page/node coordination;
- register logical read/write dependencies and intents;
- stage redo/recovery information through the transaction log;
- mark pages dirty without forcing them durable before transaction acknowledgement;
- expose direct point operations plus resumable **batch-first** scans/selection vectors so vectorization, compressed decode and prefetch are not hidden behind mandatory per-item dispatch;
- surface access/I/O intent where useful (random/sequential, expected lifetime, prefetchability) without leaking device policy into SQL semantics;
- participate in checkpoint, verification and physical GC;
- report memory/I/O/write-amplification metrics.

The exact Rust trait/API is intentionally not fixed by this ADR. The first implementation should use concrete types and the smallest common interfaces required by the first two access methods rather than designing a speculative universal trait hierarchy.

### 5. The first two physical structures are deliberately narrow

The vNext vertical slice implements only enough structure to prove the new boundary:

1. **Buffered ordered B-tree** for ordered key/range access, scalar primary/secondary indexes and the standalone ordered-KV facade.
2. **Canonical row-record path** using ADR 0010's schema-driven compact row-family format. The implementation will benchmark clustered primary-B-tree rows against row pages/heap + primary index rather than permanently selecting one from convention.

Vector ANN, inverted/BM25, graph adjacency and analytical chunks come later, after the transaction/log/buffer kernel is qualified. Their future requirements inform the kernel seams now but do not justify speculative code before the core is measured.

### 6. MVCC is a kernel service but version layout may be access-method aware

Transaction identity, commit state, visibility rules and snapshot retention are global kernel semantics. The physical location of version information need not be uniform across every storage object.

For small OLTP row/index mutations, the target should benchmark an Umbra-like memory-optimized path in which common version metadata stays memory-resident and durability comes from the log, against the current persistent per-key undo/version representation. Large transactions and recovery still need a bounded persistent fallback.

Therefore ADR 0002's logical visibility contract remains, but its exact persistent version-store layout is no longer presumed optimal for every access method.

### 7. Recovery is one transaction outcome across all authoritative access methods

A transaction's durable decision covers every authoritative mutation regardless of physical access method. The log framing must identify enough information to replay each mutation safely after a crash.

The first implementation should prefer a small set of kernel recovery primitives over arbitrary callback-driven log records. If an access method needs specialized physiological redo, it uses a versioned compile-time record kind owned by that module. Unknown record kinds fail closed.

Derived structures recover by validating their frontier and either replaying committed changes or rebuilding from authoritative state.

### 8. Buffer translation is an explicit measured seam

No single translation strategy is locked in. Recent systems and 2026 work show pointer swizzling, virtual-memory mapping, validated hints, predictive translation, sharded hash tables and direct arrays win in different regimes.

The vNext buffer manager therefore isolates:

```text
PageId -> guarded resident frame
```

from the B-tree and row layout closely enough to benchmark at least:

- sharded/optimistic hash translation baseline;
- predictive/validated fast path;
- pointer/hint-assisted tree path where justified;
- relation/object-local placement for sequential scans;
- NUMA/tier-aware placement.

The chosen default may vary by deployment profile, but page formats should not be permanently polluted by one translation trick without measured benefit.

Residency correctness precedes choosing a faster translation strategy. Loading
ownership must serialize publication for a `PageKey` across I/O, mutation,
writeback and eviction; checking only for a currently resident winner permits
a delayed obsolete image to reappear. Exact frame incarnation protects frame
reuse but does not by itself establish logical page-load freshness. The baseline
now claims an exact Loading frame before device reads and wakes contenders on
completion or cleanup. Independent read-then-publish duplicate loading is removed;
the plan owns further residency and persistence qualification.

Translation/residency experiments must measure more than hit rate: LLC misses,
TLB/page walks, memory bandwidth and NUMA movement can dominate very large DRAM
caches. Huge pages, object-local arenas and tier-aware placement are therefore
measured policies rather than permanent format requirements.

### 9. Physical row/version references are optional optimization hints

Specialized access methods may benefit from a compact reference that maps a hit
directly to resident/persisted row state, conceptually something like
`(PageId, SlotId, generation)` or another access-method-specific locator.

Such a reference is **not** logical row identity and cannot become correctness
authority by convenience. Before any format commits to it, experiments must
define row movement, page/slot reuse, ABA protection, split/reorganization,
MVCC-version lifetime, checkpoint/recovery validation, and GC invalidation.
Every consumer needs either stable-handle semantics or a logical-identity
fallback when the hint is stale.

This seam is especially interesting for inverted indexes, graph adjacency and
secondary-index payloads, but no physical reference is added to the first vNext
formats merely to anticipate them.

### 10. SeerDB's independent package status is secondary to OmenDB architecture

The Apache-2.0 crate may remain independently publishable if useful, but package independence is not allowed to impose an artificial optimization boundary. Internal APIs may change freely during the alpha rewrite.

If a clean separation later emerges, the generic ordered-KV facade can remain a public SeerDB product over the shared kernel. If not, `seerdb` simply remains OmenDB's reusable storage-kernel crate. The database architecture, not branding or historical package structure, decides the boundary.

### 11. Snapshot/bootstrap is a retained handoff, not just two numbers

The target `snapshot CSN X + restart LSN Y` contract includes source incarnation,
a live retained snapshot, the corresponding committed-change suffix and its
retention ownership. Register the pins under the same authority that establishes
the cut; copying data and acquiring a lease later can race reclamation.

The current ordered-KV `TransactionDatabase::snapshot_export` returns a durable
position only. It does not implement this complete bootstrap handoff. No external
consumer may infer a pinned snapshot or gap-free stream from that return value.

Keep separate:

- the snapshot's logical CSN;
- the consumer cursor after the exported cut;
- the physical WAL/undo/catalog recovery floors required to reproduce changes;
- the consumer's last durably applied transaction.

A physical recovery floor may precede the consumer cursor. Names and APIs must
not imply that one LSN grants all four properties. The eventual logical change
format carries transaction boundaries, stable effect identity and enough
historical values/schema information for its declared consumer contract; it does
not expose physical page redo as the public CDC API.

A consumer applies complete source transactions idempotently and commits its
progress with the resulting destination effects before acknowledging retention
advancement. Chunking a large transaction must not publish a partial source
transaction as fully covered. Replayed delivery is permitted; duplicate effects
are not. If the destination cannot provide that atomic unit, expose the weaker
integration contract explicitly instead of calling it shared SQL atomicity.

Pins need observable byte/age pressure and explicit release, advancement and
invalidation semantics. Lost history or a mismatched source incarnation requires
an explicit rebootstrap/error, never silent continuation. Exact API and retention
policy remain implementation choices; they must be qualified before backup,
CDC or remote projection uses this handoff.

Primary reference: PostgreSQL 18 documents
[exported snapshots, crash replay and slot retention](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html),
and distinguishes [physical restart retention from consumer progress](https://www.postgresql.org/docs/18/view-pg-replication-slots.html).
These support the boundary semantics, not a requirement to copy PostgreSQL's
slot implementation. References checked 2026-10-11.

## Migration plan

The rewrite proceeds alongside the current implementation only long enough to preserve a continuously testable oracle:

1. Add a vNext kernel namespace/module on `storage-kernel-vnext` with IDs, transaction state machine interfaces, log framing, page/frame guard types and storage-object identities.
2. Implement the buffered ordered B-tree over the new buffer manager without OmenDB SQL integration.
3. Implement log-authoritative commit/recovery and qualify it with SeerDB's existing crash/fault matrix.
4. Add the compact canonical row path and enough scalar index support to run OmenDB's existing relational differential tests.
5. Run old-vs-new semantic differential tests plus YCSB/TPC-C-shaped performance and write-amplification measurements.
6. Cut OmenDB over once correctness gates pass and the new path has no material regression in its intended regimes.
7. Delete the generation-COW/current transactional implementation rather than maintaining two engines.
8. Port vector/text/analytical access methods only onto the new kernel.

The compatibility facade must never cause OmenDB to keep the old physical architecture alive.

## Acceptance gates

Before the replacement becomes mainline it must demonstrate:

- the full current transaction/crash/fault oracle;
- atomic transactions spanning more than one authoritative storage object/access method;
- current-snapshot correctness when derived structures lag or are absent;
- hot-set performance close to an in-memory path;
- graceful larger-than-memory degradation;
- measured page-translation overhead;
- p50/p95/p99 and throughput under 1/4/16+ committers;
- allocation count/bytes and CPU profile;
- batch/selection-vector throughput versus row-at-a-time adapters for scan-heavy paths, with compiler/SIMD evidence where relevant;
- cache/LLC/TLB/page-walk and memory-bandwidth evidence for large-buffer workloads where available;
- logical, host and flash write amplification;
- recovery time versus checkpoint/log distance;
- no global database lock on ordinary reads/writes;
- any adopted physical record-reference fast path proves stale/reused references cannot change query correctness across movement, recovery or GC;
- x86-64 Linux/NVMe and AArch64 coverage where practical.

## Consequences

- Current SeerDB is a correctness/reference implementation, **not** the architecture to incrementally polish into permanence.
- Ordered B-trees remain a central first-class access method, but "everything is ordered KV" is no longer an architectural constraint.
- Vector, text, graph and HTAP work can share buffer/log/transaction infrastructure without each inventing a sibling database or forcing unnatural KV encodings.
- We preserve the strongest existing semantics while giving the rewrite freedom to adopt memory-optimized MVCC, modern buffer translation, autonomous/adaptive commit and specialized physical structures where benchmarks justify them.
