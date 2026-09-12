# ADR 0009: Buffered B-tree state and asynchronous physical materialization

- **Status:** accepted target architecture; replaces whole-tree/per-generation
  copy-on-write as the long-term OLTP update model
- **Scope:** SeerDB B-tree concurrency, buffer residency, dirty-page handling,
  page mapping, and checkpoint materialization
- **Depends on:** [ADR 0002](0002-seerdb-mvcc-version-storage.md),
  [ADR 0003](0003-seerdb-commit-recovery-state-machine.md), and
  [ADR 0006](0006-deployment-storage-and-durability.md)

## Context

SeerDB's current implementation proved crash-safe out-of-place generations, but
its publication path still couples logical commits to generation/page
materialization. The benchmark suite now measures the resulting cost directly:
publication performs physical flush work before the authority frame, and the
transaction layer can clone/cascade candidate B-tree state per publication
wave.

That is a useful correctness implementation but not the target for a
high-throughput OLTP engine on modern RAM + NVMe.

Recent LeanStore work demonstrates a more useful distinction: **B-tree pages can
be ordinary mutable buffered data structures in RAM while persistence still
writes those pages out-of-place to SSD**. Out-of-place persistence does not
require transactional copy-on-write of the whole tree.

OmenDB already has the semantic prerequisite for this separation: logical MVCC
versions and transaction status are distinct from physical page versions.

## Decision

### 1. The shared B-tree is mutable buffered state

The target local engine maintains one shared logical B-tree (per SeerDB tree)
whose hot nodes live in buffer frames. Transactions do not clone the complete
B-tree or a complete root candidate merely to update a key.

A page/frame may receive many logical updates while resident. Dirty state is
tracked explicitly and can be written later. The buffer manager owns residency
and eviction; the transaction manager owns logical visibility.

### 2. Transaction visibility is MVCC/status-based, not page-root-based

A current record can identify an owning `TxnId` or a frozen committed CSN as
specified by ADR 0002. Installing a record into a shared buffered page does not
make it visible merely because the page is reachable.

Conceptually:

```text
shared B-tree page
  key -> current record(owner = TxnId, undo = VersionId, value = ...)
                         |
                         +--> transaction status
                                 Active      -> invisible to others
                                 Aborted     -> follow undo / absence
                                 Committed N -> visible according to snapshot
```

This permits logical installation and B-tree maintenance to be decoupled from
the durable commit decision. Structural changes such as page split/merge are
physical/index maintenance and must not themselves imply row visibility.

### 3. The durable log contains enough information to recover logical state

Before a synchronous commit is acknowledged, the selected durability mechanism
must contain all redo/change/decision information necessary to reconstruct the
transaction's complete logical effect after a crash.

Dirty pages are therefore a cache/materialization of committed logical state,
not the sole durable copy of that state.

Recovery is:

```text
load latest valid checkpoint/page map
        |
replay committed log after checkpoint
        |
resolve transaction status / MVCC records
        |
accept traffic once logical frontier is complete
```

A missing post-commit page write is recovery work, not an ambiguous transaction
outcome.

### 4. Dirty pages flush out-of-place

When the buffer manager chooses to persist a dirty page, it writes a new
physical image to an append/placement region. The durable page mapping is
advanced only after the image is valid. Old physical locations remain readable
until the checkpoint/retention rules prove they are reclaimable.

This keeps the SSD advantages of SeerDB's current direction:

- no small random overwrite requirement;
- sequential/batched page writes;
- page compression and packing;
- grouping by expected lifetime/deathtime;
- efficient garbage collection;
- future ZNS/FDP placement;
- crash-safe old-or-new page mapping.

It also coalesces multiple logical updates to the same hot page into fewer
physical writes.

### 5. Page concurrency is fine-grained and optimistic on the read path

The target B-tree uses page/node-local synchronization rather than a tree-global
writer mutex. The first implementation should evaluate an optimistic-lock-
coupling style guard similar to modern LeanStore:

- readers sample a page version, inspect without exclusive ownership, and
  validate the version before trusting the result;
- writers acquire the page's write latch/version lock for local mutation;
- traversal validates parent/child transitions and retries only the affected
  operation on concurrent structural change;
- split/merge protocols have explicit lock ordering and invariants.

Rust ownership must not force long-lived `&mut` access to the entire tree merely
because a page is being changed. Page/frame guards are the safety boundary.

Other concurrency algorithms (B-link variants, Bw-tree-like indirection, etc.)
remain benchmark candidates, but they must beat the simpler OLC baseline on
mixed cached/out-of-memory workloads.

### 6. Buffer translation is a measured hot-path component

The page identity -> resident frame lookup is expected to become visible in CPU
profiles as NVMe and cache hit rates improve. The current mapping is therefore
not format/API law.

Benchmark at least:

- direct indexed frame tables where ID density permits them;
- optimized hash translation;
- pointer swizzling / tagged references;
- virtual-memory-assisted approaches;
- Predictive Translation-style deterministic placement.

Prefer the simplest portable mechanism within measurement noise of the fastest
one. Any swizzled/virtual address is process-local cache state and never enters
durable page formats.

### 7. Page/node size is not fixed by the transaction architecture

The current 4 KiB page is an alpha format choice. Before format stability,
benchmark 4/8/16 KiB and adaptive/packed variants for:

- point lookup/update;
- variable-sized rows/keys;
- scan bandwidth;
- split frequency;
- buffer hit rate;
- physical and device write amplification;
- compression/packing efficiency.

The B-tree node layout should incorporate the modern variable-record techniques
from *B-Trees Are Back* rather than preserving today's representation for
compatibility.

### 8. Small values favor clustered locality; large/cold values may be separated

Primary/secondary ordered access benefits from keeping small records close to
keys in leaf pages. Large values, infrequently read columns/objects, and large
MVCC history should use separate append-oriented storage when the extra
indirection reduces cache/write amplification.

The current blob threshold is therefore a policy to benchmark rather than one
universal constant. Future storage-temperature hints may alter placement without
changing logical rows or index keys.

### 9. Checkpoints bound recovery, they do not define every commit

A checkpoint durably captures a consistent physical mapping/root and the log
position it includes. Checkpoint cadence is chosen from recovery-time, write
amplification, cache pressure, and archival requirements.

Object-storage/disaggregated deployments may upload immutable checkpoint
segments asynchronously. Local mode may keep only local checkpoint generations
plus optional archival backups.

## Migration strategy

Do not rewrite the current storage layer all at once. The replacement vertical
slice should establish:

1. a shared buffered page/frame API with explicit page guards;
2. one transactional tree using logical MVCC visibility independent of root
   generation;
3. WAL-authoritative commit/recovery for that tree;
4. out-of-place dirty-page flush plus durable mapping checkpoint;
5. fault tests at log, page-image, mapping, checkpoint, and GC boundaries;
6. side-by-side benchmarks against the current generation implementation;
7. only then move catalog/remaining trees and delete the old path.

There is no storage-format compatibility obligation before the declared stable
format. An experimental implementation should use a new format version and fail
closed on old bytes.

## Acceptance gates

- point updates do not clone the complete B-tree/root candidate;
- disjoint page/key writers progress concurrently;
- committed state survives when the process is killed after log durability but
  before any corresponding page flush;
- aborted/uncommitted records installed in buffered pages are never visible to
  another snapshot and are safely reclaimable;
- repeated updates of one hot page can be coalesced into fewer physical writes;
- reader traversal stays lock-light and correct during split/merge;
- buffer translation, page latch, allocator and WAL costs are individually
  visible in profiling;
- larger-than-memory random read and TPC-C-style runs report p99 plus device
  write amplification;
- recovery time remains bounded by checkpoint/log policy;
- process-level fault tests reopen at least twice and verify the logical model.

## Research inputs

- LeanStore — pointer swizzling/VMCache, optimistic lock coupling, scalable SI;
- *B-Trees Are Back* (SIGMOD 2025) — optimized pageable variable-record nodes;
- *How to Write to SSDs* (VLDB 2026) — out-of-place page persistence,
  compression/packing, lifetime grouping, ZNS/FDP;
- *Predictive Translation* (SIGMOD 2026) — low-overhead page translation;
- FASTER/Garnet — hot-memory/update locality and memory/storage tiering, as a
  contrasting record/log design rather than an ordered-index replacement.

## Consequences

- SeerDB keeps the B-tree as its ordered local access structure but stops using
  whole-tree copy-on-write as its transaction mechanism.
- Logical MVCC and physical persistence finally have independent lifecycles, as
  ADR 0002 intended.
- Commit latency can converge toward log durability rather than page durability.
- Hot pages can be updated at memory speed and persisted efficiently later.
- Physical GC/checkpoint complexity increases, but it is isolated below the
  transaction contract and justified by both current profiling and modern SSD
  behavior.
