# ADR 0009: Buffered B-tree state and asynchronous physical materialization

- **Status:** historical design record; maintained target now in [living specification](../design/storage-kernel.md). This document retains rationale, not an independent current contract.
- **Refined by:** [ADR 0014](0014-vnext-installation-and-recovery.md) supersedes pre-decision shared installation for vNext; the buffer/materialization separation remains
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

Logical visibility and B-tree maintenance remain separate. In vNext, shared
installation follows durable decision authority under ADR 0014; an Active owner
is not permission to install speculatively. Structural split/merge does not imply
row visibility, and logical effect identity does not prove parent/root completion.

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

Modern variable-record layouts are experiment inputs, not required algorithms.
Preserve no layout solely for experimental-format compatibility. Production page
size and inline thresholds remain measurement-gated.

### 8. Bound physical ordered cells; overflow logical keys and values

Page fit is not a logical record-size contract. Every supported ordered key and
value must remain representable through fenced leaf splits, internal routing,
root creation, replacement and replay. Empty-leaf fit does not prove this.

Use small inline records for locality and immutable, object-local logical overflow
for larger keys and complete encoded values. Keys, high fences and separators use
one bounded descriptor representation: an inline key or cached prefix plus logical
chain reference. Values similarly use inline bytes or a logical reference. Never
reuse legacy physical `BlobPointer`s as logical identity. WAL, MVCC equality and
undo remain logical; replay may choose different physical chains.

For page size `P`, derive bounds from the **actual codec**: header/reserved bytes,
maximum fence descriptor, slots, tags, length fields and references. Bound complete
leaf cells so a worst-case fenced page can hold at least two, and preserve useful
internal fanout with nonempty separator sets on both split sides. A proposed v5
encoding with 33-byte key descriptors yields a 62-byte pointer leaf cell and a
53-byte separator cell; its arithmetic is an implementation candidate, not a
qualified minimum page size or stable format. Alignments and extra fields must be
included before admission accepts that configuration.

Build complete left/right fenced candidates for each feasible pivot **before**
choosing byte balance or publishing. Preserve the inherited right fence/link;
prebuild or reserve all propagation/root work under structural coordination.
Shortened separators may improve density but cannot alone bound arbitrarily long
common prefixes. Oversized logical records use overflow, not silently reduced
supported sizes. Failure before publication preserves the prior logical record.

Overflow chains have typed pages, stable logical PageIds, declared total length,
validated chunk offsets/lengths and exact terminal links. Build tail-to-head after
required WAL/undo durability, attaching dependencies before each dirty page can
become resident. Allocate from the owning object's namespace/high-water authority.
Validate head, kind, store/object identity, successor sequence and termination;
bound decoding memory, visited work and I/O. Checksums/store binding stay in the
existing image envelope rather than acquiring a second page authority.

Compare logical key bytes, never tags, pointer bytes or padded cached prefixes.
Resolve ambiguous prefixes outside tree byte latches and long-lived pins. Writers
capture a page version/descriptor set, resolve needed keys, then revalidate before
publication; splits move bounded descriptors rather than expanding all payloads.
Reader/installer leases keep referenced chains alive. Repeated retained completed
effects allocate nothing. Checkpoint closure includes leaf/fence/separator/value
references, successors and allocation high-water marks.

The first overflow baseline stores the complete encoded MVCC value. It is simple
but may fetch a large chain merely to inspect visibility metadata; compare inline
MVCC metadata plus immutable payload objects before format stability/cutover if
history-read cost warrants it. That alternative requires explicit history and
payload liveness, not another hidden arena. Ordered overflow does not select the
canonical-row access method or imply per-column value separation in ADR 0010.

No PageId reuse or chain GC is qualified. Abandoned allocations need declared
accounting and recovery retry-space policy under ADR 0014. Bounded cells prove
representability, **not** buffer/allocation progress: reserve chain, split, scratch
and installer capacity before a possible decision. Logical limits must also close
under future undo conversion; overflow cannot bypass WAL/undo or memory bounds.

Independent references, checked 2026-10-11:
[SQLite overflow and index fanout](https://www.sqlite.org/fileformat2.html#b_tree_pages)
demonstrate bounded local payload; [PostgreSQL B-tree limits](https://www.postgresql.org/docs/current/btree.html)
show an index-entry limit is distinct from general row storage. Neither system's
thresholds specify SeerDB.

### 9. Checkpoints bound recovery, they do not define every commit

A checkpoint durably captures a consistent physical mapping/root and the log
position it includes. Checkpoint cadence is chosen from recovery-time, write
amplification, cache pressure, and archival requirements.

Object-storage/disaggregated deployments may upload immutable checkpoint
segments asynchronously. Local mode may keep only local checkpoint generations
plus optional archival backups.

## Migration strategy

The [vNext plan](../roadmap.md) owns the replacement sequence.
Keep the legacy engine only as a semantic/fault reference until qualified cutover;
known legacy defects are not expected behavior. New experimental layouts use new
versions and fail closed on old bytes, with explicit offline conversion where
supported data must be retained.

## Acceptance gates

- point updates do not clone the complete B-tree/root candidate;
- disjoint page/key writers progress concurrently;
- committed state survives when the process is killed after log durability but
  before any corresponding page flush;
- aborted/uncommitted records installed in buffered pages are never visible to
  another snapshot and are safely reclaimable;
- admitted keys/values fit actual fenced leaf/internal/root layouts and replay,
  including common-prefix overflow, inline/overflow replacement and deletion;
- chain corruption/missing references fail closed, retained readers survive
  replacement, and failure-atomic publication preserves the old logical value;
- reserved installer capacity finishes structural obligations before readiness;
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
