# ADR 0014: vNext installation, replay and checkpoint boundaries

- **Status:** accepted integration contract; upsert/write-set/intent/install primitives implemented, persistent runtime/materializer pending
- **Scope:** storage-kernel vNext, not the legacy production engine
- **Depends on:** [ADR 0013](0013-storage-kernel-and-access-methods.md)
- **Refines:** [ADR 0003](0003-seerdb-commit-recovery-state-machine.md) and
  [ADR 0009](0009-buffered-btree-and-materialization.md). For vNext, the ordering
  below supersedes their allowance for installing shared current records before
  the durable decision. Their transaction-outcome and MVCC semantics remain.

## Context

The storage-kernel rewrite intentionally separated transaction logging, MVCC,
ordered access, undo durability and physical page materialization so each layer
could be qualified before they became one crash-sensitive runtime. That exposed
several integration hazards that are easy to miss when the pieces are reviewed
in isolation:

- shared-record installation and WAL ordering had been described inconsistently;
- a dirty page could otherwise reach `PageIo` before the WAL and referenced undo
  state were durable;
- a page's maximum WAL dependency does not prove that every older logical effect
  has actually been installed;
- independently durable page images do not by themselves form a structurally
  coherent recoverable B-link tree;
- replay needs stable effect identity without confusing transaction identity,
  commit ordering and physical page ordering;
- transaction owner status, allocation identities and replay evidence must remain
  recoverable after WAL retention advances.

The review also found concrete implementation defects in the foundation. WAL and
undo length fields were previously trusted before torn-tail classification,
retained undo scanning read the entire file into RAM, undo links did not reject
self/forward references, reopened WAL segments lost pending synchronization
state, and a buffer victim could be stolen between `Evicting -> Free` and a
second `Free -> Loading` transition. Those defects are fixed in the current
vNext branch.

## Current implementation boundary

Implemented and qualified as independent primitives:

- atomic B-tree `upsert` separate from duplicate-rejecting `insert`, including
  replacement growth that can split and unchanged-on-oversize behavior;
- repeated raw logical put/delete replay under split-heavy tiny-buffer eviction;
- one shared canonical final-effect reducer over the validated original mutation
  sequence, preserving the final logical ordinal for each `(object,key)`;
- sharded nonblocking write-intent ownership with canonical batch acquisition,
  conflict rollback, same-owner idempotence and RAII release;
- MVCC record envelope version 2 with optional install identity
  `(TxnId, final mutation ordinal)`; newly installed transaction-owned current
  records carry it and freezing may preserve it;
- an access-method-specific ordered MVCC installer for transient/non-authoritative
  page I/O, with exact-install no-op detection, before-image allocation, aborted
  owner bypass, live snapshot conflict checks and ordered recovery checks;
- standalone durable undo storage with strictly backward predecessor links;
- outer WAL and undo framing version 2 with checksummed fixed headers validated
  before length-based torn-tail repair;
- reopen durability barriers for retained WAL/undo directory state;
- frame-by-frame undo scanning, while the retained version-offset index still
  grows with retained history.

Still incomplete:

- the full durable-WAL-first transaction coordinator across all authoritative
  objects;
- grouped undo synchronization and status/frontier publication around the
  installer;
- same-image page dependency capture, materialization eligibility and checksums;
- out-of-place page mapping and structurally complete checkpoint publication;
- process-restart replay into the integrated persistent B-tree;
- snapshot lookup/range traversal through undo chains;
- freezing, owner-status retention, undo/WAL reclamation and physical GC;
- canonical row storage and OmenDB cutover.

The transient ordered installer is deliberately **not** proof of crash-safe
persistent current-record integration. Until page dependencies and checkpoint
publication exist, tests must use page I/O whose persisted bytes are not treated
as recovery authority.

## 1. Durable-WAL-first shared installation

The first integrated runtime uses private staged writes until a complete
transaction decision is durable. Read-your-writes comes from that private
transaction view; it does not require speculative replacement in shared pages.

For a writing transaction:

1. Validate the original mutation sequence and compute canonical final effects.
2. Acquire logical write intents for the unique `(StorageObjectId, key)` set in
   canonical order. Resolve write/write conflicts, snapshot/isolation rules,
   constraints and configured dependency certification while ownership is held.
3. Preflight deterministic failures: reserved identities, object availability,
   record/undo size limits, access-method representability, allocation progress
   and whole-transaction WAL segment limits. A value recovery cannot install
   must be refused before the durable decision.
4. Assign CSN and append the complete mutation/decision batch in the ordered
   append lane. Keep validation, page work and fsync outside that lane.
5. Synchronize through the decision LSN. The durable decision is irrevocable
   commit authority even if installation has not finished.
6. Under the retained intents, obtain each actual predecessor, append any
   required before-image and atomically install the canonical transaction-owned
   current record. Attach the decision/page and undo dependencies to the same
   guarded page mutation once that facility exists. Finish every authoritative
   object before publication.
7. Publish committed transaction status and mark its CSN ready. Advance new
   snapshots only through the contiguous ready frontier.
8. Complete the synchronous API only after durability and frontier visibility;
   release intents and the ordinary transaction snapshot on completion.

An ordinary pre-I/O validation/refusal can abort. Any uncertain WAL operation,
post-decision installation failure or poisoned critical state fences runtime
write admission and requires recovery, not a fabricated abort. The runtime fence
must be established before unresolved writer ownership is released.

This baseline intentionally forgoes speculative shared installation while WAL is
syncing. A future measured alternative needs its own abort, eviction and recovery
proof rather than silently reversing the protocol.

## 2. Canonical final effects and install identity

The original WAL mutation count, digest and contiguous ordinals remain the
transaction authenticity contract. Only after validation is the sequence reduced
to the final mutation for every `(object,key)`, sorted canonically. Live commit
and recovery use the same reducer.

The install identity is:

```text
(TxnId, final logical mutation ordinal)
```

within the addressed `(StorageObjectId, key)`. It identifies one canonical
logical effect. It is **not** a chronological ordering key; `TxnId` magnitude
must never be used to order transactions.

Required semantics:

- same retained install identity plus the same intended value/tombstone is a
  no-op;
- matching identity with different logical content is corruption;
- one transaction's different final identity may not silently replace its own
  already-installed current record;
- older recovery may not replace a newer committed current state merely because
  its LSN is smaller;
- MVCC delete installs a versioned tombstone rather than physically erasing
  replay evidence;
- aborted current state is bypassed through its undo link and is not appended as
  visible history;
- missing owner status fails closed unless recovery has explicitly established
  the outcome.

A crash after undo append but before current replacement can leave an
unreferenced undo record. Install identity in the new current record cannot
prevent that orphan. The baseline therefore requires logical/history idempotence
and allocation-free repeats of a retained completed install, while permitting
unreachable allocation to become GC work. Stronger allocation deduplication
would require a separate durable installation ledger and is not currently
justified.

## 3. Atomic upsert is an access-method primitive, not a persistence protocol

The ordered B-tree now has a distinct atomic `upsert` path. It builds a complete
candidate leaf off to the side while holding the page write guard and publishes
it only after validation. Ordinary `insert` remains duplicate-rejecting. Growth
that requires a split enters structural coordination and revalidates before the
B-link split is published. Oversized replacement leaves the old logical record
unchanged.

Raw logical replay is qualified by applying mixed inserts/replacements/deletes
twice under a tiny buffer with forced eviction and split pressure and comparing
the final ordered state to a `BTreeMap` oracle.

This does **not** make B-tree persistence crash safe. A B-link right sibling
keeps concurrent traversal correct, but arbitrary independently materialized
old/new sibling, parent and root images need not form a valid recovery graph.
All same-key transactional replacement paths must also retain logical intent
ownership for the entire read-before-image-upsert sequence.

## 4. Page dependencies and logical replay completion are different

For every captured page image, the materialization layer must retain at least:

```text
required WAL LSN  = highest WAL durability dependency of that exact image
required undo ID  = highest referenced undo VersionId, if any
```

The page bytes and dependencies must be captured under the same page guard or
validated version. A post-hoc metadata update is insufficient because writeback
could observe the new bytes first. Split siblings must inherit every dependency
of copied state as well as dependencies introduced by the mutation causing the
split.

Before an image can reach durable page storage, require both frontiers to cover
its dependencies. The generic materialization envelope owns this authority; a
frame version, dirty bit, root generation or transaction CSN is not a substitute.

A maximum page WAL LSN is **not** a logical-redo skip watermark. Example: two
independent keys share a page, and installation for decision LSN 200 completes
before installation for LSN 100. An image requiring WAL 200 can still lack the
effect at 100. Splits can then move records to different pages. Logical replay
may be skipped only using a complete checkpoint prefix or explicit validated
per-effect replay evidence.

## 5. Structurally complete checkpoints precede persistent cutover

The WAL/undo durability frontiers prevent write-ahead violations but do not prove
that independently durable pages form a complete B-tree graph or that every
logical transaction before a replay boundary is represented.

The first recovery model is therefore one structurally complete checkpoint plus
the committed logical WAL suffix:

1. Quiesce new commit installation and structural/GC mutation long enough to
   drain a fully installed contiguous visible frontier and capture its decision
   LSN.
2. Materialize the complete reachable authoritative graph and checkpoint
   metadata: roots, logical-to-physical map, allocation high-water marks,
   retained owner outcomes and retention state. Enforce WAL/undo barriers and
   integrity checks for every referenced image.
3. Publish the checkpoint only after all dependencies are durable. Retain the
   prior complete checkpoint and its dependencies until publication succeeds.
4. Reopen one complete checkpoint, validate and synchronize the retained WAL
   suffix, replay committed canonical final effects in validated order, validate
   resulting object/current/undo references, then publish the recovered
   visibility frontier.

Uncheckpointed eviction images may be useful working spill state during the
running process, but their existence or in-memory page-map publication does not
advance recovery authority. After a crash they can be ignored unless a later
protocol explicitly proves them part of a coherent checkpoint epoch.

Checkpoint quiescence is a correctness baseline, not a desired permanent
latency strategy. A nonblocking coherent epoch or crash-qualified structural
logging may replace it only after proving reference closure, replay completeness
and retention.

## 6. Owner status, identity and retention are checkpoint state

A process-local transaction-status table rebuilt only from retained WAL is
insufficient once old decisions are reclaimed. Every reachable transaction-owned
current or undo record must either have a checkpointed durable outcome or be
safely frozen to its CSN before its decision disappears. Freezing install
identity also requires proving the replay horizon no longer needs it.

Snapshot, CDC, replica and backup leases constrain reclamation together. The
current undo file is append-only with contiguous logical `VersionId`s and no
front-truncation/reuse protocol; do not reuse IDs or truncate the front as an
ad-hoc GC mechanism.

Checkpoint/object metadata must recover allocation high-water marks and prevent
reuse of live transaction, object or page identities. WAL, undo and page-map
components must eventually be bound to the same database/store incarnation so
numeric IDs cannot accidentally validate a foreign component. The owning
runtime also needs exclusive writable directory ownership; per-handle mutexes do
not provide a cross-process writer lock.

## 7. Format and resource qualification

The experimental vNext formats deliberately fail closed rather than silently
reinterpret incompatible bytes:

- outer transaction WAL framing: version 2;
- outer undo-store framing: version 2;
- inner `MvccRecord` envelope: version 2 with optional install identity.

Version-1 vNext files/envelopes are disposable development formats and are not
silently migrated. The legacy production engine's formats are unaffected.

Checksummed fixed headers distinguish length corruption from a valid-header torn
append. Unknown formats and complete checksum/payload corruption fail closed;
only an incomplete final header/frame is repairable under the append/torn-write
failure model.

Undo payload scanning is now bounded to one frame, but the offset index, WAL
recovery vectors and pending/terminal transaction bookkeeping still grow with
retained history. Explicit retention and streaming/indexing policy remain
required before claiming bounded-memory large-history recovery.

## Required integration qualification

Before persistent cutover, tests must cover at least:

- same-key writers and multi-object decisions with intents retained through
  publication;
- canonical repeated writes and exact install-identity replay;
- active, aborted, frozen and committed predecessor classification;
- failure after undo append, after undo sync and after subsets of current-record
  installation;
- failure around WAL append/sync, status publication and frontier advancement;
- runtime fencing before unresolved intent release;
- out-of-order installers on different keys of one page;
- page dependency inheritance through split sibling/parent/root publication;
- writeback of one captured image while a newer frame version becomes dirty;
- structurally complete checkpoint graph closure and two consecutive reopen
  attempts at every crash boundary;
- checkpointed owner outcomes, allocation high-water marks and WAL/undo
  retention;
- small-buffer progress, allocation refusal and oversized deterministic
  precommit rejection.

## Next implementation order

1. Finish qualification of the transient ordered MVCC installer and keep all
   stable/MSRV/Clippy/oracle gates green.
2. Add the durable-WAL-first transaction coordinator around canonical effects,
   intents, preflight, WAL append/sync, grouped undo durability, installation,
   status publication and contiguous visibility. Keep its page I/O transient
   until step 4.
3. Add point snapshot resolution through current/undo chains, then range/cursor
   traversal using the same visibility resolver.
4. Add same-image page dependency metadata, durability gating, integrity,
   out-of-place working placement and a structurally complete checkpoint/page
   map.
5. Qualify process crash/reopen twice at every transaction and structural
   boundary before allowing persistent vNext current-record pages.
6. Only then optimize durability batching, frame latching/translation,
   nonblocking checkpoints, compression/fence truncation, version placement and
   structural coordination from end-to-end measurements.
