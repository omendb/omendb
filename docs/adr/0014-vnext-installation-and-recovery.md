# ADR 0014: vNext installation, replay and checkpoint boundaries

- **Status:** accepted integration contract; transaction/MVCC/dependency-aware materialization baseline implemented, persistent checkpoint authority pending
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
- write-conflict/predecessor validation must complete before an irrevocable
  commit decision is created;
- a dirty page could otherwise reach `PageIo` before the WAL and referenced undo
  state were durable;
- a transaction could otherwise install a page referencing its own unsynced undo
  and then need that ineligible page as an eviction victim to finish commit;
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

Implemented and integrated:

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
- ordered MVCC prepare/apply: predecessor visibility/order is validated and any
  complete before-image is appended without page mutation, required undo is
  group-synchronized, then the exact predecessor is revalidated before install;
- live predecessor/write-conflict validation under retained intents before WAL
  append, including clean snapshot/active-writer rejection with no commit
  decision; pre-WAL before-images may remain unreachable GC work on clean abort;
- durable-WAL-first multi-object commit coordination, exact WAL sync on the same
  owned log, grouped undo durability, authoritative page installation, status
  publication and contiguous visibility;
- private point/range read-your-writes and point/range fixed-snapshot traversal
  through the shared current/undo visibility resolver;
- sequential ordered recovery using the same canonical effects, intents,
  prepared predecessor state, grouped undo barrier, installer and visibility
  frontier as live commit;
- standalone durable undo storage with strictly backward predecessor links;
- outer WAL and undo framing version 2 with checksummed fixed headers validated
  before length-based torn-tail repair;
- reopen durability barriers for retained WAL/undo directory state;
- frame-by-frame undo scanning, while the retained version-offset index still
  grows with retained history;
- sharded in-process per-page WAL/undo dependency tracking and a checked `PageIo`
  gate that refuses physical writeback until both durability frontiers cover the
  exact page requirements;
- synchronous completion: `VisibilityFrontier` distinguishes readiness
  publication from completion, waits for contiguous coverage, and reports
  recovery-required instead of blocking or acknowledging when an earlier durable
  decision cannot complete;
- dependency-aware B-tree upsert that attaches requirements while the exact page
  remains pinned and conservatively inherits source requirements through leaf
  splits, internal splits and root replacement;
- live commit/recovery page dependency frontier wiring: decision WAL is covered
  before page mutation, referenced undo is group-synchronized before page
  mutation, and each resulting page image records decision LSN plus actual
  resulting undo head.

Still incomplete:

- checksummed persistent page envelope and out-of-place physical page map;
- structurally complete checkpoint/manifest publication retaining roots, object
  metadata, allocation high-water marks, page map, owner outcomes and retention;
- process-restart recovery from checkpoint plus synchronized retained WAL suffix
  into authoritative vNext access methods;
- deterministic/bounded resource admission for arbitrary pin/buffer/allocation
  pressure and whole-transaction WAL bounds;
- runtime-wide read/snapshot/checkpoint admission fencing and pending-commit
  wakeup after unresolved post-decision failures, including defined handling for
  already-admitted writers; current fencing is write-admission scope;
- complete failpoint/crash matrix and two consecutive reopens across transaction,
  dependency, structural and checkpoint boundaries;
- freezing, owner-status retention, undo/WAL reclamation and physical GC;
- canonical row storage and OmenDB cutover.

The in-process dependency-aware page path is deliberately **not** proof of
restart-safe persistent current-record integration. Until a complete checkpoint
and page-map authority exists, durable working spill pages are not recovery
authority after a crash.

## 1. Durable-WAL-first shared installation

The integrated runtime uses private staged writes until a complete transaction
decision is durable. Read-your-writes comes from that private transaction view;
it does not require speculative replacement in shared pages.

For a writing transaction:

1. Validate the original mutation sequence and compute canonical final effects.
2. Acquire logical write intents for the unique `(StorageObjectId, key)` set in
   canonical order.
3. Preflight deterministic representability/object failures and, while intents
   remain held, inspect each actual predecessor. Resolve write/write and snapshot
   conflicts, reject corrupt/unknown current owners, and prepare the resulting
   current record. If a visible predecessor must become history, append its
   complete before-image to undo **without mutating shared pages**. A clean
   pre-WAL refusal may leave such an unreferenced undo record as GC work.
4. Enter transaction validation, assign CSN and append the complete
   mutation/decision batch in the ordered append lane. Keep predecessor analysis,
   page work and fsync outside that lane.
5. Synchronize through the decision LSN. The durable decision is irrevocable
   commit authority even if installation has not finished. Only after this
   barrier may the in-process page WAL frontier advance.
6. Group-synchronize undo through the highest `VersionId` referenced by any
   prepared effect. Only after this barrier may the page undo frontier advance.
7. Under the retained intents, revalidate each exact prepared predecessor and
   atomically install the canonical transaction-owned current record. Attach the
   decision LSN and actual resulting undo head to the same guarded page mutation.
   Split siblings/parents/roots conservatively inherit copied-state dependencies.
   Finish every authoritative object before publication.
8. Publish committed transaction status and mark its CSN ready. Advance new
   snapshots only through the contiguous ready frontier.
9. Wait until the contiguous frontier covers this transaction's CSN before
   returning synchronous success; marking a CSN ready is not completion. Release
   intents and the ordinary transaction snapshot on completion. An unresolved
   earlier decision wakes pending completion waits with recovery-required
   semantics rather than allowing success or an indefinite wait.

Pre-WAL snapshot/write conflicts are ordinary clean refusals: they create no WAL
decision and leave the transaction active for caller-directed abort/retry.
Corrupt current state, uncertain undo I/O or poisoned critical state fails closed
and may fence the runtime even before WAL because continuing could make later
transactions depend on untrustworthy storage state. Any uncertain WAL operation
or post-decision failure fences runtime write admission and requires recovery,
not a fabricated abort. The runtime fence must be established before unresolved
writer ownership is released.

Preparing undo before WAL is safe because no shared page references those bytes
until after both the WAL decision and required undo are durable. The trade-off is
that a clean pre-WAL abort may leave unreachable undo allocation. Avoiding that
space leak would require a more elaborate reservation/deduplication protocol and
is not part of the correctness baseline.

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

A crash or clean refusal after undo append but before current replacement can
leave an unreferenced undo record. Install identity in a later current record
cannot prevent that orphan. The baseline therefore requires logical/history
idempotence and allocation-free repeats of a retained completed install, while
permitting unreachable allocation to become GC work. Stronger allocation
deduplication would require a separate durable installation ledger and is not
currently justified.

## 3. Atomic upsert is an access-method primitive, not a persistence protocol

The ordered B-tree has a distinct atomic `upsert` path. It builds a complete
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
All same-key transactional replacement paths retain logical intent ownership for
the entire prepare-before-image-revalidate-upsert sequence.

## 4. Page dependencies and logical replay completion are different

For every captured page image, the materialization layer retains at least:

```text
required WAL LSN  = highest WAL durability dependency of that exact image
required undo ID  = highest referenced undo VersionId, if any
```

The page bytes and dependencies are attached while the exact page remains pinned
and writer-owned. A post-hoc metadata update is insufficient because writeback
could observe the new bytes first. Split siblings inherit every conservative
dependency of copied state as well as dependencies introduced by the mutation
causing the split; structural parents/new roots inherit the relevant maxima.

Before an image can reach physical working-page storage, require both frontiers
to cover its dependencies. The generic materialization envelope owns this
authority; a frame version, dirty bit, root generation or transaction CSN is not
a substitute.

A maximum page WAL LSN is **not** a logical-redo skip watermark. Example: two
independent keys share a page, and installation for decision LSN 200 completes
before installation for LSN 100. An image requiring WAL 200 can still lack the
effect at 100. Splits can then move records to different pages. Logical replay
may be skipped only using a complete checkpoint prefix or explicit validated
per-effect replay evidence.

The current dependency table is process-local. It makes in-process spill obey
write-ahead ordering, but its loss on crash is exactly why written working pages
are not yet restart authority.

## 5. Structurally complete checkpoints precede persistent cutover

The WAL/undo durability frontiers prevent write-ahead violations but do not prove
that independently durable pages form a complete B-tree graph or that every
logical transaction before a replay boundary is represented.

The first recovery model is therefore one structurally complete checkpoint plus
the committed logical WAL suffix:

1. Close admission to new committing mutations, while allowing admitted work
   to finish installation/publication. After draining, establish the exact
   `(visible CSN, decision LSN)` cut. Hold install/structural/GC and allocation
   metadata mutation quiescent during graph capture. Do not park installers
   needed by the drain or include partially installed transactions above the
   checkpoint prefix. Failed drains require recovery, not partial publication.
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
latency strategy or a promise of a brief pause. Graph traversal and materializing
dirty images can hold admission closed through substantial I/O. Reuse unchanged
immutable images where valid and measure pause cost against database size and
dirty fraction. A nonblocking coherent epoch or crash-qualified structural
logging may replace it only after proving reference closure, replay completeness
and retention. Meet checkpoint latency budgets before product cutover.

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
components must be bound to the same database/store incarnation in the first
persistent runtime so numeric IDs cannot accidentally validate a foreign
component. The owning
runtime also needs exclusive writable directory ownership; per-handle mutexes do
not provide a cross-process writer lock. Both store binding and exclusive
writable ownership are Milestone F acceptance requirements, not deferred GC work.

The runtime lifecycle must also govern snapshot/read/checkpoint admission and
pending commit completion after failure. Already-admitted writers need explicit
drain/stop behavior; an entry-only fence is insufficient. Either fence reads or
prove an explicit safe-prior read boundary before exposing a persistent runtime.

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

Undo payload scanning is bounded to one frame, but the offset index, WAL
recovery vectors and pending/terminal transaction bookkeeping still grow with
retained history. Explicit retention and streaming/indexing policy remain
required before claiming bounded-memory large-history recovery.

## Required integration qualification

Before persistent cutover, tests must cover at least:

- same-key writers and multi-object decisions with intents retained through
  publication;
- pre-WAL snapshot/active-writer conflict rejection with no decision bytes;
- canonical repeated writes and exact install-identity replay;
- active, aborted, frozen and committed predecessor classification;
- clean refusal/failure after unreachable pre-WAL undo append;
- failure after decision WAL sync, after grouped undo sync and after subsets of
  current-record installation;
- failure around status publication and frontier advancement;
- a later installed CSN cannot acknowledge success while an earlier CSN leaves a
  frontier gap; completing the gap releases waiters, and unresolved failure wakes
  them with recovery-required semantics;
- already-admitted writers and checkpoint drains racing runtime failure;
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

## Implementation roadmap

The [vNext plan](../plans/storage-kernel-vnext.md#immediate-sequence) owns the
implementation sequence and open gates. Fix visibility completion first;
qualify runtime lifecycle and store identity with checkpoint authority; then
address measured architectural performance costs before canonical-row/product
cutover. This ADR owns the protocol, not a second execution backlog.
