# ADR 0014: vNext installation, replay and checkpoint boundaries

- **Status:** accepted integration contract; runtime/materializer implementation pending
- **Scope:** storage-kernel vNext, not the legacy production engine
- **Depends on:** [ADR 0013](0013-storage-kernel-and-access-methods.md)
- **Refines:** [ADR 0003](0003-seerdb-commit-recovery-state-machine.md) and
  [ADR 0009](0009-buffered-btree-and-materialization.md). For vNext, the ordering
  below supersedes their allowance for installing shared current records before
  the durable decision. Their transaction-outcome and MVCC semantics remain.

## Context and implementation boundary

The 2026-09-12 review found incompatible execution orders in the implementation
plan: WAL-first application in milestone D, but current-record installation
before WAL and before write intents in the immediate sequence. The standalone
buffer can already evict dirty pages, while page dependency tracking and durable
mapping/checkpoint publication do not exist. Wiring these pieces together in
the wrong order would turn independently tested primitives into an unsafe
persistent runtime.

The review also identified and corrected concrete foundation defects:

- WAL and undo parsers trusted an unchecked length before deciding to truncate a
  final record. A corrupted complete record could be mistaken for a torn append.
  Their experimental outer formats now use version 2 with a separate fixed-header
  checksum verified before length-based tail classification.
- Undo recovery read the entire retained file into RAM. It now scans one bounded
  frame at a time, in addition to its still-resident version-offset index.
- Undo before-images accepted self/forward links. Append and retained-frame
  validation now require every predecessor to be nonzero and strictly earlier.
- Reopened WAL segments lost their pending synchronization state. The device now
  treats retained segments and directory entries as requiring a new barrier;
  recovery parsing alone does not confirm a durable frontier.

The inner `MvccRecord` envelope remains version 1, without an install identity.
Atomic B-tree upsert, final-write-set normalization, write intents, the MVCC
installer, page dependency metadata and checkpoints are still unimplemented.
This ADR specifies their contract; it does not mark them complete.

## 1. Durable-WAL-first shared installation

The first integrated runtime uses private staged writes until a complete
transaction decision is durable. Read-your-writes comes from that private
transaction view; it does not require speculative replacement in shared pages.

For a writing transaction:

1. Validate the original mutation sequence and compute canonical final effects.
2. Acquire logical write intents for the unique `(StorageObjectId, key)` set in
   canonical order. Resolve write/write conflicts, snapshot/isolation rules,
   constraints and configured dependency certification while ownership is held.
3. Preflight deterministic failures: object availability, encodable identities,
   record/undo size limits, access-method representability and whole-transaction
   WAL segment limits. Establish bounded buffer/allocation progress before
   making a decision that recovery could never apply. Lack of an overflow-value
   path must be a precommit refusal, not an unreplayable committed value.
4. Assign CSN and append the complete mutation/decision batch in the ordered
   append lane. Leave validation, page I/O and fsync outside that lane.
5. Synchronize through the decision LSN. The durable decision is irrevocable
   commit authority even if installation has not finished.
6. Under the retained intents, obtain each actual predecessor, append its
   required before-image and atomically install the final transaction-owned
   current record. Attach the decision LSN and undo dependencies to the same
   guarded page mutation. Finish every authoritative object before publication.
7. Publish the committed transaction status and mark its CSN ready. Advance new
   snapshots only through the contiguous ready frontier.
8. Complete the synchronous API only after durability and frontier visibility;
   release intents and the ordinary transaction snapshot on completion.

An ordinary pre-I/O validation/refusal can abort. Any uncertain WAL operation,
post-decision installation failure or poisoned critical state fences runtime
write admission and requires recovery, not a fabricated abort. RAII cleanup
must not release an unresolved durable writer's keys to a still-running writer:
establish the runtime fence before releasing ownership. Read continuation is
allowed only through an explicitly proven safe prior snapshot boundary.

This baseline forgoes speculative overlap between shared installation and WAL
sync. A future measured alternative needs its own abort, eviction and recovery
proof; it must not be introduced by silently reversing these steps.

## 2. One physical final effect, with explicit replay semantics

Keep the original WAL mutation count, digest and contiguous ordinals as the
validation contract. Only after validation, reduce to the final mutation for
each `(object, key)`, sorted canonically and retaining its final ordinal. Live
commit and recovery consume the same reducer. This is physical normalization,
not permission to skip statement-time constraints, triggers or savepoint rules.

Define install identity together with this normalized view. A candidate is
`(TxnId, final mutation ordinal)` within the addressed object/key. It identifies
an effect, not a chronological ordering: `TxnId` magnitude does not order commits.

Required rules:

- The same retained install identity and intended value/tombstone is a no-op.
  A matching identity with different content is corruption.
- An already completed install must not append another reachable copy of its
  predecessor. The first baseline installs final effects only; it need not
  support an arbitrary sequence of shared intermediate writes by one owner.
- Older replay must not replace a newer committed current state. Resolve order
  using validated commit metadata, while preserving any history still required
  by retained snapshots. A newer current value is not proof that an earlier
  historical effect is present.
- MVCC delete installs a versioned tombstone. Raw-tree replay's missing-delete
  no-op is a separate test property, not permission to remove MVCC replay
  evidence. Tombstone removal/freezing must respect both snapshot and replay
  horizons, or old redo can resurrect a deleted key.
- Aborted current state is bypassed through undo, not preserved as a visible
  historical version. Missing transaction status is an error unless recovery
  has explicitly established the owner's outcome.

A crash after undo append but before current replacement can leave an
unreferenced undo record. A marker stored only in the new current record cannot
prevent that orphan. Permit and eventually reclaim unreachable allocations;
require logical/history idempotence and no allocation on repeats of a retained
completed install. Claiming allocation-free retries across every crash boundary
would require additional durable deduplication/installation metadata, which is
not part of this baseline.

## 3. Atomic upsert is not a complete persistence protocol

Add raw upsert separately from duplicate-rejecting `insert`. Build and validate
a candidate leaf image before publishing it under the write guard; never expose
a delete-then-insert gap. Revalidate after entering structural coordination when
growth requires a split. Oversized replacements must leave the old value intact.

All same-key logical replacement paths must respect the intent protocol or use
a validated conditional replacement; a prior unprotected lookup followed by an
unconditional upsert is not an atomic read-modify-write.

Distinguish failure before publication from failure after a split has published
new reachable state. The latter cannot promise an unchanged tree. A B-link
right link makes concurrent traversal possible, but does not by itself recover
arbitrary combinations of old/new durable sibling, parent and root images.
Current delayed-root-promotion fault tests are not a power-loss proof for the
future page map. The persistent runtime must satisfy section 5.

## 4. Page dependencies and logical replay completion are different

For each captured page image, record:

```text
required WAL LSN   = highest WAL dependency of that image
required undo ID  = highest referenced undo VersionId, if any
```

Capture the bytes and their dependency metadata under the same guard/version.
Before writing that image, require the durable WAL and undo frontiers to cover
both dependencies. Preserve them through eviction/reload and copy all inherited
dependencies into split siblings. Frozen owners and structural changes do not
excuse losing dependencies. The generic materialization envelope, not the
B-tree hot-path header or frame dirty bit, owns persistence authority.

A maximum page LSN is **not** a logical-redo skip watermark. For example, two
independent keys share a page: the installer for decision LSN 200 finishes before
the installer for LSN 100. An image requiring WAL 200 can still lack the effect
at 100. Skipping every older logical record would lose committed data. Moving a
record across pages during a split further separates physical page ordering from
logical effect identity.

Only a complete checkpoint prefix or explicit validated per-effect replay
metadata can justify skipping logical effects. Do not import a physiological
page-LSN replay rule without its physical logging/ordered-application premises.

## 5. Structurally complete checkpoints precede persistent cutover

The two write-ahead frontiers are necessary, not sufficient. They do not prove
that a recovered root and page map include every child/sibling or that all
logical transactions before a replay boundary are represented.

Use a structurally complete checkpoint plus committed logical WAL suffix as the
first recovery model. Its minimum executable correctness baseline is:

1. Quiesce new commit installation and structural/GC mutation, drain outstanding
   decisions through a fully installed contiguous visible frontier, and capture
   its decision LSN. Private, uncommitted staged effects are excluded.
2. Materialize a complete reachable object graph and checkpoint metadata:
   roots, logical-to-physical page map, allocation high-water marks, retained
   owner outcomes and retention metadata. Enforce page/undo/log barriers and
   integrity checks for all referenced images.
3. Durably publish the new checkpoint only after all dependencies exist. Retain
   the prior checkpoint and its dependencies until publication succeeds.
4. Reopen from one complete published checkpoint, validate the retained WAL
   suffix, establish its durability barrier, then replay committed final effects
   before publishing the recovered visibility frontier. Validate all resulting
   current/undo/object references before accepting transactions.

Ordinary out-of-place eviction images may be used by the running process as
working spill state; their individual existence or in-memory map publication
must not advance recovery authority. After a crash, ignore uncheckpointed
candidate images rather than selecting independently newest pages. Logical WAL
replay cannot generally repair an arbitrary torn structural graph.

Quiescence is a checkpoint-only correctness baseline, not a transaction-global
mutex or a production latency target. Measure checkpoint pauses and write
amplification before cutover. A nonblocking coherent checkpoint epoch or
crash-qualified structural/physiological logging can replace it, but must prove
reference closure, replay completeness and retention before permitting fuzzy
maps. Do not keep a pause-heavy baseline merely because it exists.

The current `PageIo` contract carries bytes only. Until dependency capture and
checkpoint publication are implemented, MVCC installer tests may use transient
page devices, but must not claim crash-safe persistent integration.

## 6. Status, identity and retention are part of the checkpoint

A process-local status table rebuilt solely from retained WAL is insufficient
once older WAL is reclaimed. Every reachable current/undo transaction owner
must either have a checkpointed durable outcome or be safely frozen to its CSN
before its decision records disappear. Freeze must also preserve required replay
evidence or prove the replay horizon has advanced past it.

Snapshot, CDC, replica and backup leases constrain reclamation together. The
current contiguous-prefix undo file has no segmentation/GC protocol: do not
truncate its front or reuse version IDs as a shortcut. Physical relocation must
preserve logical identities or update references through an explicit protocol.

Checkpoint/object metadata must recover allocation high-water marks and prevent
reuse of live `TxnId`, object and page identities. Bind WAL, undo and page-map
files to the same database/store incarnation so numeric IDs cannot validate an
accidentally paired foreign file. The owning runtime must hold exclusive
writable directory ownership; standalone per-handle mutexes are not a
cross-process writer lock. Reopening an existing database must not silently
create a missing required component as though it were a new empty database.

## 7. Format and resource qualification

Outer vNext WAL and undo version 1 files are deliberately rejected; there is no
silent fallback or reinterpretation. Recreate disposable vNext stores, or use a
separately validated offline migration. The legacy engine's format and the
inner MVCC envelope are not changed by this framing correction.

Checksummed headers distinguish length corruption from valid-header truncation.
A complete bad header, unknown format or complete bad payload fails closed and
must not be rewritten. Repair only the final incomplete suffix under the
append/torn-write failure model; checksums are not a general proof against
arbitrary file deletion or malicious rewriting.

Streaming undo payloads does not make all recovery memory bounded: its offset
index, the WAL recovery result vector, pending transaction mutations and terminal
identity set still grow with retained state. Before large-history qualification,
add explicit retention/size bounds and streaming/indexing policy. Keep the
16 MiB undo bound and whole-transaction segment bound visible to admission.

Required integration tests, beyond standalone framing regressions:

- same-key writers and multi-object decisions; intents held across publication;
- out-of-order installers on different keys of the same page;
- repeated final effects, tombstones, freezing and retained snapshot history;
- failure after undo append/sync and after a subset of current installs;
- failure around WAL append/sync, status publication and frontier advancement;
- split sibling/parent/root publication and checkpoint graph closure;
- writeback of one captured image while a newer version becomes dirty;
- recovery barriers for complete bytes surviving only a process restart;
- runtime fencing before unresolved-intent release, including injected undo I/O
  failure rather than only raw-file tail manipulation;
- small-buffer progress, allocation refusal and oversized precommit rejection;
- checkpointed owner outcomes, WAL/undo retention and two consecutive reopen
  attempts at every crash boundary.

The next implementation work remains atomic raw upsert and replay qualification,
then shared normalization/intents and the integrated installer. This decision
closes the protocol gaps without adding another speculative runtime hierarchy.
