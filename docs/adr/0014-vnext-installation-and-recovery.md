# ADR 0014: vNext decisions, installation and checkpoint recovery

- **Status:** accepted target protocol; component baseline implemented, integrated runtime/certification/checkpoint qualification pending
- **Scope:** storage-kernel vNext, not the legacy production engine
- **Depends on:** [ADR 0013](0013-storage-kernel-and-access-methods.md)
- **Refines:** [ADR 0003](0003-seerdb-commit-recovery-state-machine.md) and [ADR 0009](0009-buffered-btree-and-materialization.md). For vNext, durable-decision-first installation supersedes speculative shared installation. ADR 0009 owns ordered-page representation; this ADR owns transaction and recovery authority.

## Decision and rationale

Keep private mutations until a complete transaction decision is durable. Use one
runtime to own certification, log ordering, completion, visibility, resource and
history leases, and checkpoint authority. A collection of independently fenced
components is not a persistent database.

Durability, logical visibility and physical materialization are separate:

- a durable decision is irrevocable commit authority;
- synchronous success requires every authoritative effect installed and the
  contiguous visibility frontier covering that decision;
- working page images obey write-ahead dependencies but are not restart
  authority without a complete checkpoint and retained committed WAL suffix.

The component implementation in `crates/seerdb/src/vnext` establishes useful
pieces of this protocol, not all of it. In particular, optional coordinator
admission, empty-leaf preflight, fixed snapshots plus write intents, and lexical
commit guards do not establish the target contracts. The
[vNext plan](../plans/storage-kernel-vnext.md) owns status and remaining gates.

## 1. Runtime-owned transaction and completion capabilities

One persistent runtime constructs and owns:

```text
store incarnation + exclusive component/directory ownership
transaction/status/visibility authority
snapshot, catalog/object and certification-history leases
write intents + certification/decision sequencer
WAL/undo/page dependency services
workload reservations + protected progress capacity
commit-attempt responsibility + checkpoint mutation drain
```

Transactions, reads, installers and checkpoints obtain capabilities from that
owner. Independent service bundles and optional attachment remain component-test
seams, not supported persistent entry points. SQL meaning stays above the kernel.
Every actual read/snapshot/direct/server entry point checks runtime failure;
long-running operations recheck at bounded work boundaries. The baseline fences
reads on recovery-required state rather than claiming an unproven safe-prior
read boundary.

Workload admission is transaction-grain and precedes snapshot/intents. Retain it
through explicit transaction lifetime; later statements and completion do not
queue behind waiters needing that transaction's resources. Checkpoint membership
is distinct: it covers committing mutations through installation and contiguous
visibility. Checkpoint closure must not park installers, WAL completion, rollback
or other work required by its own drain. Structural, freezing, GC and allocation
mutators use the same capture authority.

Establish commit-attempt responsibility **before entering any append that could
succeed**, not after recording a returned ticket. Ownership ends only with:

1. explicit contiguous-visibility completion;
2. a proven clean refusal before possible decision append; or
3. terminal recovery fencing established before releasing unresolved intents,
   reservations or drain membership.

In a continuing-process host, unresolved unwind fences runtime admission and
visibility waiters before ownership drops. A process-aborting panic profile is
an explicit alternative hosting contract, not an assumption for an embeddable
library or a server catching worker failure. Cancellation/disconnect after
possible append abandons response delivery, never commit responsibility. No
ordinary retry is safe after an unknown outcome without durable request identity
or outcome lookup.

## 2. Decision-ordered optimistic certification

The initial SERIALIZABLE implementation uses fixed snapshots and conservative
optimistic certification ordered with commit decisions. Exact write intents
alone provide write conflict protection, not serializability.

A writing transaction at snapshot `S` freezes its effects and logical dependencies:

- existing and absent point reads;
- ranges and gaps, including deletions;
- catalog/name resolution, table enforcement and object lifecycle dependencies;
- constraint probes required independently of user isolation mode.

Certify these observations against **every earlier accepted decision after S**,
including uninstalled, unsynchronized or not-yet-visible effects. Acceptance,
CSN assignment, complete WAL append and publication of the conflict summary have
one sequencer authority. No later certifier may pass through a validate/decide
or decide/summary gap. Uncertain append fences the runtime; it cannot leave a
usable sequencer with missing conflict evidence.

Prepare predecessors, evaluate constraints and search historical summaries
outside the short decision lane. Recheck the remaining decision tail under that
lane before acceptance. Do not run table scans, page I/O or synchronization
there. Physical page latches and cursor positions are not logical predicates.
Conservative range coverage is acceptable where tighter LIMIT/search dependency
coverage is not yet proven.

Retain indexed, CSN-ordered write/object summaries after the oldest snapshot
that may still write. Row versions, layouts and retired objects have separate
leases. Dependency sets and summary bytes are budgeted; pressure may spill,
coarsen conservatively, refuse new work or explicitly expire an abortable
transaction. Never discard evidence and then report successful certification;
never pressure-abort an unresolved accepted decision.

This imposes decision order as the serial order of accepted writers, rejecting
some histories that another serial order could accept. It is not PostgreSQL SSI.
SSI or pessimistic ranges are measured alternatives only if abort/retention costs
justify their dependency graph or deadlock machinery. A genuinely read-only
transaction can serialize at its snapshot when the prefix consists of these
certified writers; a future mixed-isolation design must revisit that exemption.
Certification indexes need not survive restart when all old live transactions
end and new admission waits for complete recovery.

Primary comparison: [FoundationDB transaction semantics](https://apple.github.io/foundationdb/developer-guide.html#transactions-in-foundationdb)
support fixed-snapshot optimistic validation, not adopting its transaction-duration
or record-size limits. [PostgreSQL isolation](https://www.postgresql.org/docs/18/transaction-iso.html)
describes a different SSI trade-off. Sources checked 2026-10-11.

## 3. Admission and durable-decision-first installation

For a writing transaction:

1. Validate the original mutation stream and reduce it to canonical final effects.
2. Acquire logical intents in canonical order. Preflight objects, codecs, logical
   limits and every physical layout needed by install/replay. Reserve bounded
   progress capacity for the original WAL batch, complete predecessor undo,
   overflow/structural allocation, scratch, status and dependency metadata.
3. Under retained intents inspect each actual predecessor, reject write/snapshot
   conflicts and unknown/corrupt owners, and prepare complete logical before-images
   without mutating shared pages. Clean pre-WAL refusal may leave unreachable undo.
4. Certify at the sequencer, assign CSN, append the complete transaction/decision
   and publish its conflict summary under the ordering contract above.
5. Synchronize through the decision LSN on the same owned WAL. Only then advance
   the in-process page WAL frontier. The decision is now irrevocable.
6. Group-synchronize all required undo before any shared page mutation; only then
   advance the page undo frontier.
7. Revalidate exact prepared predecessors, install all authoritative effects,
   and finish structural obligations using reserved capacity. Attach exact WAL/undo
   dependencies with each guarded mutation, including overflow and split pages.
8. Publish owner status and CSN readiness; advance visibility only through the
   contiguous ready prefix. Wait for coverage before acknowledging and releasing.

Resource reservations are a progress contract under healthy storage and stated
configuration limits, not immunity to permanent I/O failure. They must survive
concurrent structural growth and account for peak pins and worst-case split/root
work, not merely a leaf observed during preflight. Keep a coordinated, protected
installer slow path rather than retrying indefinitely after decision authority.
Do not hold page byte latches or arbitrary foreground pins across WAL barriers.

Deterministic refusal belongs before device I/O. In particular, bound the complete
original WAL stream, not only reduced effects; the current segmented device
requires a batch wholly within one segment's record region and the LSN offset
domain. A known-too-large batch is not an ambiguous commit. Uncertain I/O and
post-decision failure instead fence before ownership release.

Supported logical records must remain representable as future undo before-images
on update/delete. Current undo framing caps encoded records at 16 MiB while MVCC
and WAL have other framing limits. Own a coherent logical admission policy across
those codecs and memory budgets; larger support requires changing the limiting
framing, not bypassing it with page overflow. Neither a slot width nor one framing
constant is the general-purpose database's permanent logical-size requirement.

This baseline forgoes shared speculative install during WAL sync. Reordering or
removing a WAL/undo barrier requires a separate recovery proof and measurements.

## 4. Logical effect identity and structural completion

Validate original WAL count, digest and contiguous ordinals before reducing to
the final mutation per `(object,key)`, sorted canonically. Live and recovery use
one reducer. The retained install identity is `(TxnId, final logical ordinal)`
within that object/key, never transaction chronological order.

- Same identity and same logical value/tombstone: allocation-free logical no-op.
- Same identity with different logical content: corruption.
- One transaction's different identity cannot replace its own retained effect.
- Older recovery cannot replace newer committed state by comparing page LSNs.
- Delete leaves an MVCC tombstone/replay evidence, not raw erasure.
- Aborted predecessors are bypassed through undo; unknown owners fail closed.

Logical idempotence is not proof of completed structural installation. A leaf
B-link publication may precede parent/root work. The installer must finish that
work before readiness, and a retained logical effect must not silently discharge
an unresolved structural obligation. Start with coarse structural coordination,
reserved/prebuilt split work and explicit completion ownership. Do not introduce
a durable installation ledger merely to recover arbitrary uncheckpointed spill.

Preparation can leave unreachable undo or overflow allocations on failure.
Allocation-free repeats apply to retained completed effects, not to recovery
restarted from an older checkpoint. See the retry-space contract below.

## 5. Exact page dependencies are not logical replay completion

Each captured image carries at least its highest required WAL LSN and referenced
undo VersionId. Attach bytes and dependencies while the exact page remains pinned
and writer-owned; post-hoc attachment can lose to writeback. Copied split state,
parents/new roots and immutable overflow pages inherit conservative requirements.
An image reaches working storage only after both durability frontiers cover it.

A maximum page LSN cannot skip logical redo. Independent keys can install out of
LSN order; a page requiring 200 can lack effect 100, and splits move effects.
Replay skips only a complete checkpoint prefix or validated per-effect evidence.
Frame versions, dirty bits, root generations and process-local dependency tables
are not substitutes for that authority.

## 6. Complete checkpoint and restart authority

The first persistent model is a structurally complete checkpoint plus committed
logical WAL suffix:

1. Close new committing-mutation admission; let admitted work reach installation
   and contiguous visibility. Establish the exact `(visible CSN, decision LSN)`
   cut after drain. Quiesce structural/freezing/GC/allocation mutations during
   capture. A failed drain fences; zero members alone is not a safe cut.
2. Capture reference closure: child/right links; leaf keys, fences, separators,
   value chains and successors; roots, object lifecycle and allocation high-water
   marks; placement map; reachable undo, owner outcomes and retained history.
3. Satisfy image, WAL, undo and directory barriers. Publish a store-bound complete
   manifest only after dependencies are durable. Retain prior complete authority
   and its dependencies until candidate publication succeeds.
4. Reopen the selected complete authority, re-establish durable file/directory
   boundaries, validate/synchronize the retained suffix, replay canonical effects
   in contiguous CSN order, validate resulting references, then expose visibility.

Snapshot/history lease registration and capture of retention floors share runtime
authority. Readers may continue during capture, but capture cannot wait indefinitely
for every reader pin: use guarded stable image capture and bounded pin lifetimes.
No byte latch spans output suspension. Reuse unchanged immutable images where valid.
One cohesive placement/reference validator rejects foreign, duplicate, dangling,
spliced and corrupt references without quadratic map work.

Every reachable transaction owner needs a durable checkpoint outcome or safe
freezing before reclaiming its decision. Retain install identity until the replay
horizon permits removal. Allocation metadata prevents reuse of live transaction,
object, version and page IDs. The current undo format has no front-truncation or
reuse protocol; never invent GC by truncating it ad hoc.

Recovery attempts have explicit working-allocation ownership. Restarting from the
same prior checkpoint must not consume unbounded undo/page arena space or force
reopen to scan all abandoned images forever. Implement disposable recovery working
epochs with reclaimable unreferenced allocations, or qualify an explicit bounded
retry-space policy before persistent exposure. Reclamation requires closure over
current/prior checkpoints and reader/history leases; do not truncate shared undo
or ignore corrupt referenced images. Append-only/no-reuse is an acceptable bounded
prototype, not a large-history or unlimited-retry claim.

Blocking capture is a correctness baseline, not a brief-pause promise. Measure
graph traversal and materialization versus database size/dirty fraction. Replace
it with a coherent epoch or qualified physiological/structural logging if cutover
pause budgets require it; do not retain long pauses by inertia.

## 7. Formats and qualification

Current experimental WAL/undo outer framing and MVCC envelope are version 2.
Ordered-page overflow requires a new experimental format, not silently interpreting
v4 bytes. Legacy production formats are unaffected. Only provably incomplete final
appends are repairable; complete bad headers, unknown kinds/flags, checksum failures
and invalid retained references fail closed.

Frame-at-a-time undo scanning is not bounded-history recovery: undo indexes,
retained WAL output and pending/terminal owner bookkeeping still grow with history.
Streaming/indexing, retention and retry-space limits are required before that claim.

Qualify through real runtime entry points, not only codecs:

- clean pre-decision conflicts/size/resource refusal produce no decision;
- certify point/absence/range/catalog/constraint dependencies against a paused,
  earlier accepted but uninstalled decision; retain evidence under pressure;
- unwind/cancel/error at append, WAL/undo sync, partial install, structural finish,
  status/readiness and visibility; fence before ownership release and wake waiters;
- later readiness cannot acknowledge across an earlier gap; checkpoint drain cannot
  outrun unresolved completion or prevent protected progress;
- admitted layout and future undo conversion work under split/root/overflow pressure;
- out-of-order same-page installers, dependency inheritance, image/writeback races
  and delayed loads cannot resurrect an acknowledged predecessor;
- candidate failure preserves prior authority; missing/foreign/spliced chains,
  outcomes and allocation metadata fail closed;
- crash during recovery, two consecutive reopens and a subsequent normal mutation
  preserve outcomes within stated memory/storage budgets;
- long snapshots, retained prior authority, catalog/object retirement and reclamation
  never invalidate admitted views.

The [qualification skill](../../.agents/skills/vnext-qualification/SKILL.md) owns
the reusable procedure; the [plan](../plans/storage-kernel-vnext.md#immediate-sequence)
owns work order and open evidence, not this ADR.
