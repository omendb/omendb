# ADR 0014: vNext decisions, installation and checkpoint recovery

- **Status:** accepted target protocol, revised 2026-10-13; the current component
  baseline is not an integrated, restart-safe runtime
- **Scope:** storage-kernel vNext, not the legacy production format
- **Depends on:** [ADR 0013](0013-storage-kernel-and-access-methods.md)
- **Refines:** ADRs 0003, 0006 and 0009. Durable-decision-first installation replaces
  speculative shared installation for this baseline; the complete-log target below
  replaces a permanent requirement for separately synchronized undo at every commit.

## Decision and rationale

Keep mutations private until their complete recoverable decision is durable.
One runtime owns certification, ordering, completion, visibility, resources,
history and restart authority. Durability, logical visibility and physical
materialization are separate: a durable decision is irrevocable, but synchronous
success also requires all authoritative effects installed and the contiguous
visibility frontier covering them. Working spill pages are not restart authority.

The current code synchronizes decision WAL and then required undo before installation.
That is a real dependency of the existing format, not a desirable permanent
latency floor. The target uses one recoverable log boundary for ordinary mutations,
with recent history resident and history/page materialization off the ACK path.
This requires new recoverability and retention evidence before any barrier is removed.
The [plan](../plans/storage-kernel-vnext.md) owns status and implementation order.

## 1. Runtime-owned transaction and completion capabilities

One persistent runtime owns store incarnation/exclusion, transaction/status/visibility,
snapshot/catalog/object/history leases, intents/certification, log/materialization,
resource reservations, completion responsibility and checkpoint admission. Optional
component attachment remains a test seam, not a supported persistent entry point.
All direct/server/read/snapshot operations share the failure boundary and recheck
at bounded work intervals. Recovery-required state fences reads unless a separately
proved safe prior-read contract exists.

Workload admission is transaction-grain and precedes snapshot/intents. Keep that
reservation through transaction lifetime; subsequent statements or completion must
not queue behind work needing the same transaction's resources. Checkpoint membership
is different: it covers committing mutation through installation and visibility.
Checkpoint drain cannot block logging/installers/cleanup needed by its own members.
Structural changes, freezing, GC and allocation participate in capture authority.

Arm commit-attempt responsibility **before possible decision append**, not only after
receiving a ticket. Release it only after explicit visible completion, proven clean
pre-decision refusal, or terminal fencing established before unresolved intents,
reservations and drain membership are released. A caught unwind must fence and wake
visibility waiters. An abort-on-panic host is a different explicit contract, not a
library assumption. Cancellation/disconnect can discard a response, not an irrevocable
decision. Unknown outcomes need durable request identity/outcome lookup before safe retry.

## 2. Decision-ordered optimistic certification

Initial serializable writers use fixed snapshots and conservative optimistic
certification. Freeze point/absence, range/gap, catalog/name/enforcement/lifecycle
and required constraint observations. Validate against every earlier accepted writer
after the snapshot, including effects not yet synchronized, installed or visible.
Acceptance, CSN assignment and publication of conflict evidence share the decision
sequencer. No validate/append or decision/summary gap may admit another writer.
Uncertain append fences rather than leaving a usable sequencer with missing evidence.

Prepare predecessors, encode mutation bodies and inspect historical summaries
outside the short lane; recheck the remaining decision tail inside it. Do not hold
it over page reads, full-batch encoding, table scans or durability waits. An ordered
append/reservation mechanism must preserve sequence and crash framing even when
preparation and I/O are parallel. One logical log authority does not require one
physical worker forever; reservation holes cannot be silently reused or acknowledged.

Retain indexed CSN-ordered conflict summaries after the oldest snapshot that may
still write. Budget summaries and dependency sets separately from row-version
retention. Pressure can conservatively coarsen, spill, refuse new work or explicitly
expire an abortable transaction; it cannot discard evidence and certify success.
Never pressure-abort an unresolved accepted decision.

This chooses decision order as the serial order and may reject histories accepted
by another serial order. It is not PostgreSQL SSI. Read-only snapshots can serialize
at their prefix only when that prefix consists of appropriately certified writers;
mixed isolation must explicitly revisit the exemption. Conflict indexes need not
survive restart when old live transactions end before new admission.

Use low-overhead optimistic access when uncontended. Queued hot-key intents may
reduce repeated aborts, but a fixed snapshot cannot silently refresh after waiting.
READ COMMITTED statement refresh/rechecks and serializable dependencies must follow
[ADR 0007](0007-runtime-execution-and-contention.md), not an ad hoc retry loop.
[FoundationDB](https://apple.github.io/foundationdb/developer-guide.html#transactions-in-foundationdb)
and [PostgreSQL](https://www.postgresql.org/docs/18/transaction-iso.html) illustrate
different valid isolation trade-offs; neither dictates OmenDB's size/duration limits.

## 3. Admission and durable-decision-first installation

### Current two-barrier component baseline

The existing coordinator validates/reduces the original mutation stream, acquires
intents, prepares predecessor undo, appends a decision, synchronizes WAL, synchronizes
required undo, installs effects, then completes contiguous visibility. Until replaced,
page dependencies must enforce both durability frontiers. Calling `append_validated`
does not by itself prove read/range certification. Current empty-leaf preflight and
optional runtime admission do not satisfy the target below.

### Complete-log target

1. Freeze canonical effects and logical dependencies; acquire intents in a safe order.
2. Validate predecessor owners, logical limits and install/replay representability.
   Reserve log/history/overflow/structural storage, peak pins, scratch, summaries
   and completion capacity before a decision can become irrevocable.
3. Prepare recoverable effects and history outside the ordering lane. A retained
   checkpoint plus the complete committed log must reconstruct current records,
   required predecessor history, owner outcomes and all authoritative participants.
4. Certify, order and append the complete transaction/decision under the sequencer
   contract. Make the decision durable on the owned log, or required quorum in an
   explicitly replicated profile.
5. Install every authoritative effect, revalidating exact prepared predecessors;
   attach dependencies while guarded and finish parent/root/overflow obligations.
6. Publish owner status/readiness, advance the contiguous visible prefix, wait for
   coverage, then acknowledge and release completion ownership.
7. Materialize pages/history and publish checkpoints independently, retaining the
   log and prior authority until their recovery and reader dependencies are covered.

History may be encoded in the log or reconstructed from retained checkpoints and
ordered replay. This choice must be proved for updates/deletes, schema/object changes,
long retained snapshots, CDC, backup and crash during recovery. Logical version
identities cannot depend on unpersisted raw undo-file offsets. Checkpoints either
retain required history/outcomes or provide a recoverable mapping; log reclamation
waits for that closure. Measure extra log bytes versus barriers and history-read cost.
A separate history file may still exist, but ordinary ACK must not wait on it merely
because the baseline did. Large external payloads can require extra durable dependency
barriers; never advertise a universal single-fsync guarantee.

Use bounded adaptive group commit without a mandatory timer delay for a lone writer.
Compare autonomous/parallel durable appends once semantics and matched hardware
measurements justify them. [Autonomous commit on NVMe](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/latency.pdf)
is evidence for an experiment, not permission to weaken durability or ordering.

### Bounded progress and logical limits

Reserve against actual fenced layouts and worst-case structural work, not only an
empty leaf observed at preflight. Provide a protected installer slow path; healthy
admitted decisions cannot depend on unlimited `NoVictim` or allocation retries.
Do not hold page byte latches or arbitrary reader pins across durability waits.
Large transactions need bounded private-write spilling and completion accounting,
not an implicit all-in-RAM requirement.

Deterministic refusal occurs before device entry. The current original WAL batch
must fit one segment record region and the LSN domain; check that before append.
A future fragmented transaction format may span segments with a complete final
decision/count/digest and incomplete-prefix recovery. Do not turn the current
single-segment implementation into a permanent general-purpose transaction limit.

Records must remain representable as future undo/history on update/delete. The
current 16 MiB undo framing limit and other codec limits must agree with logical
admission. Overflow does not bypass framing or decode-memory limits. Extend a
limiting format deliberately rather than shrink legitimate records to pass tests.

## 4. Logical effect identity and structural completion

Validate original record count, digest and contiguous ordinals, then reduce to the
final effect per `(object,key)` using one live/recovery reducer. The retained install
identity is `(TxnId, final logical ordinal)` within that object/key, not page LSN
or transaction chronological order.

Identical retained identity/content is an allocation-free logical no-op; conflicting
content is corruption. One transaction's different identity cannot overwrite its own
retained effect. Older recovery cannot replace newer state by comparing page LSNs.
Deletes retain MVCC tombstone/replay evidence. Bypass aborted predecessors through
history; unknown owners fail closed.

Logical idempotence does not discharge incomplete structural work. A leaf B-link
publication can precede parent/root repair; the installer must finish it before
readiness. Start with explicit completion ownership and reserved/prebuilt split work,
not a new durable installation ledger merely to recover arbitrary uncheckpointed spill.
Unreachable preparation allocations are reclaimed through an explicit lifecycle.

## 5. Exact page dependencies are not logical replay completion

Attach bytes and recovery dependencies while the exact page remains pinned and
writer-owned. Split copies, parents, new roots and immutable extents inherit the
needed dependencies. Current images require WAL plus referenced undo frontiers;
the complete-log design can change those dependencies only after its recovery proof.
Post-hoc attachment can lose a race with writeback.

A page requiring LSN 200 may still lack an independent effect at 100. Splits also
move effects. Replay skips only a complete checkpoint prefix or validated logical
effect evidence, never a maximum page LSN, dirty bit or process-local table.

## 6. Complete checkpoint and restart authority

The first persistent model is a structurally complete checkpoint plus retained
committed logical WAL suffix. Close new committing mutations, drain already admitted
installation/visibility and establish the exact `(visible CSN, decision LSN)` cut.
Quiesce structural/freezing/GC/allocation mutations while capturing reference closure:
roots, object lifecycle, child/right links, fences/separators/value chains, allocation
high-water marks, placement map, owner outcomes and retained history.

Make all dependencies durable before atomically publishing a store-bound manifest.
Preserve prior complete authority until candidate success. Reopen validates the
selected checkpoint, re-establishes durable boundaries, validates/synchronizes the
retained suffix and replays complete effects in order before exposing visibility.
A successful open is not a substitute for reference verification.

Snapshot/lease registration and retention-floor capture share authority. Readers may
continue, but checkpoint capture cannot wait indefinitely for arbitrary pins. Use
stable guarded capture, bounded pins and reusable immutable images. One nonquadratic
validator rejects duplicate/foreign/dangling/spliced/corrupt references.

Before decision-log reclamation, every reachable owner has a durable outcome or safe
freezing, all required history is covered and install identities remain until their
replay horizon permits removal. Allocation state prevents live ID reuse. Current undo
has no safe front-truncation/reuse protocol; do not invent one by truncating files.

Recovery owns disposable working epochs or another qualified bounded retry-space
policy. Repeated crashes from the same checkpoint cannot cause unlimited arena/history
growth or scanning of abandoned images. Reclamation respects current/prior checkpoints
and reader/history leases. Append-only allocation is a bounded prototype, not an
unlimited-history or unlimited-retry contract.

Blocking graph capture is only the initial correctness baseline. Measure pause versus
dirty fraction and database size. A coherent epoch/incremental or physiological-log
checkpoint must replace it if cutover pause budgets require that; "async persistence"
does not excuse a stop-the-world checkpoint or unbounded restart.

## 7. Formats and qualification

Current WAL/undo framing and MVCC envelope are v2; current ordered pages are v4.
Overflow and complete-log/history changes require explicitly versioned experimental
formats. Legacy product bytes are unaffected. Only provably incomplete terminal
appends are repairable; complete bad headers/checksums, unknown kinds/flags and
invalid retained references fail closed.

Qualification exercises the real runtime: pre-decision clean refusal; certification
against paused accepted writers; unwind/cancel/error at every append/sync/install/
structural/visibility boundary; blocked earlier completion with later readiness;
checkpoint drain and pressure; feasible splits/overflow/future history; delayed loads
and writeback; prior-authority preservation; corruption and missing references;
crash during recovery, two reopens and a subsequent normal mutation; retained readers
and reclamation. Complete-log qualification additionally kills after decision durability
but before any history/page flush and proves every required retained view.

Bound recovery memory as well as payload reads: indexes, owner tables, replay output
and retained history can still grow with log length. Measure serial-lane service,
barrier count, ready-to-visible waits and checkpoint pauses before claiming modern
OLTP performance. The [qualification skill](../../.agents/skills/vnext-qualification/SKILL.md)
owns procedures; the [plan](../plans/storage-kernel-vnext.md#immediate-sequence) owns
sequence and open evidence. Documentation changes close no implementation gate.
