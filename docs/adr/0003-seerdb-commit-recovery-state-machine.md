# ADR 0003: SeerDB commit outcome and recovery ownership

- **Status:** accepted outcome/failure contract; vNext ordering is owned by ADR 0014
- **Scope:** durable transaction outcome, recovery ownership and ambiguous I/O
- **Depends on:** [ADR 0001](0001-seerdb-transaction-contract.md) and
  [ADR 0002](0002-seerdb-mvcc-version-storage.md)
- **Refined by:** [ADR 0014](0014-vnext-installation-and-recovery.md)

## Decision

A complete durable commit decision is irrevocable transaction-outcome authority.
It does not by itself establish visible completion. Synchronous acknowledgment
requires the selected durability barriers, complete authoritative installation
and contiguous visibility coverage. Recovery must derive the same logical
outcome even if materialization or response delivery was interrupted. Any
asynchronous receipt must explicitly distinguish its non-durable state from a
synchronous acknowledgment. Read-only work may complete at its captured snapshot
without assigning a new CSN.

A proven clean refusal before WAL authority remains abortable. An uncertain WAL
attempt or unresolved durable decision requires recovery, not a fabricated
abort. `RecoveryRequired` describes the runtime/caller's need to resolve state;
it is not a committed or aborted transaction outcome.

The former prepared-prefix/two-barrier sketch is superseded for vNext. ADR 0014
owns WAL/undo/install ordering, replay identity, checkpoint closure and retained
history; this ADR does not maintain a second protocol or execution sequence.

## State ownership

- The transaction manager owns transaction state, intents, snapshots and release.
- The log owns record order, framing, synchronization and durable LSNs.
- Recovery interprets durable outcomes and restores complete authoritative state.
- Materialization owns physical images and checkpoint/page-map publication; it
  cannot independently advance logical transaction visibility.
- Committed-change decoding preserves transaction boundaries and historical
  effects; fetching present-day rows is not historical replay.

One persistent runtime binds these services and their failure authority.
Components report outcomes rather than infer success from file existence or
maintain independent commit frontiers.

## Failure outcomes

| Boundary | Required outcome |
|---|---|
| Proven clean refusal before WAL authority | No commit decision; prior committed state remains; caller may abort/retry |
| Uncertain append/sync | Fence the runtime; recovery validates whether a complete decision exists |
| Durable decision before installation/visibility | Committed outcome; finish or recover every authoritative effect; no success ACK yet |
| Interrupted checkpoint publication | Prior complete checkpoint remains retained; publish no partial authority |
| Interrupted response after completion | Commit remains committed; retry safety requires application/request semantics |
| History or image reclamation | Every retained checkpoint, reader and recovery dependency remains reachable |

Only a provably incomplete final append is repairable as a torn tail. Complete
bad framing, checksums, contradictory decisions or missing retained references
are corruption. Reopening must re-establish durability before exposing recovered
frontiers. No automatic fallback may silently discard acknowledged history.

## Failure lifecycle

Uncertain external effects fence further admission and wake completion/drain
waiters. Fencing precedes release of unresolved ownership. Caught unwinding in a
continuing process obeys the same responsibility contract; an explicit
process-abort profile is a different hosting policy. Cancel/disconnect after a
durable decision does not make commit responsibility disappear.

Diagnostics distinguish transaction identity, last phase, decision position,
affected objects, resolved outcome and the next safe action. Callers do not
blindly retry against a fenced handle or treat an ambiguous result as an abort.

## Qualification and formats

The [vNext plan](../plans/storage-kernel-vnext.md) owns unfinished gates; the
[qualification procedure](../../.agents/skills/vnext-qualification/SKILL.md)
owns the recurring checks. Cross durability, installation, checkpoint and
reclamation boundaries through the real runtime and verify logical state after
two consecutive reopens, not merely successful directory opening.

Experimental formats may change before stability. Fail closed on unsupported
bytes or use an explicit validated migration; no silent dual-format fallback.
The existing engine remains a qualification reference, not the permanent
physical architecture or an infallible semantic specification.
