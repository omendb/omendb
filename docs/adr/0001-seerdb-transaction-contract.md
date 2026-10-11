# ADR 0001: SeerDB transaction and durability contract

- **Status:** accepted transaction semantics; physical boundary refined by ADR 0013 and vNext commit protocol by ADR 0014
- **Scope:** OmenDB ↔ SeerDB boundary
- **Supersedes:** the global expected-`CommitId` CAS batch as the final
  production transaction API

## Decision

SeerDB supplies OmenDB's shared transaction/storage semantics and remains
SQL-agnostic. [ADR 0013](0013-storage-kernel-and-access-methods.md) supersedes
this ADR's original universal ordered-KV boundary: the following is the ordered
access-method facade, not the only physical representation:

```text
TreeId + unsigned-lexicographically ordered key bytes + opaque value bytes
```

The API must support first-class tree lifecycle, multi-writer transactions,
point/range cursors, atomic multi-tree mutation, snapshot visibility, conflict
observations, explicit durability, and a restartable committed-change stream.
The temporary `StorageKernel` seam was deleted under [ADR 0005](0005-delete-storage-kernel-seam.md).
The replacement must not revive it as a permanent engine matrix.

## Identity rules

Never overload one number for different ordering domains:

```text
TxnId       transaction identity
CommitSeq   logical visibility/commit order (CSN)
Lsn         durable log position
TreeId      stable logical ordered-tree identity
PageId      stable physical/logical page identity
PageVersion physical page incarnation
```

A successful synchronous commit returns at least `{CommitSeq, durable Lsn}`.
OmenDB must not maintain a second independent commit frontier.

## Transaction invariants

1. A transaction reads one snapshot according to its isolation mode. `READ
   COMMITTED` may refresh the statement snapshot without losing its writes;
   repeatable-read/SI keeps one transaction snapshot.
2. A committed transaction makes all mutations across all touched trees visible
   together. An aborted transaction makes none visible.
3. Snapshot isolation rejects conflicting writes to the same logical key. A
   conditional insert also conflicts with a concurrent insert into the same
   absent key/gap.
4. Point reads, absent-key reads, range reads, and writes produce enough
   dependency information for the decision-ordered certifier in ADR 0014 without
   putting SQL semantics into SeerDB. Mandatory constraint/catalog observations
   remain certified even if weaker user isolation modes are introduced.
5. Older snapshots can read previous key/value versions and dropped trees until
   retention-safe reclamation. Physical page versions are not transaction MVCC.
6. A borrowed record reference cannot outlive its page/frame guard. Batch
   execution copies values that survive the guard lifetime.

## Commit state machine

```text
Active
  -> Validating / Prepared
  -> WalAppended
  -> DurableDecision
  -> Visible (contiguous CSN coverage)
  -> Released

Proven clean pre-WAL refusal -> remains abortable
Uncertain WAL attempt or unresolved durable decision -> RecoveryRequired
```

For synchronous local durability, the acknowledgement invariant is:

```text
acknowledged commit => WAL and required publication state survive the
                       selected local durability barrier
```

A durable decision is irrevocable even before visibility. Readiness publication
alone does not acknowledge success. [ADR 0014](0014-vnext-installation-and-recovery.md)
owns the exact vNext ordering and completion/failure protocol.

Recovery must derive the same committed/aborted outcome from durable records;
it must never expose a partial multi-tree transaction.

## Crash-state table

| Crash point | Required reopen result |
|---|---|
| before WAL reservation | old state; transaction absent |
| after WAL append, before durable barrier | old state or a recoverable complete committed prefix; never partial visibility |
| after WAL durable, before commit publication | transaction outcome resolved from WAL; no partial visibility |
| after page/blob writes, before authority publication | old state or complete new state; unreachable allocations are reclaimable |
| after authority publication, before cleanup | new state; cleanup may retry |
| during page-map/checkpoint publication | previous valid root or complete new root; corrupt candidate refused |
| during GC/version reclaim | all versions reachable by retained snapshots remain readable |

The exact WAL/page record layout remains an implementation choice, but every
arrow in this table needs a deterministic fault test before format stability is
claimed. The target logical version/undo ownership and retention rules are
specified in [ADR 0002](0002-seerdb-mvcc-version-storage.md); the detailed
commit/recovery protocol is specified in [ADR 0003](0003-seerdb-commit-recovery-state-machine.md).

## Snapshot and change position

The target export is a retained handoff containing snapshot CSN `X`, a logical
consumer cursor/restart position, source incarnation and atomically registered
snapshot/history leases. A position pair alone is not a copyable snapshot or a
retained stream. The current `snapshot_export` reports a durable position only.

[ADR 0013 §11](0013-storage-kernel-and-access-methods.md#11-snapshotbootstrap-is-a-retained-handoff-not-just-two-numbers)
owns the complete contract, including historical effects, transaction-atomic
consumer progress and separate physical retention floors. Physical WAL/page
formats remain private to recovery and physical replication.

## Consequences

- The current serialized-writer/generation implementation is a baseline to
  qualify, not an architecture to optimize into permanence.
- The next implementation can start with full before-image undo and a simple
  transaction-status table, then benchmark deltas, freezing, certifiers,
  buffer translation, and commit I/O.
- OmenDB owns SQL row/index encoding and maps returned `TreeId`s into its
  catalog. SeerDB never learns table, index, NULL, or schema meaning.
- `omen-olap`, backups, CDC, and future replication share the snapshot/change
  position contract without sharing physical storage formats.
