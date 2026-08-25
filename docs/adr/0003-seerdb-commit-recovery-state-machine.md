# ADR 0003: SeerDB commit and recovery state machine

- **Status:** accepted target architecture; implementation pending
- **Scope:** transaction commit, durable outcome, recovery, and ambiguous I/O
- **Depends on:** [ADR 0001](0001-seerdb-transaction-contract.md) and
  [ADR 0002](0002-seerdb-mvcc-version-storage.md)

## Decision

SeerDB treats a transaction's durable commit decision as the visibility
boundary. Data/version records may be appended before that decision, and
physical pages may be materialized after it, but no reader may expose a
transaction unless recovery can establish its committed CSN from a complete
durable decision.

The state machine is:

```text
                 +------------------+
                 |                  v
Active -> Validating -> Prepared -> WalDurable -> Committed(CSN) -> Released
  |          |             |             |
  +--------> Aborted <-----+-------------+
                              uncertain I/O
                                   |
                                   v
                            RecoveryRequired
```

`RecoveryRequired` is a caller-visible state for an ambiguous operation. It is
not an outcome. The caller must reopen or otherwise invoke the recovery
boundary before deciding whether to retry an application operation.

## State ownership

- The transaction manager owns state transitions, write intents, active
  snapshot registration, and release.
- The WAL manager owns record ordering, framing, sync barriers, and LSNs.
- The recovery manager owns interpretation of durable transaction decisions
  after restart.
- The page publisher owns physical checkpoint/materialization and page-map
  publication. It cannot independently publish transaction visibility.
- The logical change stream owns decoding committed generic tree/key events
  from durable transaction records; it does not rescan tables.

Each layer exposes outcomes to the next layer rather than duplicating a commit
frontier or inferring success from file existence.

## Commit protocol

For a transaction with snapshot `S`:

1. **Validating:** acquire/validate logical key and range intents, check
   first-committer-wins conflicts, and run the configured dependency
   certifier.
2. **Prepared:** append the transaction's version/current-record records and
   generic change records. They reference `TxnId` and remain invisible.
3. **WalDurable:** make the complete prepared prefix durable according to the
   selected durability mode. A prepared prefix without a durable commit
   decision is not visible.
4. **Committed(CSN):** append a commit decision containing the CSN, transaction
   identity, covered record/change range, and durable position. Make that
   decision durable before synchronous acknowledgement.
5. **Released:** publish the in-memory committed status, release intents, and
   unregister the ordinary snapshot. Physical checkpoint and GC may follow.

The commit decision is one transaction boundary across all touched trees. A
read-only transaction can complete at its captured snapshot without allocating
a new CSN.

An asynchronous durability mode may return a clearly different non-durable
status, but it must never label a transaction as synchronously durable. The
public result always distinguishes logical CSN from LSN.

## Recovery algorithm

Recovery performs these steps before accepting new transactions:

```text
select latest valid physical manifest/checkpoint
 -> restore page mapping and transaction metadata
 -> scan/replay WAL after the checkpoint
 -> validate record framing, transaction boundaries, CSN order, and LSNs
 -> resolve every prepared transaction from its durable decision
 -> discard/queue abandoned prepared state with no decision
 -> validate current-record/undo/blob references
 -> register durable snapshot/change-consumer leases
 -> expose the recovered frontier
```

A complete durable commit decision makes the transaction committed, even when
its physical page materialization was interrupted; WAL replay or a later
checkpoint must make the logical state available. A prepared transaction with
no durable decision is aborted/ignored and its allocations become GC work. A
truncated or contradictory decision is corruption, not an implicit commit.

## Crash matrix

| Failure point | Reopen result |
|---|---|
| before prepare append | transaction absent; prior state remains |
| during prepared record append | prior state or a validated incomplete prefix; transaction invisible |
| after prepared records, before durable barrier | transaction invisible; incomplete records truncated/ignored |
| after prepared barrier, before commit decision | transaction invisible; prepared state is abort/GC work |
| during commit-decision append | transaction invisible unless a complete valid decision exists |
| after commit decision is durable, before page publication | transaction committed; replay/materialization completes it |
| after page publication, before manifest/authority publication | old or complete-new physical root; logical outcome remains unambiguous |
| after authority publication, before cleanup | committed state; cleanup is retryable |
| during version/undo GC | all versions reachable from retained snapshots remain readable |
| during WAL/segment reclamation | no required checkpoint, replica, CDC, or snapshot position is lost |

The old-or-new rule applies only where the operation is not yet logically
committed. Once the durable commit decision exists, reopening must not roll the
logical transaction back merely because cleanup or page publication was
interrupted.

## Ambiguous failure handling

Any error after an external write, sync, rename, or commit-decision attempt may
be partial or uncertain. The handle becomes fenced for further writes and
returns `RecoveryRequired` unless the operation can prove it failed before the
authoritative effect. Callers must not blindly retry the same transaction
identity against a fenced handle.

Recovery diagnostics must identify:

- transaction identity;
- last observed state;
- prepared and decision LSNs;
- affected tree/key range when available;
- whether the outcome is committed, aborted, or corrupt;
- the next safe action.

## Fault and simulation requirements

The implementation must provide deterministic seams for:

- short/torn writes and sync failure;
- crash before and after each WAL barrier;
- crash before and after commit-decision publication;
- page/blob publication interruption;
- recovery replay and truncation;
- version-store append and GC interruption;
- allocation/capacity refusal;
- change-consumer advancement and WAL retention.

Each fault case must reopen at least twice and verify the logical model, not
only that the directory opens. Tests must distinguish process refusal,
committed outcome, aborted outcome, corruption, and resource equivalence.

## Compatibility and migration

This is a new target protocol, not a promise to preserve the current v3
inline-chain format. Until the replacement passes its recovery gates, the
current format remains a transitional development format. A future format
version must fail closed on old bytes or use an explicit offline migration;
there is no silent dual-format fallback.
