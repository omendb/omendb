# ADR 0004: Group-commit publication lane

- **Status:** accepted; implemented for the transactional slice
- **Scope:** SeerDB `TransactionDatabase` commit pipeline and the `DB`
  group-publication primitive
- **Depends on:** [ADR 0003](0003-seerdb-commit-recovery-state-machine.md)

## Context

The first transactional implementation serialized every commit end to end:
one transaction at a time held the runtime database mutex across conflict
validation, before-image appends, the version-store sync, and the WAL sync of
`commit_batch_at`. Throughput was bounded by two fsyncs per transaction, and
the whole-database expected-base CAS made concurrency impossible above the
engine. The same single-lane shape existed inside `DB`: one physical batch
published exactly one logical commit.

## Decision

Publication is a two-phase pipeline with one ordered publish lane.

1. **Stage (concurrent).** A committer takes the prepare mutex, validates
   against published state *and* queued-but-unpublished work (key overlay,
   tree overlay, queued range writes), appends before-images, and enqueues
   data-only mutations. It holds no lock while waiting on I/O.
2. **Publish (serialized).** Committer threads become leader in turn. The
   leader swaps out the staged queue **before** acquiring the database handle
   (staging waits on the database lock while holding prepare, so taking the
   database lock first would deadlock), assigns each member its sequence as
   `head + position + 1`, builds per-member status/change records, chains the
   candidate states, syncs the version store once and the WAL once, and
   publishes **one authority frame** covering all members.
3. **Control-plane writers join the lane.** Tree reservations, retention-lease
   writes, change GC, and version GC drain staged work before their own inline
   single-commit publications, so every consumer of a sequence number passes
   through one ordered lane.

Engine support: `DB::commit_group_at(expected_commit_id, batches)` accepts
*k* logical batches, performs one CAS, one admission check, one WAL sync, and
publishes an authority frame whose explicit `commit_seq` advances by *k*.
`CommitId` (generation) and `CommitSeq` (logical order) are distinct counters;
callers must never use one as the other.

## Failure semantics

- Version-store sync failure precedes all publication: certain abort, no
  fence, every member may retry on the same handle.
- WAL or authority-frame failures keep engine fence semantics: uncertain,
  every member reports "may have committed" at its assigned sequence; reopen
  resolves one atomic outcome for the whole wave.
- Clean refusals (backpressure, capacity preflight) fail the whole wave
  retryably with no fence.

## Consequences

- Sync cost amortizes across the group; writer CPU (validation, staging)
  runs outside the critical section.
- Readers still block during a wave's install; reader/publisher separation
  requires page-level MVCC and remains future work.
- True page-level multi-writer installation is the next stage; this lane is
  its scheduling skeleton, not its replacement.
