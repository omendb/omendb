# ADR 0004: Group-commit publication lane

- **Status:** accepted and implemented as the current transactional baseline;
  hardware-specific commit scheduling remains benchmark-gated
- **Scope:** SeerDB `TransactionDatabase` commit pipeline and the `DB`
  group-publication primitive
- **Depends on:** [ADR 0003](0003-seerdb-commit-recovery-state-machine.md)
- **Refined by:** [ADR 0006](0006-deployment-storage-and-durability.md)

## Context

The first transactional implementation serialized every commit end to end:
one transaction at a time held the runtime database mutex across conflict
validation, before-image appends, the version-store sync, and the WAL sync of
`commit_batch_at`. Throughput was bounded by two fsyncs per transaction, and
the whole-database expected-base CAS made concurrency impossible above the
engine. The same single-lane shape existed inside `DB`: one physical batch
published exactly one logical commit.

The group lane fixed that correctness/performance shape and remains the
qualified implementation. It is not, however, a promise that every deployment
or future device must use group commit. Modern local NVMe can reward parallel
small durable writes, while replicated/cloud deployments naturally batch work
around quorum log appends. ADR 0006 makes the durable transaction decision the
stable contract and leaves commit scheduling behind a measured deployment
policy.

## Decision

The **current implementation** uses a two-phase pipeline with one ordered
publish lane.

1. **Stage (concurrent).** A committer takes the prepare mutex, validates
   against published state *and* queued-but-unpublished work (key overlay,
   tree overlay, queued range writes), appends before-images, and enqueues
   data-only mutations. It holds no lock while waiting on I/O.
2. **Publish (serialized).** Committer threads become leader in turn. The
   leader swaps out the staged queue **before** acquiring the database handle
   (staging waits on the database lock while holding prepare, so taking the
   database lock first would deadlock), assigns each member its sequence as
   `head + position + 1`, installs the per-member status record plus the
   member's **stage-time-encoded** change record under its assigned key
   (encoding and the 16 MiB record bound are validated during staging, so the
   serialized lane never re-encodes or panics), chains the candidate states,
   syncs the version store once and the WAL once, and publishes **one
   authority frame** covering all members.
3. **Control-plane writers join the lane.** Tree reservations, retention-lease
   writes, change GC, and version GC drain staged work before their own inline
   single-commit publications, so every consumer of a sequence number passes
   through one ordered lane.

Engine support: `DB::commit_group_at(expected_commit_id, batches)` accepts
*k* logical batches, performs one CAS, one admission check, one WAL sync, and
publishes an authority frame whose explicit `commit_seq` advances by *k*.
`CommitId` (generation) and `CommitSeq` (logical order) are distinct counters;
callers must never use one as the other.

Any replacement commit scheduler must preserve:

- one unambiguous logical commit order;
- atomic multi-tree visibility;
- the durable-decision/recovery contract from ADR 0003;
- explicit `{CSN, LSN}` results;
- deterministic failure semantics;
- the same committed-change ordering visible to CDC/replication.

## Failure semantics

- Version-store sync failure precedes all publication: certain abort, no
  fence, every member may retry on the same handle.
- WAL or authority-frame failures keep engine fence semantics: uncertain,
  every member reports "may have committed" at its assigned sequence; reopen
  resolves one atomic outcome for the whole wave.
- Clean refusals (backpressure, capacity preflight) fail the whole wave
  retryably with no fence.

These are current implementation semantics. A future log-authoritative path may
remove a separate version/page sync from acknowledgement; in that case its
fault matrix must be restated and qualified rather than silently inheriting
this ordering.

## Consequences

- Sync cost is currently amortized across the group; writer CPU (validation,
  staging) runs outside the critical section.
- Readers still block during parts of a wave's install; reader/publisher
  separation and page-level multi-writer materialization remain open work.
- The lane is a proven scheduling skeleton and fallback, **not the final
  hardware policy**.
- Local-NVMe autonomous/parallel commit, adaptive batching, and regional quorum
  logging are legitimate successors when their benchmark plus recovery matrix
  beats this baseline.
- No physical scheduling experiment may change transaction semantics merely to
  win a benchmark.
