# ADR 0002: SeerDB transactional MVCC and version storage

- **Status:** accepted target architecture; implementation pending
- **Scope:** SeerDB logical record visibility, transaction status, retention, and
  version garbage collection
- **Depends on:** [ADR 0001](0001-seerdb-transaction-contract.md)

## Decision

SeerDB will separate transactional MVCC from physical page versioning. A
current ordered-tree record points to an append-oriented logical version/undo
record. The version record stores the prior logical state and a link to the
previous version. The user value remains opaque bytes (or a SeerDB blob
reference); SeerDB does not encode SQL rows, columns, or index meaning.

The first implementation uses complete before-images. Delta encoding,
compression, and co-location are later optimizations and must not change the
visibility contract.

Conceptually:

```text
ordered-tree current record
  owner: TxnId | frozen CommitSeq
  flags: live | tombstone | overflow
  current opaque value or blob reference
  undo_head: VersionId | none

append-oriented version record
  creator/status reference
  prior owner/flags
  prior opaque value or blob reference
  previous VersionId | none
```

`VersionId` is a logical version-store identity. It is not a physical page
offset, WAL LSN, or public OmenDB identifier.

## Ownership and authoritative state

Each guarantee has one owner:

| Guarantee | Owner | Authoritative representation |
|---|---|---|
| transaction identity and state | transaction manager | durable transaction-status records plus recovery state |
| current key/value state | ordered-tree record | current record and its undo head |
| snapshot visibility | MVCC visibility layer | transaction status + CSN frontier |
| active ordinary snapshots | transaction manager | process-local registry of `(TxnId, snapshot CSN)` |
| explicit retained snapshots | retention manager | durable lease registry with snapshot CSN and restart LSN |
| version reclamation | MVCC GC | computed watermark and version reachability |
| physical page liveness | page-map/physical GC | durable page mapping and physical retention metadata |
| WAL retention | WAL/consumer manager | checkpoint, replica, and change-consumer positions |

Physical page generations, root generations, and retained physical manifests
may protect bytes, but they are not transactional visibility metadata.
OmenDB must not maintain a second CSN frontier.

## Transaction status

A transaction has a distinct `TxnId` and follows:

```text
Active -> Committing -> Committed(CSN)
Active -> Aborted
Committing -> RecoveryRequired
```

A version owned by an active transaction is not visible to other snapshots. A
committed owner is visible according to its CSN. An aborted owner is skipped,
and readers follow its prior version. Old committed transaction entries may be
frozen to a direct CSN once no retained snapshot or dependency can require the
transaction-status entry.

The status table is indirection, not a second source of visibility truth. Its
recovery result is derived from the durable commit/abort decision records and
is rebuilt or validated during open.

## Snapshot and retention watermarks

Every ordinary transaction registers its snapshot CSN at begin and releases
that registration on commit, abort, or drop. The registry stores individual
snapshots, not only a count, so reclamation can compute the oldest active
snapshot and diagnose the pinning transaction.

The MVCC reclamation watermark is the minimum of all applicable consumers:

```text
oldest active transaction snapshot
oldest explicit retained snapshot
oldest serializable/dependency retention point
oldest logical change-consumer position, when it pins versions
```

No version needed by a snapshot at or above that watermark may be reclaimed.
A durable retained lease survives process restart; an ordinary transaction
registration does not survive a crash.

Retention pressure is observable. SeerDB must expose at least oldest snapshot
age/CSN and bytes pinned by MVCC history separately from physical pages, blobs,
and WAL. Admission may reject or warn on configured pressure, but GC must not
silently invalidate a live snapshot.

## Visibility and mutation rules

For a read at snapshot CSN `S`:

1. inspect the current record;
2. resolve its owner through transaction status when necessary;
3. if the version is visible at `S`, return its value or logical absence;
4. otherwise follow undo links until a visible version or absence is found.

A write obtains a logical-key write intent, validates first-committer-wins
rules, appends the prior state to the version store before changing the current
record, and records the transaction's logical change identity. Inserts,
updates, deletes, and conditional inserts all use the same visibility rules.
Point and range observations produce dependency events for a later
serializability certifier; SQL uniqueness and schema semantics remain above
SeerDB.

A delete is a visible tombstone until the MVCC watermark proves no retained
snapshot can need the prior value. Only then may the clustered key and its
undo/blob history be removed. Aborted inserts never become discoverable to a
committed snapshot.

## Commit and abort ownership

The transaction manager owns logical commit state. WAL owns durable ordering.
The physical page publisher owns checkpoint/materialization, but it cannot make
an uncommitted transaction visible by publishing a page.

Synchronous commit follows this order:

```text
validate intents and dependencies
 -> append version/current-record and change records
 -> make the transaction's WAL prefix durable
 -> append and make the commit decision durable
 -> publish Committed(CSN) to the status view
 -> release intents and snapshot registration
```

The commit decision is the single durable visibility decision for the complete
transaction. It covers all touched trees. Acknowledgement is allowed only
when the configured durability mode has made the decision durable. A WAL-first
mode may defer physical page materialization, but recovery must replay or
resolve the complete transaction before exposing it.

Abort marks the transaction aborted and releases intents. It does not rely on
unsafe inverse B-tree mutations. Uncommitted current records are invisible and
are later discarded or replaced by GC after recovery establishes the outcome.

## GC rules

MVCC GC is independent from physical page GC, blob GC, and WAL GC:

- reclaim undo/version records only below the computed MVCC watermark;
- remove committed tombstones only when no retained snapshot needs them;
- reclaim old blob references only after logical version reachability is gone;
- freeze old transaction owners only after status retention is safe;
- never reclaim a version merely because the current root no longer points to
  it if an undo chain, retained snapshot, dependency, or change consumer still
  references it;
- bound GC work and make interrupted GC restartable and idempotent.

GC runs as maintenance under resource bounds. Foreground transactions remain
correct if GC is delayed; they must not observe partially reclaimed chains.

## Non-goals

This ADR does not choose:

- the exact byte encoding of version records;
- the physical file/segment layout of the version store;
- a particular buffer translation algorithm;
- a serializable certifier;
- an autonomous-commit versus group-commit policy;
- a public cursor ABI.

Those choices require separate measurements or ADRs. No implementation may
silently turn a physical page offset into a logical `VersionId` or claim that
inline version chains are the final architecture.

## Acceptance gates

Before this design can support an alpha claim, tests must cover:

- disjoint writers committing without a global expected-generation CAS;
- same-key first-committer-wins conflicts, including absent-key inserts;
- committed, aborted, and active-owner visibility;
- long snapshots retaining old values and tombstones;
- snapshot release advancing the watermark and reclaiming safe history;
- crash/reopen at every append, WAL, commit-decision, and GC boundary;
- interrupted GC preserving all reachable versions and remaining retryable;
- durable retained leases and restart/change-consumer pins;
- opaque values and blob references surviving version traversal and reopen.

Until these gates pass, the transactional facade remains a development
surface and SeerDB is not an alpha release candidate.
